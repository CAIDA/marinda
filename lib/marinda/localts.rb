#############################################################################
## The local tuple space, which implements the heart of the local server.
##
## This class should normally be invoked by the marinda-ls script, which
## contains the remaining pieces (like option parsing and various
## initializations) needed to create a standalone program.
##
## The local tuple space (local TS) maintains a persistent TCP connection
## with the global tuple space.  Operations on global regions from all
## local clients are multiplexed over the single connection via
## GlobalSpaceMux.  The global TS executes the forwarded operations and
## returns the results to the local TS, which then forwards the results to
## the originating client.
##
## The local TS directly executes operations on local regions on behalf of
## clients.
##
## --------------------------------------------------------------------------
## Copyright (C) 2007, 2008, 2009 The Regents of the University of California.
## 
## This file is part of Marinda.
## 
## Marinda is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
## 
## Marinda is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with Marinda.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

require 'ostruct'
require 'yaml'

require 'marinda/port'
require 'marinda/region'
require 'marinda/channel'

module Marinda

class LocalSpace

  private

  include Socket::Constants

  TIMEOUT = 5 # seconds of timeout for select
  RECONNECT_DELAY = 60 # seconds between attempts to connect to global server
  RECONNECT_JITTER = 30 # seconds of random jitter added to delay
  HEARTBEAT_TIMEOUT = 30 # seconds from last read/write activity to global serv

  def initialize(config, server_sock)
    @config = config
    @server_sock = server_sock
    @server_sock.extend ConnectionState
    @server_sock.__connection_state = :listening
    @server_sock.__connection = nil

    @node_id = @config.node_id

    @last_public_portnum = Port::PUBLIC_PORTNUM_RESERVED
    @last_private_portnum = Port::PRIVATE_PORTNUM_RESERVED
    @commons_port = Port.make_public_local

    @commons_region = Region.new self, @commons_port
    @regions = {  # port => local public/private regions
      @commons_port => @commons_region
    }

    # private port => [ Channel ]
    @channels = Hash.new { |hash, key| hash[key] = Array.new }
    @services = {}

    if @config.localspace_only
      @mux = nil
      @connection = nil
      @ssl_connection = nil
    else
      @mux = Marinda::GlobalSpaceMux.new @node_id
      @connection = Marinda::InsecureClientConnection.new @config.global_server_addr, @config.global_server_port
      @ssl_connection = nil
    end

    # The timestamp of the next attempt at connecting to the global server.
    @next_connection_attempt = nil

    # IO sets to pass to Kernel::select, and IO sets returned by
    # select.  We use instance variables so that it is easier to inspect
    # a core dump with gdb.
    @read_set = []
    @write_set = []
    @readable = nil
    @writable = nil
  end


  public  #..................................................................

  # The main event loop, handling new client connections, requests on
  # established client connections, and message exchange with the global
  # tuple space.
  #
  # This is called by the marinda-ls script, and any exceptions are caught
  # and reported there.
  def execute
    loop do
      @read_set.clear
      @write_set.clear

      @read_set << @server_sock

      unless @config.localspace_only
        @loop_timestamp = Time.now.to_i
        if @mux.sock  # connection fully established
          @read_set << @mux.sock if @mux.need_io_read
          if @mux.need_io_write
            @write_set << @mux.sock
          else
            if @loop_timestamp - @mux.last_activity_time > HEARTBEAT_TIMEOUT
              @mux.schedule_heartbeat @loop_timestamp
              @write_set << @mux.sock
            end
          end
        elsif @ssl_connection  # TCP connected, but trying to establish SSL
          case @ssl_connection.need_io
          when :read then @read_set << @ssl_connection.sock
          when :write then @write_set << @ssl_connection.sock
          else
            fail "INTERNAL ERROR: unhandled @ssl_connection.need_io=%p",
              @ssl_connection.need_io
          end
        else # @connection != nil  # disconnected, or trying to connect
          case @connection.need_io
          when :connect
            unless @next_connection_attempt &&
                @loop_timestamp < @next_connection_attempt
              connect_to_global_server()

              # An optimization to speed up the connection attempt.  Under
              # normal circumstances, the above attempt to connect will
              # block on write, and so we can immediately put the socket
              # in the write set.
              @write_set << @connection.sock if @connection.need_io == :write
            end

          when :write  # waiting for connect_nonblock to finish
            @write_set << @connection.sock

          when :none
            # If :none, then we were at least connected to the global server
            # without SSL.  So either the SSL connection attempt failed, or
            # we got disconnected after being fully connected (that is,
            # the GlobalSpaceMux detected a connection loss and cleared
            # @mux.sock).
            schedule_next_connection_attempt() unless @next_connection_attempt
            @connection.reset()

          else
            fail "INTERNAL ERROR: unhandled @connection.need_io=%p",
              @connection.need_io
          end
        end
      end

      @channels.each_value do |cs|
        cs.each do |channel|
          @read_set << channel.sock if channel.need_io_read
          @write_set << channel.sock if channel.need_io_write
        end
      end

      if $debug_io_select
        $log.debug "LocalSpace: waiting for I/O ..."
        $log.debug "select read_set (%d fds): %p", @read_set.length, @read_set
        $log.debug "select write_set (%d fds): %p", @write_set.length,@write_set
      end

      # XXX select can throw "closed stream (IOError)" if a write file
      #     descriptor has been closed previously; we may want to catch
      #     this exception and remove the closed descriptor.  Strictly
      #     speaking, users shouldn't be passing in closed file descriptors.
      @readable, @writable = select @read_set, @write_set, nil, TIMEOUT
      if $debug_io_select
        $log.debug "select returned %d readable, %d writable",
          (@readable ? @readable.length : 0), (@writable ? @writable.length : 0)
      end

      if @readable
        @readable.each do |sock|
          $log.debug "readable %p", sock if $debug_io_select
          case sock.__connection_state
          when :connected then sock.__connection.read_data @loop_timestamp
          when :listening then handle_incoming_connection()
          when :ssl_connecting then establish_ssl_connection()
          when :defunct  # nothing to do
          when :connecting
            msg = sprintf "INTERNAL ERROR: __connection_state=:connecting " +
              "for readable %p", sock
            fail msg
          else
            msg = sprintf "INTERNAL ERROR: unknown __connection_state=%p " +
              "for readable %p", sock.__connection_state, sock
            fail msg
          end
        end
      end

      if @writable
        @writable.each do |sock|
          $log.debug "writable %p", sock if $debug_io_select
          case sock.__connection_state
          when :connected then sock.__connection.write_data @loop_timestamp
          when :connecting then connect_to_global_server()
          when :ssl_connecting then establish_ssl_connection()
          when :defunct  # nothing to do
          when :listening
            msg = sprintf "INTERNAL ERROR: __connection_state=:listening " +
              "for writable %p", sock
            fail msg
          else
            msg = sprintf "INTERNAL ERROR: unknown __connection_state=%p " +
              "for writable %p", sock.__connection_state, sock
            fail msg
          end
        end
      end

      if $shutdown_requested
        $shutdown_requested = false
        $log.info "exiting by request."
        # XXX cleanly shut down all client connections and the
        #     global tuple space connection
        return
      end

      if $reload_config
        $reload_config = false
        $log.info "reloading config file on SIGHUP."
        begin
          config = Marinda::LocalConfig.new $options.config_path
          config.export_debugging_flags()
          $log.debug "%p", config if $options.verbose
          @config = config
        rescue # Marinda::ConfigBase::MalformedConfigException & YAML exceptions
          msg = $!.class.name + ": " + $!.to_s
          $log.err "ERROR: couldn't load new config from '%s': %s; " +
            "backtrace: %s", $options.config_path, msg,
            $!.backtrace.join(" <= ")
        end
      end
    end
  end


  private  #.................................................................

  def connect_to_global_server
    begin
      $log.info "trying to connect to global server %s",
        @config.global_server_addr
      sock = @connection.connect
      if sock
        $log.info "opened connection to global server %s",
          @config.global_server_addr
        @next_connection_attempt = nil
        if @config.use_ssl
          @ssl_connection = Marinda::ClientSSLConnection.new @config.global_server_addr, sock
        else
          sock.__connection = @mux
          @mux.setup_connection sock
        end
      else
        schedule_next_connection_attempt()
      end

    # not sure EINTR can be raised by connect_nonblock;
    # IO::WaitReadable is never raised.
    # Ruby 1.9.2 preview 2 uses IO::WaitWritable, but earlier versions use
    # plain Errno::EINPROGRESS, so technically we could get rid of
    # IO::WaitWritable.
    rescue Errno::EINTR, Errno::EINPROGRESS, IO::WaitWritable
      # do nothing; we'll automatically retry in next select round
      if $debug_io_select
        msg = $!.class.name + ": " + $!.to_s
        $log.debug "non-SSL connect_nonblock raised %s", msg
      end
    end
  end


  def establish_ssl_connection
    begin
      $log.info "trying to establish SSL with global server"
      sock = @ssl_connection.connect

      # By this point, either the connect succeeded, or we got an error
      # like a post connection failure.  In either case, we'll never try
      # connecting again with the current connection object, so clean up.
      @ssl_connection = nil

      if sock
        $log.info "established SSL connection with global server"
        @next_connection_attempt = nil
        sock.__connection = @mux
        @mux.setup_connection sock
      else
        schedule_next_connection_attempt()
      end

    # not sure EINTR can be raised by connect_nonblock
    rescue Errno::EINTR, IO::WaitReadable, IO::WaitWritable
      # do nothing; we'll automatically retry in next select round
      if $debug_io_select
        msg = $!.class.name + ": " + $!.to_s
        $log.debug "SSL connect_nonblock raised %s", msg
      end
    end
  end


  def schedule_next_connection_attempt
    @next_connection_attempt = @loop_timestamp + RECONNECT_DELAY +
      rand(RECONNECT_JITTER)
    $log.debug "next connection attempt at %s",
      Time.at(@next_connection_attempt).to_s
  end


  def handle_incoming_connection
    begin
      sock = @server_sock.accept_nonblock

      flags = ChannelFlags.new
      flags.allow_commons_privileges!
      private_port = allocate_private_port
      @regions[private_port] = Region.new self, private_port

      channel = Channel.new flags, self, @commons_port, private_port, sock
      @channels[private_port] << channel

      # XXX can't set TCP_NODELAY: Errno::EINVAL: Invalid argument
      # sock.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true

      sock.extend ConnectionState
      sock.__connection_state = :connected
      sock.__connection = channel

    rescue Errno::EWOULDBLOCK, Errno::EINTR  # not sure EINTR is raised
      # nothing to do; the local server always retries

    rescue
      $log.err "LocalSpace#handle_incoming_connection: accept_nonblock " +
        "failed: %p", $!
    end
  end


  def handle_shutdown(command)
    fail "UNIMPLEMENTED"
  end


  def forward_message(command, channel, *args)
    if channel.sock
      channel.__send__ command, *args
    else
      $log.info "LocalSpace#forward_message: discarding message %p to " +
        "dead channel %#x\n", command, channel.object_id

      case command
      when :binding_created, :channel_duplicated, :port_opened
	new_channel, client_sock = args
	if new_channel
	  new_channel.shutdown
	  client_sock.close rescue nil
	end
      else
        fail "INTERNAL ERROR: unhandled operation %p", operation
      end
    end
  end


  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  # Both {public_port} and {private_port} must be valid allocated ports.
  #
  # For global channels, the corresponding public and private Regions must
  # have been allocated in the remote GlobalSpace.
  def create_channel(flags, public_port, private_port)
    sockets = UNIXSocket.pair SOCK_STREAM, 0
    return nil unless sockets

    channel = Channel.new flags, self, public_port, private_port, sockets[0]
    @channels[private_port] << channel

    # XXX not sure disabling Nagle is required or (universally) supported
    # sockets[0].setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true
    # sockets[1].setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true

    sockets[0].extend ConnectionState
    sockets[0].__connection_state = :connected
    sockets[0].__connection = channel

    return channel, sockets[1]
  end


  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  def allocate_public_port
    begin
      @last_public_portnum += 1
      retval = Port.make_public_local @last_public_portnum
    end while @regions.has_key? retval
    retval
  end


  def allocate_private_port
    begin
      @last_private_portnum += 1
      retval = Port.make_private @node_id, @last_private_portnum
    end while @regions.has_key? retval
    retval
  end


  def return_channel(channel, sockets)
    if channel
      return channel, sockets[1]
    else
      sockets[0].close rescue nil
      sockets[1].close rescue nil
      return nil
    end
  end


  def find_region(port)
    @regions[port]
  end


  public #=================================================================

  # Region proxy methods --------------------------------------------------

  def region_write(port, tuple, channel)
    if Port.global? port
      @mux.write port, tuple
    else
      region = find_region port
      # note: a private peer region may no longer exist--tolerate this
      region.write tuple, channel if region
    end
    nil
  end

  # operation == :read, :readp, :take, :takep
  def region_singleton_operation(operation, port, template, channel)
    if Port.global? port
      request = RegionRequest.new self, port, operation, template, channel
      @mux.__send__ operation, port, request
      request
    else
      find_region(port).__send__ operation, template, channel
    end
  end

  # operation == :read_all, :take_all, :monitor, :consume
  def region_iteration_operation(operation, port, template, channel, cursor=0)
    if Port.global? port
      request = RegionRequest.new self, port, operation, template, channel
      @mux.__send__ operation, port, request, cursor
      request
    else
      find_region(port).__send__ operation, template, channel, cursor
    end
  end

  # operation == :monitor_stream_setup, :consume_stream_setup,
  #              :monitor_stream_start, :consume_stream_start
  def region_stream_operation(operation, port, template, channel)
    if Port.global? port
      # The global server performs the setup and start procedures itself,
      # so we only need to trigger the operation as a whole.
      case operation
      when :monitor_stream_setup then operation = :monitor_stream
      when :consume_stream_setup then operation = :consume_stream
      when :monitor_stream_start, :consume_stream_start then return
      else
        fail "INTERNAL ERROR: unhandled stream operation %p", operation
      end

      request = RegionRequest.new self, port, operation, template, channel
      @mux.__send__ operation, port, request
      request
    else
      find_region(port).__send__ operation, template, channel
    end
  end

  def region_cancel(port, request)
    if Port.global? port
      @mux.cancel port, request
    else
      find_region(port).cancel request
    end
  end

  def region_shutdown(port)
    find_region(port).shutdown unless Port.global? port
  end

  def region_dump(port, resource="all")
    if Port.global? port
      $stderr.puts "==========================================================="
      $stderr.printf "%s %s Region (port=%#x):\n",
	(Port.public?(port) ? "public" : "private"),
	(Port.global?(port) ? "global" : "local"), port

      @mux.dump port, resource
    else
      find_region(port).dump resource
    end
  end


  # Events from GlobalSpaceMux --------------------------------------------

  # An event generated by GlobalSpaceMux#create_private_region.
  def global_private_region_created(request_data, private_port)
    request_data.call private_port
  end

  # An event generated by GlobalSpaceMux#create_region_pair.
  def global_region_pair_created(request_data, private_port)
    request_data.call private_port
  end


  def mux_result(port, request, result)
    if request.channel.sock
      request.channel.region_result port, request.operation,
        request.template, result
    # else XXX possibly return taken tuple into region
    end
  end


  # Events from Channel ---------------------------------------------------

  def create_new_binding(channel)
    flags = channel.flags.privileges
    public_port = channel.public_port
    if @mux && Port.global?(public_port)
      @mux.create_private_region self, lambda { |private_port|
	result = (private_port ?
		  create_channel(flags, public_port, private_port) : nil)
	result ||= [nil, nil]
        forward_message :binding_created, channel, *result
      }
    else
      private_port = allocate_private_port()
      result = create_channel flags, public_port, private_port
      if result
	@regions[private_port] = Region.new self, private_port
      else
	result = [nil, nil]
      end
      channel.binding_created *result
    end
  end


  def duplicate_channel(channel)
    flags = channel.flags.privileges
    result = create_channel flags, channel.public_port, channel.private_port
    result ||= [nil, nil]
    forward_message :channel_duplicated, channel, *result
  end


  # {portnum} should be a port number (not the full port value).  A zero value
  # causes the next available port number to be used.
  #
  # This opens a public port with this port number in the scope implied by
  # {want_global}.  The corresponding private port will be created at the
  # same time.
  def open_port(channel, flags, portnum, want_global)
    want_global &&= @mux

    if portnum == 0
      public_port = Port::UNKNOWN
    else
      public_port = (want_global ? Port.make_public_global(portnum) :
		     Port.make_public_local(portnum))
    end

    if want_global
      @mux.create_region_pair self, public_port, lambda { |private_port|
	result = (private_port ?
		  create_channel(flags, public_port, private_port) : nil)
	result ||= [nil, nil]
        forward_message :port_opened, channel, *result
      }
    else
      private_port = allocate_private_port()
      result = create_channel flags, public_port, private_port
      if result
	@regions[public_port] ||= Region.new self, public_port
	@regions[private_port] = Region.new self, private_port
      else
	result = [nil, nil]
      end
      channel.port_opened *result
    end
  end


  def unregister_channel(channel)
    private_port = channel.private_port
    @channels[private_port].delete channel
    if @channels[private_port].empty?
      @channels.delete private_port

      if @mux && Port.global?(private_port)
	@mux.delete_private_region self, private_port
      else
	private_region = @regions.delete private_port
	private_region.shutdown
      end
    end
  end


  # {service} may be integer, string, or symbol
  def lookup_service(service)
    service = service.to_sym if service.kind_of? String
    @services[service]
  end

  # {name} may be string or symbol
  def register_service(name, index, region)
    name = name.to_sym if name.kind_of? String
    @services[name] = region
    @services[index] = region
  end

  # {name} may be string or symbol
  def unregister_service(name, index)
    name = name.to_sym if name.kind_of? String
    @services.delete name
    @services.delete index
  end


  # Events from Region ---------------------------------------------------

  # Note: We could simply use forward_message, but inlining
  #       channel.region_result call might be faster.
  def region_result(port, channel, operation, template, tuple)
    if channel.sock
      channel.region_result port, operation, template, tuple
    else
      $log.info "LocalSpace#region_result: discarding message to " +
        "dead channel %#x\n", channel.object_id

      case operation
      when :take, :takep, :take_all, :consume, :consume_stream
        if tuple.access_fd
          tuple.access_fd.close rescue nil
          tuple.access_fd = nil
        end
        # XXX possibly return tuple into region
      else
        fail "INTERNAL ERROR: unhandled operation %p", operation
      end
    end
  end

end

end  # module Marinda
