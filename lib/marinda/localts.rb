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

require 'eva'
require 'mioext'
require 'marinda/port'
require 'marinda/region'
require 'marinda/channel'

module Marinda

class GlobalSpaceConnectionLost < IOError; end

class LocalSpaceEventLoop

  SIGNAL_INTERVAL = 5.0 # seconds between checks for signals
  RECONNECT_DELAY = 60 # seconds between attempts to connect to global server
  RECONNECT_JITTER = 30 # seconds of random jitter added to delay

  # The coarse check interval and the unpredictableness of the last activity
  # time combine to help reduce synchronized heartbeats across nodes.
  HEARTBEAT_INTERVAL = 16.0 # seconds between checks for heartbeat
  HEARTBEAT_TIMEOUT = 30 # seconds from last read/write activity to global serv

  def initialize(eva_loop, config, server_sock, space, mux)
    @eva_loop = eva_loop
    @config = config
    @server_sock = server_sock  # Unix domain socket for LocalSpace
    @server_sock.extend ConnectionState
    @server_sock.__connection_state = :listening
    @server_sock.__connection = nil
    @space = space  # must not be nil
    @mux = mux  # must not be nil unless @config.localspace_only

    @eva_loop.add_repeating_timer SIGNAL_INTERVAL, &method(:check_signals)
    @eva_loop.add_io server_sock, :r, &method(:handle_incoming_connection)

#     @eva_loop.add_repeating_timer 5.0 do
#       @eva_loop.instance_variable_get("@watchers").each_with_index do |w, i|
#         $log.debug "  %3d: %p", i, w
#       end
#     end

    unless @config.localspace_only
      @connection = nil
      @connect_watcher = nil  # temporary watcher for performing connect()
      connect_to_global_server()

      @eva_loop.add_repeating_timer HEARTBEAT_INTERVAL do
        if @mux.watcher &&
            @eva_loop.now - @mux.last_activity_time >= HEARTBEAT_TIMEOUT
          @mux.enqueue_heartbeat @eva_loop.now
        end
      end
    end
  end


  private #..................................................................

  # Eva callback for repeating timer.
  def check_signals(watcher)
    if $shutdown_requested
      $shutdown_requested = false
      $log.info "exiting by request."

      # XXX cleanly shut down all client connections and the
      #     global tuple space connection
      @eva_loop.stop()
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


  # Eva callback for @server_sock becoming readable.
  def handle_incoming_connection(_ignore=nil)
    begin
      sock = @server_sock.accept_nonblock
      sock.extend ConnectionState
      sock.__connection_state = :connected

      watcher = @eva_loop.add_io sock, :r
      watcher.user_data = sock  # needed by io_on_read/io_on_write
      @space.handle_incoming_connection watcher, sock

    # not sure EINTR is raised by accept_nonblock
    rescue Errno::EINTR, Errno::EWOULDBLOCK, IO::WaitReadable
      # nothing to do; the local server always retries

    rescue
      $log.err "LocalSpace#handle_incoming_connection: accept_nonblock " +
        "failed: %p", $!
    end
  end


  def connect_to_global_server(_ignore=nil)
    begin
      unless @connection
        @connection =
          Marinda::InsecureClientConnection.new @config.global_server_addr, 
            @config.global_server_port
      end

      # Log the hostname and port from @connection and not from @config,
      # since the config might have changed between the start of the
      # connection attempt.
      $log.info "trying to connect to global server %s, port %d",
        @connection.host, @connection.port
      sock = @connection.connect
      host = @connection.host

      # By this point, either we opened a connection, or the connection
      # attempt failed (and we need to retry later), so remove the watcher
      # and purge the connection object (which allows any changes in the
      # hostname or port of the global server to take effect on the next
      # attempt).
      @eva_loop.remove_io @connect_watcher
      @connect_watcher = nil
      @connection = nil

      if sock
        $log.info "opened connection to global server %s", host
        if @config.use_ssl
          @ssl_watcher = nil
          @ssl_connection = nil
          establish_ssl_connection()
        else
          setup_mux_connection sock, sock
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
      if $debug_io_select
        msg = $!.class.name + ": " + $!.to_s
        $log.debug "non-SSL connect_nonblock raised %s", msg
      end

      # Keep executing connect_to_global_server() until we succeed or fail.
      unless @connect_watcher
        @connect_watcher = @eva_loop.add_io @connection.sock, :w,
          &method(:connect_to_global_server)
      end
    end
  end


  def establish_ssl_connection(_ignore=nil)
    begin
      $log.info "trying to establish SSL with global server"
      unless @ssl_connection
        @ssl_connection =
          Marinda::ClientSSLConnection.new @connection.host, sock
      end
      ssl = @ssl_connection.connect

      # By this point, either the connect succeeded, or we got an error
      # like a post connection failure.  In either case, we'll never try
      # connecting again with the current SSL connection object, so clean up.
      @eva_loop.remove_io @ssl_watcher
      @ssl_watcher = nil
      @ssl_connection = nil

      if ssl
        $log.info "established SSL connection with global server"
        ssl.sync_close = true
        setup_mux_connection @connection.sock, ssl
      else
        schedule_next_connection_attempt()
      end

    rescue Errno::EINTR  # not sure EINTR can be raised by connect_nonblock
      # do nothing; we'll automatically retry

    rescue IO::WaitReadable, IO::WaitWritable
      if $debug_io_select
        msg = $!.class.name + ": " + $!.to_s
        $log.debug "SSL connect_nonblock raised %s", msg
      end

      events = ($! === IO::WaitReadable ? :r : :w)
      if @ssl_watcher
        @eva_loop.set_io_events @ssl_watcher, events
      else
        # Keep executing establish_ssl_connection() until we succeed or fail.
        @ssl_watcher = @eva_loop.add_io @connection.sock, events,
          &method(:establish_ssl_connection)
      end
    end
  end


  def schedule_next_connection_attempt
    delay = RECONNECT_DELAY + rand(RECONNECT_JITTER)
    $log.debug "next connection attempt at %s",
      Time.at(@eva_loop.now + delay).to_s
    @eva_loop.add_timer delay, &method(:connect_to_global_server)
  end


  def setup_mux_connection(client_sock, ssl)
    watcher = @eva_loop.add_io client_sock, :rw
    watcher.user_data = ssl  # needed by io_on_read/io_on_write
    ssl.__connection = @mux
    @mux.setup_connection watcher
  end


  def on_io_read(watcher)
    # Don't use watcher.io because it stores the underlying IO object used
    # for polling and not the SSL socket wrapping the client connection
    # when SSL is being used.
    sock = watcher.user_data

    # We must guard against a socket becoming defunct in the same round of
    # select() processing.
    return unless sock.__connection_state == :connected

    # Dispatch to Channel or GlobalSpaceMux.
    begin
      sock.__connection.read_data sock, @eva_loop.now
    rescue GlobalSpaceConnectionLost
      connect_to_global_server()
    end
  end


  def on_io_write(watcher)
    # Don't use watcher.io because it stores the underlying IO object used
    # for polling and not the SSL socket wrapping the client connection
    # when SSL is being used.
    sock = watcher.user_data

    # We must guard against a socket becoming defunct in the same round of
    # select() processing.
    return unless sock.__connection_state == :connected

    # Dispatch to Channel or GlobalSpaceMux.
    begin
      sock.__connection.write_data sock, @eva_loop.now
    rescue GlobalSpaceConnectionLost
      connect_to_global_server()
    end
  end

end


#==========================================================================

class LocalSpace

  attr_reader :run_id, :node_id, :node_name

  private

  include Socket::Constants

  def initialize(eva_loop, node_id, node_name, mux)
    @eva_loop = eva_loop
    @node_id = node_id
    @node_name = node_name
    @mux = mux

    @client_id = 0
    @run_id = generate_run_id()  # float value of 48-bit int
    $log.info "node %d (%s); run %x", @node_id, @node_name, @run_id

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
  end


  # Generates a run ID that can be used in turn to generate globally unique
  # application-level IDs by clients of the local server.  The range of
  # this value should be large enough to make accidental collisions
  # unlikely (even taking the birthday paradox into account) but not so
  # large as to produce values that are inconvenient to use as part of
  # longer application-level IDs.
  #
  # WARNING: The run ID should NEVER be used for any authentication or
  #          cryptographic purposes.  It isn't unpredictable or unguessable
  #          enough (by design).
  #
  # This returns a double so that we can properly transmit the 48-bit integer
  # in a portable manner (the 'Q' and 'q' Array#pack options use native byte
  # order).
  def generate_run_id
    rand(281474976710656).to_f  # [0, 2**48-1] => 12 hex digits
  end


  public  #..................................................................

  # Called by LocalSpaceEventLoop for each new client connection.
  def handle_incoming_connection(watcher, sock)
    flags = ChannelFlags.new
    flags.allow_commons_privileges!
    private_port = allocate_private_port()
    @regions[private_port] = Region.new self, private_port

    channel = Channel.new flags, next_client_id(), self,
      @commons_port, private_port, watcher
    @channels[private_port] << channel

    sock.__connection = channel
  end


  private  #.................................................................

  def handle_shutdown(command)
    fail "UNIMPLEMENTED"
  end


  def forward_message(command, channel, *args)
    if channel.connected
      channel.__send__ command, *args
    else
      $log.info "LocalSpace#forward_message: discarding message %p to " +
        "dead channel %#x\n", command, channel.object_id

      case command
      when :binding_created, :channel_duplicated, :port_opened
	new_channel, client_sock = args
	if new_channel
	  new_channel.shutdown()
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

    watcher = @eva_loop.add_io sockets[0], :r
    watcher.user_data = sockets[0]  # needed by io_on_read/io_on_write
    channel = Channel.new flags, next_client_id(), self,
      public_port, private_port, watcher
    @channels[private_port] << channel

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


  def next_client_id
    @client_id += 1
    @client_id = 0 if @client_id > 2_147_483_647  # == 2**31 - 1
    @client_id
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
    if request.channel.connected
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
    if channel.connected
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
