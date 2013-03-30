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
## Author: Young Hyun
## Copyright (C) 2007-2013 The Regents of the University of California.
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
        # Save off the hostname and port from @config to guard against
        # config changes during the full connection attempt.
        @global_server_addr = @config.global_server_addr
        @global_server_port = @config.global_server_port
        @connection =
          Marinda::InsecureClientConnection.new @global_server_addr, 
            @global_server_port
      end

      $log.info "trying to connect to global server %s, port %d",
        @global_server_addr, @global_server_port
      sock = @connection.connect

      # By this point, either we opened a connection, or the connection
      # attempt failed (and we need to retry later), so remove the watcher
      # and purge the connection object (which allows any changes in the
      # hostname or port of the global server to take effect on the next
      # attempt).
      @eva_loop.remove_io @connect_watcher if @connect_watcher
      @connect_watcher = nil
      @connection = nil # force next attempt to get addr & port from config

      if sock
        $log.info "opened connection to global server %s", @global_server_addr
        if @config.use_ssl
          @ssl_watcher = nil
          @ssl_connection =
            Marinda::ClientSSLConnection.new @global_server_addr, sock
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
    sock = @ssl_connection.sock
    begin
      $log.info "trying to establish SSL with global server"
      ssl = @ssl_connection.connect

      # By this point, either the connect succeeded, or we got an error
      # like a post connection failure.  In either case, we'll never try
      # connecting again with the current SSL connection object, so clean up.
      @eva_loop.remove_io @ssl_watcher if @ssl_watcher
      @ssl_watcher = nil
      @ssl_connection = nil

      if ssl
        $log.info "established SSL connection with global server"
        ssl.sync_close = true
        setup_mux_connection sock, ssl
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

      events = (IO::WaitReadable === $! ? :r : :w)
      if @ssl_watcher
        @eva_loop.set_io_events @ssl_watcher, events
      else
        # Keep executing establish_ssl_connection() until we succeed or fail.
        @ssl_watcher = @eva_loop.add_io sock, events,
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

    @regions = {}  # local port => local region
    @channels = {}  # object_id => Channel
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
    flags.allow_all_privileges!

    channel = Channel.new flags, next_client_id(), self, watcher
    @channels[channel.object_id] = channel

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
    end
  end


  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  def find_region(port)
    @regions[port] || (@regions[port] = Region.new self, port)
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
      find_region(port).write tuple, channel
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
      $stderr.printf "%s Region (port=%#x):\n",
	(Port.global?(port) ? "global" : "local"), port

      @mux.dump port, resource
    else
      find_region(port).dump resource
    end
  end


  # Events from GlobalSpaceMux --------------------------------------------

  def mux_result(port, request, result)
    if request.channel.connected
      request.channel.region_result port, request.operation,
        request.template, result
    # else XXX possibly return taken tuple into region
    end
  end


  # Events from Channel ---------------------------------------------------

  def unregister_channel(channel)
    @channels.delete channel.object_id
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
    end
  end

end

end  # module Marinda
