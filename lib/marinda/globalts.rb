#############################################################################
## The global tuple space, which implements the heart of the global server.
##
## This class should normally be invoked by the marinda-gs script, which
## contains the remaining pieces (like option parsing and various
## initializations) needed to create a standalone program.
##
## The global tuple space (global TS) maintains a persistent TCP connection
## with each remote tuple space.  All requests from all clients on a given
## remote node are multiplexed over a single connection.  The global TS
## demultiplexes requests received over each connection, forwards the
## requests to global regions, and then multiplexes results onto the
## connections.
##
## The global TS may be referred to as the demux or GlobalSpaceDemux
## in some older code because much of the code in this file used to be
## in the GlobalSpaceDemux class.
##
## GlobalSpaceMux does the similar job of multiplexing requests onto the
## connections at the remote tuple spaces.
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
require 'marinda/list'
require 'marinda/msgcodes'
require 'marinda/port'
require 'marinda/region'
require 'marinda/globalstate'
require 'marinda/version'

module Marinda

class GlobalSpaceEventLoop

  SIGNAL_INTERVAL = 5.0 # seconds between checks for signals

  def initialize(eva_loop, config, server_connection, space)
    @eva_loop = eva_loop
    @config = config
    @server_connection = server_connection
    @space = space  # must not be nil

    @eva_loop.add_repeating_timer SIGNAL_INTERVAL, &method(:check_signals)
    @eva_loop.add_io server_connection.sock, :r,
      &method(:handle_incoming_connection)

#     @eva_loop.add_repeating_timer 5.0 do
#       @eva_loop.instance_variable_get("@watchers").each_with_index do |w, i|
#         $log.debug "  %3d: %p", i, w
#       end
#     end
  end


  private #..................................................................

  # Eva callback for repeating timer.
  def check_signals(watcher)
    if $checkpoint_state
      $checkpoint_state = false
      $log.info "checkpointing state on SIGUSR1."
      @space.checkpoint_state()
    end

    if $checkpoint_state_and_exit
      $checkpoint_state_and_exit = false
      $log.info "checkpointing state and exiting on SIGTERM/SIGINT."
      @space.checkpoint_state()
      $log.info "exiting after checkpoint upon request."
      @eva_loop.stop()
    end

    if $reload_config
      $reload_config = false
      $log.info "reloading config file on SIGHUP."
      begin
        config = Marinda::GlobalConfig.new $options.config_path
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


  # Eva callback for @server_connection becoming readable.
  def handle_incoming_connection(watcher)
    client_sock, peer_ip, node_id =
      @server_connection.accept_with_whitelist @config.nodes

    if client_sock
      if @config.use_ssl && peer_ip != "127.0.0.1"
        conn = AcceptingSSLConnection.new client_sock, peer_ip, node_id
        conn.watcher = @eva_loop.add_io client_sock, :r,
          &method(:handle_ssl_accept)
        conn.watcher.user_data = conn
      else
        setup_connection client_sock, client_sock, node_id
      end
    end
  end


  # Eva callback for an AcceptingSSLConnection becoming readable/writable.
  def handle_ssl_accept(watcher)
    begin
      accepting_connection = watcher.user_data
      ssl, node_id = accepting_connection.accept

      # By this point, either the accept succeeded, or we got an error like
      # a post connection failure.  In either case, we'll never need to try
      # accepting again, so clean up.
      @eva_loop.remove_io watcher

      client_sock = accepting_connection.sock
      if ssl
        $log.info "established SSL connection with node %d", node_id
        ssl.sync_close = true
        setup_connection client_sock, ssl, node_id
      else # error, nothing further we can do but clean up
        client_sock.close() rescue nil
      end

    # not sure EINTR can be raised by SSL accept
    rescue Errno::EINTR
      # nothing to do; we'll automatically retry

    rescue IO::WaitReadable
      @eva_loop.set_io_events watcher, :r

    rescue IO::WaitWritable
      @eva_loop.set_io_events watcher, :w
    end
  end


  def setup_connection(client_sock, ssl, node_id)
    watcher = @eva_loop.add_io client_sock, :r
    watcher.user_data = ssl
    @space.setup_connection ssl, node_id, watcher
  end

end


#==========================================================================

class GlobalSpace

  private

  include MuxMessageCodes

  MESSAGE_LENGTH_SIZE = 4  # num bytes in length field of transmitted messages
  READ_SIZE = 16384        # num bytes to read at once with sysread

  REGION_METHOD = {
    READ_CMD => :read, READP_CMD => :readp,
    TAKE_CMD => :take, TAKEP_CMD => :takep,
    READ_ALL_CMD => :read_all, TAKE_ALL_CMD => :take_all,
    MONITOR_CMD => :monitor, CONSUME_CMD => :consume, 
    MONITOR_STREAM_CMD => :monitor_stream_setup,
    CONSUME_STREAM_CMD => :consume_stream_setup
  }

  REGION_STREAM_START_METHOD = {
    MONITOR_STREAM_CMD => :monitor_stream_start,
    CONSUME_STREAM_CMD => :consume_stream_start
  }

  # The Context class contains the context of a logically persistent
  # connection between a single remote local tuple space and the demux.
  #
  # Context attributes ----------------------------------------------------
  #
  # negotiating:
  #
  #   Indicates whether the connection is in the 'hello negotiations'
  #   stage.  A connection or re-connection starts off in the 'hello
  #   negotiations' stage.  In this stage, only HELLO_CMD and HELLO_RESP
  #   messages are allowed to be exchanged (one HELLO_CMD and one
  #   HELLO_RESP), and unacked messages aren't resent in either direction.
  #   If the global server detects an error during negotiations, it
  #   sends back an error HELLO_RESP (by setting the protocol to 0 and
  #   including an error text in the payload) followed by disconnection
  #   (the disconnection is implemented in the write_data method).
  #
  # node_id: fixed identity of the remote node
  # session_id:
  #
  #   This value is used to determine whether the remote mux has restarted
  #   with a loss of state.  If on reconnection, the session ID sent by the
  #   remote mux in its hello message is zero, then the remote mux has
  #   either started for the first time or restarted after a failure with
  #   loss of state.  If the session has reset and lost state, then the
  #   global server purges any global state associated with the previous
  #   session.  This involves purging things like private global regions
  #   which are never accessible beyond any given session.
  #
  #   We also use this value to determine whether the *demux* has restarted
  #   with a loss of state.  If the mux sends a non-zero session ID and the
  #   demux doesn't have any active session with that node, then the demux
  #   probably lost state (or the mux is providing a false or obsolete
  #   session ID).  The only recourse is for the remote tuple space to shut
  #   down.  This is a fairly drastic measure, but the only scenario where
  #   this can happen is when the global tuple space suffers a catastrophic
  #   failure (that can't be recovered from with checkpoints), and it's
  #   probably pointless for a local tuple space to continue on after that.
  #
  # sock: open socket to mux or nil if lost connection
  # protocol: protocol version to use with mux
  # banner: banner of mux
  #
  #........................................................................
  # seqnum:
  #
  #   The next available sequence number (that is, use this value in a
  #   message and then post increment the variable).
  #
  #   This must be > 0, since a seqnum of 0 is used by GlobalSpaceMux and
  #   GlobalSpace to mean that no messages have ever been received
  #   from each other.
  #
  #........................................................................
  # mux_seqnum:
  #
  #   The highest sequence number ever observed in messages received from
  #   GlobalSpaceMux; specifically, this gives the seqnum of the latest
  #   command issued by the mux that has been received by the demux; 0 is not
  #   a valid seqnum and instead means no messages have ever been received.
  #
  #........................................................................
  # unacked_messages:
  #
  #   Messages whose receipt has yet to be acknowledged by the mux.
  #   All messages requested to be sent are automatically enqueued into this
  #   list, so not all messages on this list have been sent out yet.  The
  #   messages should be in sorted order according to {message.seqnum}.
  #
  #........................................................................
  # ongoing_requests:
  #
  #   Uncompleted region requests, either because they're blocking (e.g.,
  #   read) or because they're iterative (monitor and read_all).  We use
  #   this to track region operations so that they can be found if the client
  #   wants to cancel them.  The command seqnum of the original request that
  #   initiated an operation is used as the key.
  #
  class Context
    attr_reader :node_id
    attr_accessor :negotiating, :session_id, :sock, :protocol, :banner
    attr_accessor :seqnum, :mux_seqnum, :unacked_messages, :ongoing_requests

    def initialize(node_id)
      @negotiating = true  # currently in hello negotiations
      @node_id = node_id
      @session_id = 0 # a real session ID will be generated at first connection
      @sock = nil
      @protocol = 0
      @banner = ""
      @seqnum = 1
      @mux_seqnum = 0
      @unacked_messages = List.new  # [Message]
      @ongoing_requests = Hash.new  # command_seqnum => RegionRequest
    end

    # Use when permanently removing a node from the tuple space with FIN_CMD.
    def purge_all_state
      @session_id = 0
      @sock.close rescue nil
      @sock = nil
      @protocol = 0
      @banner = nil
      reset_session_state()
    end

    # Use to clear out old session state when starting up a new session for
    # whatever reason (see the handling HELLO_CMD in demux_command).
    def reset_session_state
      @seqnum = 1
      @mux_seqnum = 0
      @unacked_messages.clear
      @ongoing_requests.clear
    end

    def checkpoint_state(txn, checkpoint_id)
      txn.execute("INSERT INTO Connections VALUES(?, ?, ?, ?, ?, ?, ?)",
                  checkpoint_id, @node_id, @session_id, @protocol, @banner,
                  @seqnum, @mux_seqnum)

      seqnum = 0  # synthesized seqnum for maintaining the order of messages
      txn.prepare("INSERT INTO UnackedMessages VALUES(?, ?, ?, ?)") do
        |insert_stmt|
        @unacked_messages.each do |message|
          seqnum += 1
          message_mio = message.to_mio
          insert_stmt.execute checkpoint_id, @session_id, seqnum, message_mio
        end
      end

      txn.prepare("INSERT INTO OngoingRequests VALUES(?, ?, ?, ?, ?)") do
        |insert_stmt|
        @ongoing_requests.each do |command_seqnum, request|
          insert_stmt.execute checkpoint_id, @session_id, command_seqnum,
            request.operation, request.template
        end
      end
    end

  end

  class Message
    attr_accessor :seqnum, :command, :contents

    def self.from_mio(s)
      Message.new *MIO.decode(s)
    end

    def initialize(seqnum, command, contents)
      @seqnum = seqnum
      @command = command
      @contents = contents
    end

    def to_mio
      MIO.encode [ @seqnum, @command, @contents ]
    end

  end

  ReadBuffer = Struct.new :length, :payload

  # SockState contains state for both read and write directions of sockets.
  # The attribute {read_buffer} only applies to reading and contains a
  # instance of ReadBuffer.  The attribute {write_queue} only applies to
  # writing and contains an array of strings.
  #
  # To disconnect after flushing all writes, set write_and_disconnect to
  # true.  This is useful for disconnecting immediately after sending an
  # error response during a failed 'hello negotiations' (see comments at
  # class Context).  Note: You must enqueue something to write (even 1 byte)
  # or write_and_disconnect may not take effect (that is, disconnection only
  # happens in write_data, and write_data will only be executed if there is
  # data to write).
  #
  # The {ssl_io} attribute is for handling SSL renegotiations.  A read
  # operation may actually need the SSL socket to be writable, and a write
  # may need the SSL socket to be readable.  If we're using SSL and an
  # SSL renegotiation occurs, then this attribute will hold either
  # :read_needs_writable or :write_needs_readable.  In the former case,
  # we should include the socket in the write set of select() and then
  # perform a read when select() indicates the socket is writable, and
  # vice versa for the latter case.  We should not intermix any
  # application-level reads/writes until the SSL renegotiation completes.
  class SockState
    attr_accessor :context, :watcher, :read_buffer, :write_queue, :ssl_io
    attr_accessor :write_and_disconnect, :write_queue_ssl_workaround

    def initialize(context)
      @context = context
      @read_buffer = ReadBuffer.new
      @write_queue = []  # [ String ]
      @write_and_disconnect = false
      @write_queue_ssl_workaround = nil  # see comments at on_io_write()
      @ssl_io = nil
    end
  end


  #------------------------------------------------------------------------

  def initialize(eva_loop, state_db_path)
    @eva_loop = eva_loop
    @global_state = GlobalState.new state_db_path

    @regions = {}   # global port => global regions
    @contexts = {}       # node_id => Context

    restore_state()
  end


  def restore_state
    checkpoint_id, timestamp = @global_state.find_latest_checkpoint()
    if checkpoint_id
      $log.info "restoring state from checkpoint %d taken %s",
        checkpoint_id, Time.at(timestamp).to_s

      sessions = {}  # nonzero session_id => Context
      @global_state.db.execute("SELECT * FROM Connections
                                WHERE checkpoint_id=? ", checkpoint_id) do |row|
        node_id = row[1]
        context = Context.new node_id
        context.session_id = row[2]
        context.protocol = row[3]
        context.banner = row[4]
        context.seqnum = row[5]
        context.mux_seqnum = row[6]
        @contexts[node_id] = context
        sessions[context.session_id] = context unless context.session_id == 0
      end

      @global_state.db.execute("SELECT * FROM UnackedMessages
                                WHERE checkpoint_id=?
                                ORDER BY seqnum", checkpoint_id) do |row|
        session_id, seqnum, message_mio = row[1..-1]
        message = Message.from_mio message_mio
        context = sessions[session_id]
        if context
          context.unacked_messages.push message
        else
          $log.err "GlobalSpace#restore_state: no Context found for " +
            "session_id=%#x while restoring unacked_messages " +
            "(checkpoint_id=%d, seqnum=%d, message=%p)",
            session_id, checkpoint_id, seqnum, message_mio
          exit 1
        end
      end

      @global_state.db.execute("SELECT * FROM Regions WHERE checkpoint_id=? ",
                               checkpoint_id) do |row|
        port, tuple_seqnum, node_id, session_id = row[1..-1]

        # The block passed to {region.restore_state} ties together the
        # RegionRequest objects restored in Region to the GlobalSpace.
        region = (@regions[port] ||= Region.new(self, port))
        region.restore_state(@global_state, checkpoint_id,
                             tuple_seqnum, sessions) do |session_id, request|
          context = sessions[session_id]
          unless context
            $log.err "GlobalSpace#restore_state: no Context found for " +
              "session_id=%#x while restoring ongoing_requests " +
              "(checkpoint_id=%d, reqnum=%d, template=%p)", session_id,
              checkpoint_id, request.reqnum, request.template
            exit 1
          end
          context.ongoing_requests[request.reqnum] = request
        end
      end

      # XXX sanity check @ongoing_requests by comparing with checkpoint

      #checkpoint_state()  # might be useful for debugging persistence
    else
      $log.info "skipping restore: no previous state found."
    end
  end


  public  #..................................................................

  def checkpoint_state
    @global_state.start_checkpoint do |txn, checkpoint_id|
      # There's no need to save the values of @last_public_portnum or
      # @last_private_portnum because these values are already persisted
      # whenever they change.
      @regions.each_value do |region|
        region.checkpoint_state txn, checkpoint_id
      end

      @contexts.each_value do |context|
        context.checkpoint_state txn, checkpoint_id
      end
    end
  end


  # Called by GlobalSpaceEventLoop whenever a new client connection (with
  # or without SSL, depending on configuration) is fully established.
  #
  # The {sock} parameter is an open connection to the client when SSL
  # is not being used and an SSL socket wrapping the client connection
  # when SSL is used.
  def setup_connection(sock, node_id, watcher)
    purge_previous_connection node_id
    context = (@contexts[node_id] ||= Context.new(node_id))
    context.negotiating = true
    context.sock = sock

    # Leave SockState.write_queue empty so that nothing is sent (or
    # unacked messages retransmitted) until hello negotiations have
    # successfully completed.
    sock.__connection = SockState.new context
    sock.__connection.watcher = watcher
  end


  # Enforces the constraint that only one connection exist between the
  # global server and a node.
  #
  # This method should be called only after a new connection is fully
  # established and validated--that is, after performing socket-level
  # accept and, where applicable, SSL-level accept and client validation.
  # Otherwise, we might open ourselves up to denial of service if client
  # authentication is not performed; e.g., some program other than the
  # local server on a node could repeatedly open a connection to the global
  # server, causing legitimate connections to be dropped (this form of
  # denial of service won't work with certificate-based client
  # authentication).
  #
  # A prior connection can exist for two reasons: 1) a bug or user error
  # causing multiple connections (e.g., user starts up multiple local
  # servers on a node), or 2) a node is reconnecting after the failure of
  # the prior connection, a failure that hasn't yet been noticed by the
  # global server.
  #
  # Note: Case 2 could happen while a prior connection is waiting on SSL
  #       accept, but we shouldn't purge entries in @accepting_connections.
  #       If we do, there's a danger of dropping legitimate reconnection
  #       attempts.
  def purge_previous_connection(node_id)
    context = @contexts[node_id]
    if context
      sock = context.sock
      context.sock = nil
      if sock
        $log.info "purging existing connection with node %d", node_id        
        @eva_loop.remove_io sock.__connection.watcher

        # Because we enabled SSL sync_close, closing the SSL socket will
        # also close the underlying socket if we're using SSL.
        sock.close rescue nil
        sock.__connection_state = :defunct
        sock.__connection.watcher = nil
        sock.__connection = nil
      end
    end
  end


  #--------------------------------------------------------------------------

  # Called by GlobalSpaceEventLoop whenever a file descriptor is ready to
  # be read.
  def on_io_read(watcher)
    # Don't use watcher.io because it stores the underlying IO object used
    # for polling and not the SSL socket wrapping the client connection
    # when SSL is being used.
    sock = watcher.user_data

    # We must guard against a socket becoming defunct in the same round of
    # select() processing.
    return unless sock.__connection_state == :connected

    state = sock.__connection

    # Handle any SSL renegotiation in progress.
    if state.ssl_io == :write_needs_readable
      state.ssl_io == nil
      on_io_write watcher
      return
    end

    buffer = state.read_buffer
    buffer.payload ||= ""
    messages = []  # list of payload

    begin
#      loop do  # XXX don't loop or we might starve other connections
        data = sock.read_nonblock READ_SIZE

        $log.debug "read_data from %p (node %d): %p",
          sock, state.context.node_id, data if $debug_io_bytes

        start = 0
        while start < data.length
          data_left = data.length - start

          desired_length = buffer.length || MESSAGE_LENGTH_SIZE
          fill_amount = desired_length - buffer.payload.length
          usable_amount = [fill_amount, data_left].min

          buffer.payload << data[start, usable_amount]
          start += usable_amount

          if usable_amount == fill_amount
            if buffer.length
              if $debug_io_bytes
                $log.debug "read_data: buffer.length = %d", buffer.length
                $log.debug "read_data: buffer.payload = %p", buffer.payload
              end
              messages << buffer.payload
              buffer.length = nil
              buffer.payload = ""
            else
              buffer.length = buffer.payload.unpack("N").first
              buffer.payload = ""
              if $debug_io_bytes
                $log.debug "read_data: message length = %d", buffer.length
              end
              if buffer.length == 0
                raise EOFError, "mux protocol error: message length == 0"
              end
            end
          end
        end
#      end

    rescue Errno::EINTR  # might be raised by read_nonblock
      # do nothing, since we'll automatically retry in the next select() round

    # Ruby 1.9.2 preview 2 uses IO::WaitReadable, but earlier versions use
    # plain Errno::EWOULDBLOCK, so technically we could get rid of
    # IO::WaitReadable.  However, we need IO::WaitReadable for SSL.
    rescue Errno::EWOULDBLOCK, IO::WaitReadable
      $log.debug "read_data from %p (node %d): IO::WaitReadable",
        sock, state.context.node_id if $debug_io_bytes
      # do nothing, since we'll automatically retry in the next select() round

    rescue IO::WaitWritable
      $log.info "SSL renegotiation: read_data from %p (node %d): " +
        "IO::WaitWritable", sock, state.context.node_id
      state.ssl_io = :read_needs_writable
      @eva_loop.add_io_events watcher, :w

    rescue SocketError, IOError, EOFError,SystemCallError,OpenSSL::SSL::SSLError
      $log.info "read_data from %p (node %d): %p",
        sock, state.context.node_id, $!
      reset_connection state.context
    end

    messages.each do |payload|
      process_mux_message state.context, payload
    end
  end


  # Called by GlobalSpaceEventLoop whenever a file descriptor is ready to
  # be read.
  #
  # See the comments at class SockState and Context for information on
  # disconnecting during 'hello negotiations'.
  def on_io_write(watcher)
    # Don't use watcher.io because it stores the underlying IO object used
    # for polling and not the SSL socket wrapping the client connection
    # when SSL is being used.
    sock = watcher.user_data

    # We must guard against a socket becoming defunct in the same round of
    # select() processing.
    return unless sock.__connection_state == :connected

    state = sock.__connection

    # Handle any SSL renegotiation in progress.
    if state.ssl_io == :read_needs_writable
      state.ssl_io == nil
      @eva_loop.remove_io_events watcher, :w if state.write_queue.empty?
      on_io_read watcher
      return
    end

    if state.write_queue_ssl_workaround
      buffer = state.write_queue_ssl_workaround
      state.write_queue_ssl_workaround = nil
    else
      if state.write_queue.length == 1
        buffer = state.write_queue.first
      elsif state.write_queue.length > 1
        buffer = state.write_queue.join nil
        state.write_queue.clear
        state.write_queue << buffer
      else  # state.write_queue.length == 0 
        return  # nothing to do -- spurious write readiness
      end
    end

    begin
      while buffer.length > 0
        n = sock.write_nonblock buffer
        data_written = buffer.slice! 0, n
        if $debug_io_bytes
          $log.debug "write_data to %p (node %d): wrote %d bytes, " +
            "%d left: %p", sock, state.context.node_id, n,
            buffer.length, data_written
        end
      end

      state.write_queue.shift  # write_queue will now be empty
      if state.write_and_disconnect
        $log.info "write_data: explicitly disconnecting node %d",
          state.context.node_id
        reset_connection state.context
      else
        @eva_loop.remove_io_events watcher, :w
      end

    rescue Errno::EINTR  # might be raised by write_nonblock
      # do nothing, since we'll automatically retry in the next select() round

    rescue IO::WaitReadable
      $log.info "SSL renegotiation: write_data to %p (node %d): " +
        "IO::WaitReadable", sock, state.context.node_id
      state.ssl_io = :write_needs_readable
      # It shouldn't be necessary to enable :r monitoring, but it doesn't hurt.
      @eva_loop.add_io_events watcher, :r

    # Ruby 1.9.2 preview 2 uses IO::WaitWritable, but earlier versions use
    # plain Errno::EWOULDBLOCK, so technically we could get rid of
    # IO::WaitWritable.  However, we need IO::WaitWritable for SSL.
    rescue Errno::EWOULDBLOCK, IO::WaitWritable
      $log.debug "write_data to %p (node %d): IO::WaitWritable",
        sock, state.context.node_id if $debug_io_bytes
      # do nothing, since we'll automatically retry in the next select() round

      # XXX This implements a workaround for some tricky SSL behavior.
      #
      # When SSL_write() returns with SSL_ERROR_WANT_WRITE or
      # SSL_ERROR_WANT_READ, we must pass the exact same buffer (underlying
      # char * pointer) to SSL_write() when the socket is ready to write.
      # If we pass a different buffer, even with the same contents, then
      # SSL will raise "error:1409F07F:SSL routines:SSL3 WRITE_PENDING:bad
      # write retry", which shows up in Ruby as #<OpenSSL::SSL::SSLError:
      # SSL_write:>.
      state.write_queue_ssl_workaround = buffer

    # syswrite may throw "not opened for writing (IOError)";
    # This also catches Errno::EPIPE.
    rescue SocketError, IOError, SystemCallError, OpenSSL::SSL::SSLError
      $log.info "write_data to %p (node %d): %p", sock, state.context.node_id,$!
      reset_connection state.context
    end
  end


  def reset_connection(context)
    sock = context.sock
    context.sock = nil
    return unless sock

    sock.close rescue nil
    sock.__connection_state = :defunct
    @eva_loop.remove_io sock.__connection.watcher
    sock.__connection.watcher = nil
    sock.__connection = nil
  end


  #--------------------------------------------------------------------------

  def process_mux_message(context, payload)
    # mux_seqnum: the latest sequence number at the GlobalSpaceMux-end
    #             of the link; this gives the seqnum of the latest command
    #             issued by the mux that was just received by the demux;
    #             valid seqnums are >= 1
    #
    # demux_seqnum: the highest demux sequence number ever observed by
    #             GlobalSpaceMux in responses it received from
    #             GlobalSpace, up to the point at which the mux
    #             sent its latest command; a value of 0 means the mux
    #             had never received responses before
    mux_seqnum, demux_seqnum, command, command_payload =
      payload.unpack("wwCa*")
    
    if context.negotiating && command != HELLO_CMD
      $log.notice "ignoring non-HELLO_CMD received in process_mux_message " +
        "while negotiating: node_id=%d, mux_seqnum=%d, demux_seqnum=%d, " +
        "command=%d, payload=%p", context.node_id,
        mux_seqnum, demux_seqnum, command, command_payload
      return
    end

    if $debug_io_messages
      $log.debug "process_mux_message: node_id=%d, mux_seqnum=%p, " +
        "demux_seqnum=%p, command=%p, payload=%p", context.node_id,
        mux_seqnum, demux_seqnum, command, command_payload
    end

    unless mux_seqnum == 0  # i.e., command == HELLO_CMD or HEARTBEAT_CMD
      while !context.unacked_messages.empty? &&
          context.unacked_messages.first.seqnum <= demux_seqnum
        context.unacked_messages.shift
      end

      if mux_seqnum > context.mux_seqnum
        context.mux_seqnum = mux_seqnum 
      else
        $log.info "discarding stale command (<= mux_seqnum %d): " +
          "mux_seqnum=%d, demux_seqnum=%d, command=%p, payload=%p",
          context.mux_seqnum, mux_seqnum, demux_seqnum, command, command_payload
        return  # stale command: ignore it
      end
    end

    arguments = decode_command mux_seqnum, command, command_payload
    demux_command context, command, mux_seqnum, arguments
  end


  def decode_command(reqnum, command, payload)
    case command
    when WRITE_CMD
      port, tuple = payload.unpack("wa*") 
      return [port, tuple]

    when READ_CMD, READP_CMD, TAKE_CMD, TAKEP_CMD, READ_ALL_CMD, TAKE_ALL_CMD,
      MONITOR_CMD, CONSUME_CMD, MONITOR_STREAM_CMD, CONSUME_STREAM_CMD
      if command == READ_ALL_CMD || command == TAKE_ALL_CMD ||
          command == MONITOR_CMD || command == CONSUME_CMD
	port, cursor, template = payload.unpack("wwa*")
      else
	port, template = payload.unpack("wa*")
	cursor = nil
      end

      retval = [port, template]
      retval << cursor if cursor
      return retval

    when CANCEL_CMD
      return payload.unpack("ww")  # port, request.mux_seqnum

    when ACK_CMD, FIN_CMD
      # nothing further to extract
      return []

    when HEARTBEAT_CMD
      return payload.unpack("N")  # timestamp

    when HELLO_CMD
      protocol, rest = payload.unpack("Na*")
      if protocol < MIN_PROTOCOL_VERSION || protocol > PROTOCOL_VERSION
        return [protocol, rest]  # don't bother decoding unsupported protocol
      else
        hello_node_id, hello_session_id, banner = rest.unpack("nwa*")
        return [protocol, hello_node_id, hello_session_id, banner]
      end

    else
      # XXX -- fix this to fail or recover
      retval = [ -1, -1, INVALID_CMD,
	"protocol error: unknown command code #{command} from GSMux",
	payload ]
    end
  end


  def demux_command(context, command, command_seqnum, arguments)
    if $debug_commands
      $log.debug "demux_command: command=%p (%s), seqnum=%d, args=%p",
        command, MUX_COMMANDS[command], command_seqnum, arguments
    end

    case command
    when WRITE_CMD
      port, tuple = arguments
      region_write port, tuple, context
      enq_ack context, command_seqnum

    when READ_CMD, READP_CMD, TAKE_CMD, TAKEP_CMD
      port, template = arguments
      request, tuple, seqnum = region_singleton_operation port, command_seqnum,
        REGION_METHOD[command], template, context
      handle_command_result context, command_seqnum, request, tuple, seqnum

    when READ_ALL_CMD, TAKE_ALL_CMD, MONITOR_CMD, CONSUME_CMD
      port, template, cursor = arguments
      request, tuple, seqnum = region_iteration_operation port, command_seqnum,
        REGION_METHOD[command], template, context, cursor
      handle_command_result context, command_seqnum, request, tuple, seqnum

    when MONITOR_STREAM_CMD, CONSUME_STREAM_CMD
      port, template = arguments

      # First, set up the request in context.ongoing_requests.
      request, tuple, seqnum = region_stream_operation port, command_seqnum,
        REGION_METHOD[command], template, context
      handle_command_result context, command_seqnum, request, tuple, seqnum

      # Stream over all existing matching tuples.
      #
      # This process is separated from the setup so that we can take
      # advantage of the existing mechanisms (e.g., GlobalSpace#region_result
      # callback) for satisfying blocking operations.
      region_stream_operation port, command_seqnum,
        REGION_STREAM_START_METHOD[command], template, context

    when CANCEL_CMD
      port, reqnum = arguments
      request = context.ongoing_requests.delete reqnum
      region_cancel port, request if request
      enq_ack context, command_seqnum

    when ACK_CMD
      # nothing further to do

    when FIN_CMD
      $log.info "FIN_CMD: explicitly and permanently removing node %d",
        context.node_id
      reset_connection context
      reset_node_session_state context
      context.purge_all_state()
      @contexts.delete context.node_id
      # don't respond with ACK or anything else

    when HEARTBEAT_CMD
      timestamp = arguments[0]
      $log.debug "heartbeat from node %d, sent %d", context.node_id, timestamp
      enq_heartbeat_message context, timestamp

    when HELLO_CMD
      protocol, hello_node_id, hello_session_id, banner = arguments
      if protocol < MIN_PROTOCOL_VERSION || protocol > PROTOCOL_VERSION
        $log.err "node %d is using the unsupported v%d protocol; force " +
          "disconnecting node", context.node_id, protocol
        error_text = sprintf "unsupported v%d protocol", protocol
        enq_error_hello_message context, error_text
        return
      end

      # Do logging after the protocol check, since the arguments are correct
      # only if the protocol is valid.
      $log.info "got hello from node %d: protocol=%d, node_id=%d, " +
        "session_id=%#x, banner=%p", context.node_id, protocol,
        hello_node_id, hello_session_id, banner

      if hello_node_id != context.node_id
        $log.err "inconsistent node assignment: global server config says " +
          "node %d, while remote node config says %d; force disconnecting " +
          "node", context.node_id, hello_node_id
        error_text = sprintf "inconsistent node assignment: node %d in " +
          "global server config", context.node_id
        enq_error_hello_message context, error_text
        return
      end

      if hello_session_id != 0 &&
          !@global_state.allocated_session_id?(hello_session_id)
        $log.info "node %d sent never-before-allocated session_id=%#x in " +
          "hello; or the global server has lost all state about previously " +
          "allocated session IDs", context.node_id, hello_session_id
        error_text = sprintf "provided session_id=%#x was never allocated, " +
          "or the global server has lost all state about previously " +
          "allocated session IDs", hello_session_id
        enq_error_hello_message context, error_text
        return
      end

      if hello_session_id == 0 && context.session_id == 0
        context.session_id = @global_state.generate_session_id()
        $log.info "starting first session with node %d (session_id=%#x)",
          context.node_id, context.session_id
        enq_success_hello_message context, protocol, banner
      elsif hello_session_id == 0 && context.session_id != 0
        old_session_id = context.session_id
        context.session_id = @global_state.generate_session_id()
        $log.info "node %d lost state for session_id=%#x; purging old " +
          "state and starting afresh with session_id=%#x", context.node_id,
          old_session_id, context.session_id
        reset_node_session_state context
        enq_success_hello_message context, protocol, banner
      elsif hello_session_id != 0 && context.session_id == 0
        $log.info "global server lost state for node %d (session_id=%#x)",
          context.node_id, hello_session_id
        error_text = sprintf "global server lost state for session %#x",
          hello_session_id
        enq_error_hello_message context, error_text
      elsif hello_session_id == context.session_id
        $log.info "resuming session with node %d (session_id=%#x)",
          context.node_id, context.session_id
        enq_success_hello_message context, protocol, banner
      else # hello_session_id != context.session_id
        $log.info "node %d attempted to resume the obsolete session %#x " +
          "(current session_id=%#x)", context.node_id, hello_session_id,
          context.session_id
        error_text = sprintf "obsolete session %#x", hello_session_id
        enq_error_hello_message context, error_text
      end

    else
      fail "unexpected peer command code '#{command}'"
    end
  end


  def reset_node_session_state(context)
    context.ongoing_requests.each do |reqnum, request|
      region_cancel request.port, request
    end
    context.reset_session_state()
  end


  def handle_command_result(context, reqnum, request, tuple, seqnum)
    if request
      $log.debug "adding ongoing_requests[%d] = %p",
        reqnum, request if $debug_commands
      context.ongoing_requests[reqnum] = request
    else
      enq_tuple context, reqnum, tuple, seqnum
    end
  end


  #--------------------------------------------------------------------------

  def enq_success_hello_message(context, protocol, banner)
    context.negotiating = false
    context.protocol = [ PROTOCOL_VERSION, protocol ].min
    context.banner = banner

    hello_message = generate_success_hello_message context
    enq_hello_message context, hello_message

    state = context.sock.__connection
    context.unacked_messages.each do |message|
      state.write_queue << marshal_message(context, message)
    end
  end


  def enq_error_hello_message(context, error_text)
    message = generate_error_hello_message error_text
    enq_hello_message context, message

    context.sock.__connection.write_and_disconnect = true
  end


  def generate_success_hello_message(context)
    # command, command_seqnum, protocol, node_id, session_id, banner
    contents = [ HELLO_RESP, 0, context.protocol, context.node_id,
                 context.session_id,
             "Marinda Ruby global server v#{Marinda::VERSION}" ].pack("CwNnwa*")
    marshal_hello_contents contents
  end


  def generate_error_hello_message(error_text)
    # command, command_seqnum, protocol, error_text
    contents = [ HELLO_RESP, 0, 0, error_text ].pack("CwNa*")
    marshal_hello_contents contents
  end


  def marshal_hello_contents(contents)
    header = [ 0, 0 ].pack("ww")  # mux_seqnum, demux_seqnum
    length = header.length + contents.length
    [ length ].pack("N") + header + contents
  end


  def enq_hello_message(context, message)
    state = context.sock.__connection
    state.write_queue << message
    # Note: Don't send unacked_messages until negotiations are over.

    @eva_loop.add_io_events state.watcher, :w
  end


  #--------------------------------------------------------------------------

  # {timestamp} should be the timestamp included in the heartbeat from a node.
  def enq_heartbeat_message(context, timestamp)
    # command, command_seqnum, timestamp
    contents = [ HEARTBEAT_RESP, 0, timestamp ].pack("CwN")
    message = marshal_hello_contents contents
    enq_hello_message context, message
  end


  #--------------------------------------------------------------------------

  def enq_tuple(context, command_seqnum, tuple, seqnum)
    if tuple
      response = TUPLE_RESP
      contents = [ response, command_seqnum, seqnum, tuple ].pack("Cwwa*")
    else
      response = TUPLE_NIL_RESP
      contents = [ response, command_seqnum ].pack("Cw")
    end

    enq_message context, response, contents
  end


  def enq_ack(context, command_seqnum)
    contents = [ ACK_RESP, command_seqnum ].pack("Cw")
    enq_message context, ACK_RESP, contents
  end


  def enq_message(context, command, contents)
    message = Message.new context.seqnum, command, contents
    context.seqnum += 1

    if context.sock && context.sock.__connection_state == :connected
      state = context.sock.__connection
      state.write_queue << marshal_message(context, message)
      @eva_loop.add_io_events state.watcher, :w
    end
    context.unacked_messages << message
  end


  def marshal_message(context, message)
    marshal_contents context, message.seqnum, message.contents
  end


  def marshal_contents(context, demux_seqnum, contents)
    header = [ context.mux_seqnum, demux_seqnum ].pack("ww")
    length = header.length + contents.length
    [ length ].pack("N") + header + contents
  end


  # Region access methods ---------------------------------------------------
  #
  # Only execute operations on Regions through these methods, so that
  # details are localized here.

  def find_region(port)
    @regions[port] || (@regions[port] = Region.new self, port)
  end

  def region_write(port, tuple, context)
    find_region(port).write tuple, context
  end

  # operation == :read, :readp, :take, :takep
  def region_singleton_operation(port, reqnum, operation, template, context)
    find_region(port).__send__ operation, reqnum, template, context
  end

  # operation == :read_all, :take_all, :monitor, :consume
  def region_iteration_operation(port, reqnum, operation, template,
                                 context, cursor=0)
    find_region(port).__send__ operation, reqnum, template, context, cursor
  end

  # operation == :monitor_stream_setup, :consume_stream_setup,
  #              :monitor_stream_start, :consume_stream_start
  def region_stream_operation(port, reqnum, operation, template, context)
    find_region(port).__send__ operation, reqnum, template, context
  end

  def region_cancel(port, request)
    find_region(port).cancel request
  end

  def region_shutdown(port)
    find_region(port).shutdown
  end

  def region_dump(port, resource="all")
    find_region(port).dump resource
  end


  public #===================================================================

  # Events from Region ------------------------------------------------------

  def region_result(port, context, reqnum, operation, template, tuple, seqnum)
    $log.debug "region_result(%p, reqnum=%d) = %p",
      operation, reqnum, tuple if $debug_commands
    request = context.ongoing_requests[reqnum]
    if request  # if operation not cancelled
      # Always purge ongoing_requests for non-stream operations.  Each
      # iteration operation must re-instate the request on each iteration.
      unless operation == :monitor_stream || operation == :consume_stream
        context.ongoing_requests.delete reqnum
      end
      enq_tuple context, request.reqnum, tuple, seqnum
    end
  end

end

end  # module Marinda
