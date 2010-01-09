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

require 'mioext'
require 'marinda/list'
require 'marinda/msgcodes'
require 'marinda/port'
require 'marinda/tuple'
require 'marinda/region'
require 'marinda/globalstate'
require 'marinda/version'

module Marinda

class GlobalSpace

  private

  include MuxMessageCodes

  TIMEOUT = 5 # seconds of timeout for select
  MESSAGE_LENGTH_SIZE = 4  # num bytes in length field of transmitted messages
  READ_SIZE = 8192         # num bytes to read at once with sysread

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
          template_mio = request.template.to_mio
          insert_stmt.execute checkpoint_id, @session_id, command_seqnum,
            request.operation, template_mio
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

  # Old definition modified for use during migration.

#   Message = Struct.new :seqnum, :command, :contents
#   class Message
#     def to_yaml_properties
#       %w{ @seqnum @command @contents }
#     end

#     def mio_encode
#       MIO.encode [ seqnum(), command(), contents() ]
#     end

#     def mio_decode(s)
#       v = MIO.decode s
#       self.seqnum = v[0]
#       self.command = v[1]
#       self.contents = v[2]
#       self
#     end
#   end

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
    attr_accessor :context, :read_buffer, :write_queue, :ssl_io
    attr_accessor :write_and_disconnect

    def initialize(context)
      @context = context
      @read_buffer = ReadBuffer.new
      @write_queue = []  # [ String ]
      @write_and_disconnect = false
      @ssl_io = nil
    end
  end


  #------------------------------------------------------------------------

  def initialize(config, server_connection, state_db_path)
    @config = config
    @server_connection = server_connection
    @global_state = GlobalState.new state_db_path

    @services = {}
    @last_public_portnum = @global_state.get_last_public_portnum()
    @last_private_portnum = @global_state.get_last_private_portnum()
    @commons_port = Port.make_public_global
    @commons_region = Region.new self, @commons_port
    @regions = {   # port => global public & private regions
      @commons_port => @commons_region
    }

    @contexts = {}       # node_id => Context
    @accepting_connections = []   # [ AcceptingSSLConnection ]

    # IO sets to pass to Kernel::select, and IO sets returned by
    # select.  We use instance variables so that it is easier to inspect
    # a core dump with gdb.
    @read_set = []
    @write_set = []
    @readable = nil
    @writable = nil

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

        # The commons region already exists.  All others must be created here.
        region = (@regions[port] ||= Region.new(self, port))
        if Port.private?(port)
          region.node_id = node_id
          region.session_id = session_id
        end

        # The block passed to {region.restore_state} ties together the
        # RegionRequest objects restored in TemplateBag to the GlobalSpace.
        region.restore_state(@global_state, checkpoint_id,
                             tuple_seqnum, sessions) do |session_id, request|
          context = sessions[session_id]
          unless context
            $log.err "GlobalSpace#restore_state: no Context found for " +
              "session_id=%#x while restoring ongoing_requests " +
              "(checkpoint_id=%d, reqnum=%d, template=%p)", session_id,
              checkpoint_id, request.template.reqnum, request.template
            exit 1
          end
          context.ongoing_requests[request.template.reqnum] = request
        end
      end

      # XXX sanity check @ongoing_requests by comparing with checkpoint

      #checkpoint_state()  # might be useful for debugging persistence
    else
      $log.info "skipping restore: no previous state found."
    end
  end


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


  public  #..................................................................

  # The main event loop, handling new client connections and requests on
  # established client connections.
  #
  # This is called by the marinda-gs script, and any exceptions are caught
  # and reported there.
  def execute
    loop do
      @read_set.clear
      @write_set.clear

      @read_set << @server_connection.sock

      @accepting_connections.each do |conn|
        @read_set << conn.sock if conn.need_io == :read
        @write_set << conn.sock if conn.need_io == :write
      end

      @contexts.each_value do |context|
        sock = context.sock
        if sock && sock.__connection_state == :connected
          state = sock.__connection
          case state.ssl_io  # see comments at class SockState
          when :read_needs_writable then @write_set << sock
          when :write_needs_readable then @read_set << sock
          when nil
            @read_set << sock
            @write_set << sock unless state.write_queue.empty?
          else
            msg = sprintf "INTERNAL ERROR: unknown ssl_io=%p " +
              "for socket %p", state.ssl_io, sock
            fail msg
          end
        end
      end

      if $debug_io_select
        $log.debug "GlobalSpace: waiting for I/O ..."
        $log.debug "%d pending SSL connections", @accepting_connections.length
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
          when :connected
            if sock.__connection.ssl_io == :write_needs_readable
              sock.__connection.ssl_io = nil
              write_data sock
            else
              read_data sock
            end
          when :listening then handle_incoming_connection()
          when :ssl_accepting then handle_ssl_accept sock
          when :defunct  # nothing to do
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
          when :connected
            if sock.__connection.ssl_io == :read_needs_writable
              sock.__connection.ssl_io = nil
              read_data sock
            else
              write_data sock
            end
          when :ssl_accepting then handle_ssl_accept sock
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

      if $checkpoint_state
        $checkpoint_state = false
        $log.info "checkpointing state on SIGUSR1."
        checkpoint_state()
      end

      if $checkpoint_state_and_exit
        $checkpoint_state_and_exit = false
        $log.info "checkpointing state and exiting on SIGTERM/SIGINT."
        checkpoint_state()
        $log.info "exiting after checkpoint upon request."
        return
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
  end


  private #..................................................................

  def handle_incoming_connection
    client_sock, peer_ip, node_id =
      @server_connection.accept_with_whitelist @config.nodes

    if client_sock
      if @config.use_ssl && peer_ip != "127.0.0.1"
        conn = AcceptingSSLConnection.new client_sock, peer_ip, node_id
        @accepting_connections << conn
      else
        setup_connection client_sock, node_id
      end
    end
  end


  def setup_connection(sock, node_id)
    purge_previous_connection node_id
    context = (@contexts[node_id] ||= Context.new(node_id))
    context.negotiating = true
    context.sock = sock

    # Leave SockState.write_queue empty so that nothing is sent (or
    # unacked messages retransmitted) until hello negotiations have
    # successfully completed.
    sock.__connection = SockState.new context
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
      if sock
        $log.info "purging existing connection with node %d", node_id        
        context.sock = nil
        sock.close rescue nil
        #sock.__connection_state = :defunct
      end
    end
  end


  def handle_ssl_accept(sock)
    begin
      conn = sock.__connection
      ssl, node_id = conn.accept

      # By this point, either the accept succeeded, or we got an error like
      # a post connection failure.  In either case, we'll never need to try
      # accepting again, so clean up.
      @accepting_connections.delete conn
      if ssl
        $log.info "established SSL connection with node %d", node_id
        setup_connection ssl, node_id
      end

    # not sure EINTR can be raised by SSL accept
    rescue Errno::EINTR, IO::WaitReadable, IO::WaitWritable
      # do nothing; we'll automatically retry in next select round
    end
  end


  #--------------------------------------------------------------------------

  def read_data(sock)
    # We must guard against a socket becoming defunct in the same round of
    # select() processing.
    return unless sock.__connection_state == :connected

    messages = []  # list of payload

    state = sock.__connection
    buffer = state.read_buffer
    buffer.payload ||= ""

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

    rescue SocketError, IOError, EOFError, SystemCallError,OpenSSL::SSL::SSLError
      $log.info "read_data from %p (node %d): %p",
        sock, state.context.node_id, $!
      reset_connection state.context
    end

    messages.each do |payload|
      process_mux_message state.context, payload
    end
  end


  # See the comments at class SockState and Context for information on
  # disconnecting during 'hello negotiations'.
  def write_data(sock)
    # We must guard against a socket becoming defunct in the same round of
    # select() processing.
    return unless sock.__connection_state == :connected

    state = sock.__connection
    if state.write_queue.length == 1
      buffer = state.write_queue.first
    else # state.write_queue.length > 1
      buffer = state.write_queue.join nil
      state.write_queue.clear
      state.write_queue << buffer
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

      state.write_queue.shift
      if state.write_and_disconnect
        $log.info "write_data: explicitly disconnecting node %d",
          state.context.node_id
        reset_connection state.context
      end

    rescue Errno::EINTR  # might be raised by write_nonblock
      # do nothing, since we'll automatically retry in the next select() round

    rescue IO::WaitReadable
      $log.info "SSL renegotiation: write_data to %p (node %d): " +
        "IO::WaitReadable", sock, state.context.node_id
      state.ssl_io = :write_needs_readable

    # Ruby 1.9.2 preview 2 uses IO::WaitWritable, but earlier versions use
    # plain Errno::EWOULDBLOCK, so technically we could get rid of
    # IO::WaitWritable.  However, we need IO::WaitWritable for SSL.
    rescue Errno::EWOULDBLOCK, IO::WaitWritable
      $log.debug "write_data from %p (node %d): IO::WaitWritable",
        sock, state.context.node_id if $debug_io_bytes
      # do nothing, since we'll automatically retry in the next select() round

    # syswrite may throw "not opened for writing (IOError)";
    # This also catches Errno::EPIPE.
    rescue SocketError, IOError, SystemCallError, OpenSSL::SSL::SSLError
      $log.info "write_data to %p (node %d): %p", sock, state.context.node_id,$!
      reset_connection state.context
    end
  end


  def reset_connection(context)
    return unless context.sock
    context.sock.close rescue nil
    context.sock.__connection_state = :defunct
    context.sock.__connection = nil
    context.sock = nil
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
      flags, recipient, sender, forwarder, values_mio =
        payload.unpack("Nwwwa*") 

      tuple = Tuple.new sender, values_mio
      tuple.flags = flags
      tuple.forwarder = (forwarder == 0 ? nil : forwarder)
      return [recipient, tuple]

    when READ_CMD, READP_CMD, TAKE_CMD, TAKEP_CMD, READ_ALL_CMD, TAKE_ALL_CMD,
      MONITOR_CMD, CONSUME_CMD, MONITOR_STREAM_CMD, CONSUME_STREAM_CMD
      if command == READ_ALL_CMD || command == TAKE_ALL_CMD ||
          command == MONITOR_CMD || command == CONSUME_CMD
	recipient, sender, cursor, values_mio = payload.unpack("wwwa*")
      else
	recipient, sender, values_mio = payload.unpack("wwa*")
	cursor = nil
      end

      template = Template.new sender, values_mio
      template.reqnum = reqnum
      retval = [recipient, template]
      retval << cursor if cursor
      return retval

    when CANCEL_CMD
      return payload.unpack("ww")  # recipient, request.mux_seqnum

    when ACK_CMD, FIN_CMD, CREATE_PRIVATE_REGION_CMD
      # nothing further to extract
      return []

    when DELETE_PRIVATE_REGION_CMD, CREATE_REGION_PAIR_CMD
      return payload.unpack("w")  # port

    when HEARTBEAT_CMD
      return payload.unpack("N")  # timestamp

    when HELLO_CMD
      protocol, rest = payload.unpack("Na*")
      if protocol <= 1 || protocol > PROTOCOL_VERSION
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
      request, tuple = region_singleton_operation REGION_METHOD[command],
        port, template, context
      handle_command_result context, command_seqnum, request, tuple

    when READ_ALL_CMD, TAKE_ALL_CMD, MONITOR_CMD, CONSUME_CMD
      port, template, cursor = arguments
      request, tuple = region_iteration_operation REGION_METHOD[command],
        port, template, context, cursor
      handle_command_result context, command_seqnum, request, tuple

    when MONITOR_STREAM_CMD, CONSUME_STREAM_CMD
      port, template = arguments

      # First, set up the request in context.ongoing_requests.
      request, tuple = region_stream_operation REGION_METHOD[command],
        port, template, context
      handle_command_result context, command_seqnum, request, tuple

      # Stream over all existing matching tuples.
      #
      # This process is separated from the setup so that we can take
      # advantage of the existing mechanisms (e.g., GlobalSpace#region_result
      # callback) for satisfying blocking operations.
      region_stream_operation REGION_STREAM_START_METHOD[command],
        port, template, context

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

    when CREATE_PRIVATE_REGION_CMD
      region = create_private_region context.node_id, context.session_id
      $log.info "created private region (%#x; port=%#x) for " +
        "node %d (session_id=%#x)", region.object_id, region.port,
        context.node_id, context.session_id
      enq_port context, command_seqnum, region.port

    when DELETE_PRIVATE_REGION_CMD
      port = arguments[0]
      $log.info "deleting region (port=%#x) for " +
        "node %d (session_id=%#x)", port, context.node_id, context.session_id
      delete_private_region port
      enq_ack context, command_seqnum

    when CREATE_REGION_PAIR_CMD
      public_port = arguments[0]
      public_region = create_public_region public_port
      private_region = create_private_region context.node_id, context.session_id
      $log.info "created public region (%#x; port=%#x) for " +
        "node %d (session_id=%#x)", public_region.object_id,
        public_region.port, context.node_id, context.session_id
      $log.info "created private region (%#x; port=%#x) for " +
        "node %d (session_id=%#x)", private_region.object_id,
        private_region.port, context.node_id, context.session_id
      enq_port context, command_seqnum, private_region.port

    when HEARTBEAT_CMD
      timestamp = arguments[0]
      $log.info "[%s] received heartbeat from node %d, sent %d (%s)",
        Time.now.to_s, context.node_id, timestamp, Time.at(timestamp).to_s
      # don't send ACK_RESP

    when HELLO_CMD
      protocol, hello_node_id, hello_session_id, banner = arguments
      if protocol <= 1 || protocol > PROTOCOL_VERSION
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
    delete_private_regions_of_node context.node_id
  end


  def handle_command_result(context, reqnum, request, tuple)
    if request
      $log.debug "adding ongoing_requests[%d] = %p",
        reqnum, request if $debug_commands
      context.ongoing_requests[reqnum] = request
    else
      enq_tuple context, reqnum, tuple
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
    context.sock.__connection.write_queue << message
    # Note: Don't send unacked_messages until negotiations are over.
  end


  #--------------------------------------------------------------------------

  def enq_tuple(context, command_seqnum, tuple)
    if tuple
      response = TUPLE_RESP
      flags = tuple.flags
      sender = tuple.sender
      forwarder = (tuple.forwarder || 0)
      seqnum = tuple.seqnum
      contents = [ response, command_seqnum, flags, sender, forwarder,
                   seqnum, tuple.values_mio ].pack("CwNwwwa*")
    else
      response = TUPLE_NIL_RESP
      contents = [ response, command_seqnum ].pack("Cw")
    end

    enq_message context, response, contents
  end


  def enq_port(context, command_seqnum, port)
    contents = [ PORT_RESP, command_seqnum, port ].pack("Cww")
    enq_message context, PORT_RESP, contents
  end


  def enq_ack(context, command_seqnum)
    contents = [ ACK_RESP, command_seqnum ].pack("Cw")
    enq_message context, ACK_RESP, contents
  end


  def enq_message(context, command, contents)
    message = Message.new context.seqnum, command, contents
    context.seqnum += 1

    if context.sock && context.sock.__connection_state == :connected
      context.sock.__connection.write_queue << marshal_message(context, message)
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


  #--------------------------------------------------------------------------

  # XXX don't check existence
  def allocate_public_port
    begin
      @last_public_portnum += 1
      retval = Port.make_public_local @last_public_portnum
    end while @regions.has_key? retval

    @global_state.set_last_public_portnum @last_public_portnum
    retval
  end


  # XXX don't check existence
  def allocate_private_port
    begin
      @last_private_portnum += 1
      retval = Port.make_private Port::GLOBAL_SPACE_NODE_ID,
	@last_private_portnum
    end while @regions.has_key? retval

    @global_state.set_last_private_portnum @last_private_portnum
    retval
  end


  # Returns the newly allocated region.
  def create_public_region(port=nil)
    port ||= allocate_public_port()
    @regions[port] ||= Region.new self, port
  end


  # Returns the newly allocated region.
  def create_private_region(node_id, session_id)
    port = allocate_private_port()
    region = Region.new self, port
    region.node_id = node_id
    region.session_id = session_id
    @regions[port] = region
  end


  # NOTE: It's more safe to purge private regions by node ID than by session
  #       ID, since the node ID can't been spoofed, while the session ID
  #       must be accepted from a potentially unreliable source (the remote
  #       GlobalSpaceMux).  Purging by session ID would allow a malicious
  #       or malfunctioning mux to delete private regions of other nodes.
  #
  #       Purging by node ID also ensures that private regions never slip
  #       through and end up being persistent garbage.
  def delete_private_regions_of_node(node_id)
    ports = []
    @regions.each do |port, region|
      if Port.private?(port) && region.node_id == node_id
        $log.info "purging defunct private region %#x of node %d " +
          "(session_id=%#x)", port, node_id, region.session_id
        ports << port
      end
    end

    ports.each do |port|
      delete_private_region port
    end
  end


  def delete_private_region(port)
    region = @regions.delete port
    region.shutdown if region
  end


  # Region access methods ---------------------------------------------------
  #
  # Only execute operations on Regions through these methods, so that
  # details are localized here.

  def region_write(port, tuple, context)
    region = @regions[port]
    # note: a private peer region may no longer exist--tolerate this
    region.write tuple, context if region
  end

  # operation == :read, :readp, :take, :takep
  def region_singleton_operation(operation, port, template, context)
    @regions[port].__send__ operation, template, context
  end

  # operation == :read_all, :take_all, :monitor, :consume
  def region_iteration_operation(operation, port, template, context, cursor=0)
    @regions[port].__send__ operation, template, context, cursor
  end

  # operation == :monitor_stream_setup, :consume_stream_setup,
  #              :monitor_stream_start, :consume_stream_start
  def region_stream_operation(operation, port, template, context)
    @regions[port].__send__ operation, template, context
  end

  def region_cancel(port, request)
    @regions[port].cancel request
  end

  def region_shutdown(port)
    @regions[port].shutdown
  end

  def region_dump(port, resource="all")
    @regions[port].dump resource
  end


  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  # {service} may be integer, string, or symbol
  def lookup_service(service)
    @mutex.synchronize do
      service = service.to_sym if service.kind_of? String
      @services[service]
    end
  end

  # {name} may be string or symbol
  def register_service(name, index, region)
    @mutex.synchronize do
      name = name.to_sym if name.kind_of? String
      @services[name] = region
      @services[index] = region
    end
  end

  # {name} may be string or symbol
  def unregister_service(name, index)
    @mutex.synchronize do
      name = name.to_sym if name.kind_of? String
      @services.delete name
      @services.delete index
    end
  end


  public #===================================================================

  # Events from Region ------------------------------------------------------

  def region_result(port, context, operation, template, tuple)
    $log.debug "region_result(%p, reqnum=%d) = %p",
      operation, template.reqnum, tuple if $debug_commands
    request = context.ongoing_requests[template.reqnum]
    if request  # if operation not cancelled
      # Always purge ongoing_requests for non-stream operations.  Each
      # iteration operation must re-instate the request on each iteration.
      unless operation == :monitor_stream || operation == :consume_stream
        context.ongoing_requests.delete template.reqnum
      end
      enq_tuple context, request.template.reqnum, tuple
    end
  end

end

end  # module Marinda
