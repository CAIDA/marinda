#############################################################################
## A class for demultiplexing I/O at the global tuple space.
##
## This class demultiplexes requests from remote local tuple spaces, issues
## the requests to global regions, and then multiplexes results onto the single
## connection between the global tuple space and each remote local tuple space.
##
## GlobalSpaceMux does the multiplexing at the remote local tuple space.
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
##
## $Id: globaldemux.rb,v 1.37 2009/04/02 23:27:34 youngh Exp $
#############################################################################

require 'ostruct'
require 'thread'
require 'monitor'
require 'yaml'

require 'marinda/list'
require 'marinda/msgcodes'
require 'marinda/port'
require 'marinda/tuple'
require 'marinda/globalts'
require 'marinda/globalstate'

module Marinda

class GlobalSpaceDemux

  private

  include MuxMessageCodes

  CVS_ID = "$Id: globaldemux.rb,v 1.37 2009/04/02 23:27:34 youngh Exp $"
  MESSAGE_LENGTH_SIZE = 4  # num bytes in length field of transmitted messages
  READ_SIZE = 4096         # num bytes to read at once with sysread
  REGION_METHOD = {
    READ_CMD => :read, READP_CMD => :readp,
    TAKE_CMD => :take, TAKEP_CMD => :takep,
    MONITOR_CMD => :monitor, READ_ALL_CMD => :read_all
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
  #   GlobalSpaceDemux to mean that no messages have ever been received
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
          message_yaml = YAML.dump message
          insert_stmt.execute checkpoint_id, @session_id, seqnum, message_yaml
        end
      end

      txn.prepare("INSERT INTO OngoingRequests VALUES(?, ?, ?, ?, ?)") do
        |insert_stmt|
        @ongoing_requests.each do |command_seqnum, request|
          template_yaml = YAML.dump request.template
          insert_stmt.execute checkpoint_id, @session_id, command_seqnum,
            request.operation, template_yaml
        end
      end
    end

  end

  Message = Struct.new :seqnum, :command, :contents
  class Message
    def to_yaml_properties
      %w{ @seqnum @command @contents }
    end
  end

  ReadBuffer = Struct.new :length, :payload
  WriteBuffer = Struct.new :payload

  # SockState contains state for both read and write directions of sockets.
  # The attribute :read_buffer only applies to reading and contains a
  # instance of ReadBuffer.  The attribute :write_queue only applies to
  # writing and contains an array of WriteBuffer.
  #
  # You can insert :disconnect into write_queue to disconnect the connection
  # at a specific point, such as during a failed 'hello negotiations' (see
  # comments at class Context) when you want to disconnect immediately after
  # sending an error response.
  class SockState
    attr_accessor :context, :read_buffer, :write_queue

    def initialize(context)
      @context = context
      @read_buffer = ReadBuffer.new
      @write_queue = List.new  # [WriteBuffer or :disconnect]
    end
  end


  #------------------------------------------------------------------------

  def initialize(state_db_path)
    @global_state = GlobalState.new state_db_path
    @space = GlobalSpace.new self, @global_state

    @inbox = List.new
    @inbox.extend MonitorMixin

    @context = {}       # node_id => Context
    @sock_state = {}    # sock => SockState

    sock_pair = UNIXSocket.pair Socket::SOCK_STREAM, 0
    @control_read = sock_pair[0]
    @control_write = sock_pair[1]
    @control_write.extend MonitorMixin

    @read_set = [ @control_read ]
    @write_set = []

    @dispatch = {}
    @dispatch[:connection_opened] = method :handle_connection_opened
    @dispatch[:checkpoint_state] = method :handle_checkpoint_state
    @dispatch[:checkpoint_state_and_exit] =
      method :handle_checkpoint_state_and_exit

    restore_state()

    @thread = Thread.new(&method(:execute_toplevel))
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
        @context[node_id] = context
        sessions[context.session_id] = context unless context.session_id == 0
      end

      @global_state.db.execute("SELECT * FROM UnackedMessages
                                WHERE checkpoint_id=?
                                ORDER BY seqnum", checkpoint_id) do |row|
        session_id, seqnum, message_yaml = row[1..-1]
        message = YAML.load message_yaml
        context = sessions[session_id]
        if context
          context.unacked_messages.push message
        else
          $log.err "GlobalSpaceDemux#restore_state: no Context found for " +
            "session_id=%#x while restoring unacked_messages " +
            "(checkpoint_id=%d, seqnum=%d, message=%p)",
            session_id, checkpoint_id, seqnum, message_yaml
          exit 1
        end
      end

      @space.restore_state(checkpoint_id, sessions) do |session_id, request|
        context = sessions[session_id]
        unless context
          $log.err "GlobalSpaceDemux#restore_state: no Context found for " +
            "session_id=%#x while restoring ongoing_requests " +
            "(checkpoint_id=%d, reqnum=%d, template=%p)", session_id,
            checkpoint_id, request.template.reqnum, request.template
          exit 1
        end
        context.ongoing_requests[request.template.reqnum] = request
      end

      # XXX sanity check @ongoing_requests by comparing with checkpoint

      #handle_checkpoint_state_and_exit :checkpoint_state_and_exit
      handle_checkpoint_state :checkpoint_state
    else # XXX replay ChangeLog
      $log.info "skipping restore: no previous state found."
    end
  end


  def execute_toplevel
    begin
      execute()
    rescue
      msg = sprintf "GlobalSpaceDemux: thread exiting on uncaught exception " +
        "at top-level: %p; backtrace: %s", $!, $!.backtrace.join(" <= ")
      $log.err "%s", msg
      log_failure msg rescue nil
      exit 1
    end
  end


  def log_failure(msg)
    path = (ENV['HOME'] || "/tmp") +
      "/marinda-globaldemux-failed.#{Time.now.to_i}.#{$$}"
    File.open(path, "w") do |file|
      file.puts msg
    end
  end


  def execute
    loop do
      if $debug_io_select
        $log.debug "GlobalSpaceDemux: waiting for I/O ..."
        $log.debug "select read_set (%d fds): %p", @read_set.length, @read_set
        $log.debug "select write_set (%d fds): %p", @write_set.length,@write_set
      end

      # XXX select can throw "closed stream (IOError)" if a write file
      #     descriptor has been closed previously; we may want to catch
      #     this exception and remove the closed descriptor.  Strictly
      #     speaking, users shouldn't be passing in closed file descriptors.
      readable, writable = select @read_set, @write_set
      if $debug_io_select
        $log.debug "select returned %d readable, %d writable",
          (readable ? readable.length : 0), (writable ? writable.length : 0)
      end

      if readable
        readable.each do |sock|
          $log.debug "readable %p", sock if $debug_io_select
          if sock.equal? @control_read
            handle_messages
          else
            read_data sock
          end
        end
      end

      if writable
        writable.each do |sock|
          $log.debug "writable %p", sock if $debug_io_select
          write_data sock
        end
      end
    end
  end


  def handle_messages
    data = @control_read.sysread READ_SIZE

    messages = []
    @inbox.synchronize do
      data.length.times do
	messages << @inbox.shift
      end
    end

    messages.each do |message|
      $log.debug "handle message %s",
        inspect_message(message) if $debug_commands
      handler = @dispatch[message[0]]
      if handler
	handler.call(*message)
      else
	$log.err "GlobalSpaceDemux#handle_messages: ignoring " +
	  "unknown message type: %p", message
      end
    end
  end


  def handle_connection_opened(command, node_id, sock)
    context = (@context[node_id] ||= Context.new(node_id))
    context.negotiating = true
    reset_connection context if context.sock
    context.sock = sock

    # Leave SockState.write_queue empty so that nothing is sent (or
    # unacked messages retransmitted) until hello negotiations have
    # successfully completed.
    @sock_state[sock] = SockState.new context
    @read_set << sock
  end


  def handle_checkpoint_state(command)
    @global_state.start_checkpoint do |txn, checkpoint_id|
      @space.checkpoint_state txn, checkpoint_id
      @context.each_value do |context|
        context.checkpoint_state txn, checkpoint_id
      end
    end
  end


  def handle_checkpoint_state_and_exit(command)
    handle_checkpoint_state command
    $log.info "exiting after checkpoint upon request"
    exit 0
  end


  def process_mux_message(context, payload)
    # mux_seqnum: the latest sequence number at the GlobalSpaceMux-end
    #             of the link; this gives the seqnum of the latest command
    #             issued by the mux that was just received by the demux;
    #             valid seqnums are >= 1
    #
    # demux_seqnum: the highest demux sequence number ever observed by
    #             GlobalSpaceMux in responses it received from
    #             GlobalSpaceDemux, up to the point at which the mux
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
      flags, recipient, sender, forwarder, values_yaml =
        payload.unpack("Nwwwa*") 

      values = YAML.load values_yaml
      tuple = Tuple.new sender, values
      tuple.flags = flags
      tuple.forwarder = (forwarder == 0 ? nil : forwarder)
      return [recipient, tuple]

    when READ_CMD, READP_CMD, TAKE_CMD, TAKEP_CMD, MONITOR_CMD, READ_ALL_CMD
      if command == MONITOR_CMD || command == READ_ALL_CMD
	recipient, sender, cursor, values_yaml = payload.unpack("wwwa*")
      else
	recipient, sender, values_yaml = payload.unpack("wwa*")
	cursor = nil
      end
      values = YAML.load values_yaml

      template = Template.new sender, values
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
      @space.region_write port, tuple, context
      enq_ack context, command_seqnum

    when READ_CMD, READP_CMD, TAKE_CMD, TAKEP_CMD
      port, template = arguments
      request, tuple = @space.region_singleton_operation REGION_METHOD[command],
        port, template, context
      handle_command_result context, command_seqnum, request, tuple

    when MONITOR_CMD, READ_ALL_CMD
      port, template, cursor = arguments
      request, tuple = @space.region_iteration_operation REGION_METHOD[command],
        port, template, context, cursor
      handle_command_result context, command_seqnum, request, tuple

    when CANCEL_CMD
      port, reqnum = arguments
      request = context.ongoing_requests.delete reqnum
      @space.region_cancel port, request if request
      enq_ack context, command_seqnum

    when ACK_CMD
      # nothing further to do

    when FIN_CMD
      $log.info "FIN_CMD: explicitly and permanently removing node %d",
        context.node_id
      reset_connection context
      reset_node_session_state context
      context.purge_all_state()
      @context.delete context.node_id
      # don't respond with ACK or anything else

    when CREATE_PRIVATE_REGION_CMD
      region = @space.create_private_region context.node_id, context.session_id
      $log.info "created private region (%#x; port=%#x) for " +
        "node %d (session_id=%#x)", region.object_id, region.port,
        context.node_id, context.session_id
      enq_port context, command_seqnum, region.port

    when DELETE_PRIVATE_REGION_CMD
      port = arguments[0]
      $log.info "deleting region (port=%#x) for " +
        "node %d (session_id=%#x)", port, context.node_id, context.session_id
      @space.delete_private_region port
      enq_ack context, command_seqnum

    when CREATE_REGION_PAIR_CMD
      public_port = arguments[0]
      public_region = @space.create_public_region public_port
      private_region =
        @space.create_private_region context.node_id, context.session_id
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
      @space.region_cancel request.port, request
    end
    context.reset_session_state()
    @space.delete_private_regions_of_node context.node_id
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

    message = generate_success_hello_message context
    enq_hello_message context, message

    state = @sock_state[context.sock]
    context.unacked_messages.each do |message|
      state.write_queue << WriteBuffer[marshal_message(context, message)]
    end
  end


  def enq_error_hello_message(context, error_text)
    message = generate_error_hello_message error_text
    enq_hello_message context, message

    state = @sock_state[context.sock]
    state.write_queue << :disconnect
  end


  def generate_success_hello_message(context)
    # command, command_seqnum, protocol, node_id, session_id, banner
    contents = [ HELLO_RESP, 0, context.protocol, context.node_id,
                 context.session_id, CVS_ID ].pack("CwNnwa*")
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
    state = @sock_state[context.sock]
    state.write_queue << WriteBuffer[message]
    @write_set << context.sock
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
      values = YAML.dump tuple.values
      contents = [ response, command_seqnum, flags, sender, forwarder,
                   seqnum, values ].pack("CwNwwwa*")
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

    if context.sock  # connected?
      state = @sock_state[context.sock]
      @write_set << context.sock if state.write_queue.empty?
      state.write_queue << WriteBuffer[marshal_message(context, message)]
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

  def enqueue_event(*message)
    @inbox.synchronize do
      @inbox << message
    end
    
    @control_write.synchronize do
      @control_write.send "M", 0
    end
  end


  def inspect_message(message)
    body = message.map do |element|
      case element
      when String, Numeric, Symbol, TrueClass, FalseClass, NilClass
        element.inspect
      else
        sprintf "#<%s:0x%x>", element.class.name, element.object_id
      end
    end.join(", ")
    "[" + body + "]"
  end


  #--------------------------------------------------------------------------

  def read_data(sock)
    state = @sock_state[sock]
    return unless state  # connection was lost

    messages = []  # list of payload

    buffer = state.read_buffer
    buffer.payload ||= ""

    begin
      # NOTE: To prevent blocking, only do 1 sysread call per read_data() call.
      data = sock.sysread READ_SIZE
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

    rescue SocketError, IOError, EOFError, SystemCallError,
      OpenSSL::SSL::SSLError
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
    state = @sock_state[sock]
    return unless state    # connection was lost

    if state.write_queue.empty?
      $log.notice "runtime inconsistency: called GlobalSpaceDemux#write_data "+
        "on a socket (%p) with an empty write_queue; node_id=%d, " +
        "session_id=%#x, demux_seqnum=%d, mux_seqnum=%d", sock,
        state.context.node_id, state.context.session_id,
        state.context.seqnum, state.context.mux_seqnum
      @write_set.delete sock
      return
    end

    buffer = state.write_queue.first
    if buffer == :disconnect
      $log.info "write_data: explicitly disconnecting node %d",
        state.context.node_id
      state.write_queue.clear
      reset_connection state.context
      return
    end

    begin
      # NOTE: To prevent blocking, only do 1 syswrite call per write_data().
      n = sock.syswrite buffer.payload
      data_written = buffer.payload.slice! 0, n
      if $debug_io_bytes
        $log.debug "write_data to %p (node %d): wrote %d bytes, %d left: %p",
          sock, state.context.node_id, n, buffer.payload.length, data_written
      end

      # syswrite may throw "not opened for writing (IOError)"
    rescue SocketError, IOError, SystemCallError # including Errno::EPIPE
      $log.info "write_data to %p (node %d): %p", sock, state.context.node_id,$!
      reset_connection state.context

    else
      if buffer.payload.length == 0
	state.write_queue.shift
        @write_set.delete sock if state.write_queue.empty?
      end
    end
  end


  def reset_connection(context)
    @read_set.delete context.sock
    @write_set.delete context.sock
    @sock_state.delete context.sock
    context.sock.close rescue nil
    context.sock = nil
  end


  public #===================================================================

  # Events from GlobalServer ------------------------------------------------

  def connection_opened(node_id, sock)
    enqueue_event :connection_opened, node_id, sock
  end


  def checkpoint_state
    enqueue_event :checkpoint_state
  end


  def checkpoint_state_and_exit
    enqueue_event :checkpoint_state_and_exit
  end


  # Events from Region ------------------------------------------------------

  def region_result(port, context, operation, template, tuple)
    $log.debug "region_result(%p, reqnum=%d) = %p",
      operation, template.reqnum, tuple if $debug_commands
    request = context.ongoing_requests[template.reqnum]
    if request  # if operation not cancelled
=begin
      unless tuple && (operation == :monitor || operation == :next)
        context.ongoing_requests.delete template.reqnum
      end
=end
      # XXX: Always purge ongoing_requests for now, until/if we implement
      #      handling of NEXT_CMD, which is merely an optimization.
      context.ongoing_requests.delete template.reqnum
      enq_tuple context, request.template.reqnum, tuple
    end
  end

end

end  # module Marinda
