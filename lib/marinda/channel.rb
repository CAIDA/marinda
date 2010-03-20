#############################################################################
## A single connection between a client and the local server.
##
## A Channel object exists in the local server process.  For the
## object on the client side of the connection, see Marinda::Client.
##
## A Channel object is associated with a pair of public and private regions.
##
## --------------------------------------------------------------------------
## The client communication protocol has the following binary format:
##
##     [ length | reqnum | command-code | remaining-payload ]
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
require 'thread'
require 'socket'

require 'mioext'
require 'marinda/msgcodes'
require 'marinda/flagsets'
require 'marinda/port'
require 'marinda/tuple'
require 'marinda/version'

module Marinda

class Channel

  class ChannelError < RuntimeError; end
  class ChannelProtocolUnsupportedError < ChannelError; end
  class ChannelPrivilegeError < ChannelError; end

  include ChannelMessageCodes

  PEER_HANDLE_MAX = 2**31 - 1   # max value storable with Array#pack("N")

  CLIENT_MESSAGE_LENGTH_SIZE = 2  # num bytes in length field of client msg
  READ_SIZE = 8192   # num bytes to read at once with sysread

  ReadBuffer = Struct.new :length, :payload, :want_io
  WriteBuffer = Struct.new :payload, :file

  OPERATION_TO_COMMAND = {
    :read_all => READ_ALL_CMD, :take_all => TAKE_ALL_CMD,
    :monitor => MONITOR_CMD, :consume => CONSUME_CMD,
    :monitor_stream => MONITOR_STREAM_CMD,
    :consume_stream => CONSUME_STREAM_CMD,
    :next => NEXT_CMD
  }

  REGION_METHOD = {
    READ_CMD => :read, READP_CMD => :readp,
    TAKE_CMD => :take, TAKEP_CMD => :takep,
    TAKE_PRIV_CMD => :take, TAKEP_PRIV_CMD => :takep,
    READ_ALL_CMD => :read_all, TAKE_ALL_CMD => :take_all,
    MONITOR_CMD => :monitor, CONSUME_CMD => :consume,
    MONITOR_STREAM_CMD => :monitor_stream_setup,
    CONSUME_STREAM_CMD => :consume_stream_setup,
  }

  REGION_STREAM_START_METHOD = {
    MONITOR_STREAM_CMD => :monitor_stream_start,
    CONSUME_STREAM_CMD => :consume_stream_start
  }

  COMMAND_PRIV_NEEDED = {
    READ_CMD => :read, READP_CMD => :read,
    TAKE_CMD => :take, TAKEP_CMD => :take,
    TAKE_PRIV_CMD => :none, TAKEP_PRIV_CMD => :none,
    READ_ALL_CMD => :read, TAKE_ALL_CMD => :take,
    MONITOR_CMD => :read, CONSUME_CMD => :take,
    MONITOR_STREAM_CMD => :read, CONSUME_STREAM_CMD => :take
  }

  COMMAND_ON_PRIVATE_REGION = {
    TAKE_PRIV_CMD => true, TAKEP_PRIV_CMD => true,
  }

  # An instance of IterationState is stored in @active_command to track
  # the progress of an ongoing read_all, take_all, monitor, or consume.
  # These iteration operations return one tuple at a time and only execute
  # the next iteration upon request from the client.
  #
  # We also use this state for the stream operations (monitor_stream and
  # consume_stream), though only to track the request so that it may be
  # cancelled by the client.
  IterationState = Struct.new :template, :cursor, :request
  
  attr_reader :sock, :flags, :public_port, :private_port

  private #================================================================

  def initialize(flags, client_id, space, public_port, private_port, sock)
    $log.info "created channel %d @ %#x (pubp=%#x, privp=%#x)", client_id,
      object_id, public_port, private_port
    @flags = flags  # ChannelFlags
    @flags.is_global = Port.global? public_port
    @flags.is_commons = Port.commons? public_port
    @client_id = client_id
    @space = space

    @public_port = public_port
    @private_port = private_port
    @peer_port = @public_port
    @address_book = []  # handle - FIRST_USER_HANDLE => private peer port

    @sock = sock
    @protocol = 0

    # State for read and write directions of the client socket; @read_buffer
    # only applies to reading, and @write_queue only applies to writing.
    @read_buffer = ReadBuffer.new
    @write_queue = List.new  # [WriteBuffer]
    @messages = []  # complete raw messages from the client read in read_data

    # The code (e.g., TAKE_CMD) and state of the currently running "slow"
    # command (that is, a command that may block waiting for a response).
    #
    # It is a protocol error for a client to issue multiple commands without
    # waiting for responses.  The only exception is the CANCEL_CMD, which
    # can be issued before a response is received for any of the blocking
    # operations.  Normally, a client won't issue multiple commands, since
    # Marinda::Client enforces this protocol.  However, Channel should also
    # enforce this protocol to deal with malicious/buggy clients.
    #
    # See further comments at client_message.
    #
    # This variable will be either nil or [command_code, state], where
    # {command_code} is TAKE_CMD, etc, and {state} is any data needed
    # by the active command to resume processing after some event it is
    # waiting for.
    @active_command = nil
    @active_reqnum = nil

    @dispatch = []
    #@dispatch[INVALID_CMD]
    @dispatch[HELLO_CMD] = method :hello
    @dispatch[WRITE_CMD] = method :write
    @dispatch[REPLY_CMD] = method :reply
    @dispatch[REMEMBER_CMD] = method :remember_peer
    @dispatch[FORGET_CMD] = method :forget_peer
    @dispatch[WRITE_TO_CMD] = method :write_to
    @dispatch[FORWARD_TO_CMD] = method :forward_to
    @dispatch[PASS_ACCESS_TO_CMD] = method :pass_access_to
    @dispatch[READ_CMD] = method :execute_singleton_command
    @dispatch[READP_CMD] = method :execute_singleton_command
    @dispatch[TAKE_CMD] = method :execute_singleton_command
    @dispatch[TAKEP_CMD] = method :execute_singleton_command
    @dispatch[TAKE_PRIV_CMD] = method :execute_singleton_command
    @dispatch[TAKEP_PRIV_CMD] = method :execute_singleton_command
    @dispatch[READ_ALL_CMD] = method :execute_iteration_command
    @dispatch[TAKE_ALL_CMD] = method :execute_iteration_command
    @dispatch[MONITOR_CMD] = method :execute_iteration_command
    @dispatch[CONSUME_CMD] = method :execute_iteration_command
    @dispatch[MONITOR_STREAM_CMD] = method :execute_stream_command
    @dispatch[CONSUME_STREAM_CMD] = method :execute_stream_command
    @dispatch[NEXT_CMD] = method :next_value
    @dispatch[CANCEL_CMD] = method :cancel
    @dispatch[CREATE_NEW_BINDING_CMD] = method :create_new_binding
    @dispatch[DUPLICATE_CHANNEL_CMD] = method :duplicate_channel
    @dispatch[CREATE_GLOBAL_COMMONS_CHANNEL_CMD] =
      method :create_global_commons_channel
    @dispatch[OPEN_PORT_CMD] = method :open_port
  end


  def fail_privilege
    raise ChannelPrivilegeError, "privilege violation"
  end

  def fail_protocol
    raise ChannelError, "protocol error"
  end

  def fail_connection
    raise ChannelError, "client disconnected"
  end


  #----------------------------------------------------------------------

  def handle_client_message(payload, fd)
    fail_connection unless payload

    $log.debug "Channel#handle_client_message: payload=%p",
      payload if $debug_client_commands
    reqnum = payload.unpack("C").first
    command, *arguments = decode_command reqnum, payload[1..-1], fd
    $log.debug "Channel#handle_client_message: received command %d (%s)",
      command, CLIENT_COMMANDS[command] if $debug_client_commands

    fail_protocol if command == INVALID_CMD

    # It is a protocol error for a client to issue multiple commands without
    # waiting for responses.
    if @active_command
      fail_protocol unless command == NEXT_CMD || command == CANCEL_CMD

      # We reject a NEXT_CMD if an iteration operation has not yielded a
      # tuple yet (that is, a NEXT_CMD can only be issued after receiving a
      # tuple).  This condition also disallows NEXT_CMD for all streaming
      # operations, since streaming operations always store the request in
      # @active_command; this is by design since NEXT_CMD is unnecessary
      # for streaming operations.
      fail_protocol if command == NEXT_CMD && @active_command[1].request

      fail_protocol unless reqnum == @active_reqnum
    else
      # Tolerate any bugs in Marinda::Client (or a malicious client).
      return if command == NEXT_CMD || command == CANCEL_CMD

      @active_reqnum = reqnum
    end

    @dispatch[command].call(reqnum, command, *arguments)
  end


  def handle_singleton_result(reqnum, command, request, tuple)
    if request
      @active_command = [command, request]
    else
      @active_command = nil
      send_tuple reqnum, tuple
    end
  end


  def handle_iteration_result(reqnum, command, template, request, tuple)
    if request
      $log.debug "Channel#handle_iteration_result: request=%p",
        request if $debug_client_commands
      @active_command = [command, IterationState[template, nil, request]]
    elsif tuple
      # For stream commands, just leave the request in @active_command so
      # that we properly reject NEXT_CMD in handle_client_message.
      unless command == MONITOR_STREAM_CMD || command == CONSUME_STREAM_CMD
        @active_command = [command, IterationState[template, tuple.seqnum, nil]]
      end
      send_tuple reqnum, tuple
    else
      @active_command = nil
      send_tuple reqnum, nil
    end
  end


  def cancel_active_command
    return unless @active_command
    command, state = @active_command
    case command
    when READ_CMD, READP_CMD, TAKE_CMD, TAKEP_CMD
      @space.region_cancel @public_port, state

    when TAKE_PRIV_CMD, TAKEP_PRIV_CMD
      @space.region_cancel @private_port, state

    when READ_ALL_CMD, TAKE_ALL_CMD, MONITOR_CMD, CONSUME_CMD,
      MONITOR_STREAM_CMD, CONSUME_STREAM_CMD  # NEXT_CMD is not allowed
      @space.region_cancel @public_port, state.request if state.request

    when CREATE_NEW_BINDING_CMD, DUPLICATE_CHANNEL_CMD,
	CREATE_GLOBAL_COMMONS_CHANNEL_CMD, OPEN_PORT_CMD
      # can't cancel: state == lambda

    else
      fail "INTERNAL ERROR: unexpected command '#{command}' in @active_command"
    end
  end


  def hello(reqnum, command, client_protocol, client_banner)
    if client_protocol <= 2 || client_protocol > PROTOCOL_VERSION
      raise ChannelProtocolUnsupportedError,
       "protocol version not supported; must be >= 3 and <= #{PROTOCOL_VERSION}"
    else
      name_len = @space.node_name.length
      banner = "Marinda Ruby local server v#{Marinda::VERSION}"
      banner_len = banner.length
      @protocol = [ PROTOCOL_VERSION, client_protocol ].min
      send reqnum, "CCNNGnna#{name_len}na#{banner_len}", HELLO_RESP, @protocol,
        @flags.flags, @client_id, @space.run_id, @space.node_id,
        name_len, @space.node_name, banner_len, banner
    end
  end


  def write(reqnum, command, tuple)
    $log.debug "client write(%p)", tuple if $debug_client_commands
    fail_privilege unless @flags.can_write?
    @space.region_write @public_port, tuple, self
    send_ack reqnum
  end


  def reply(reqnum, command, tuple)
    $log.debug "client reply(%p)", tuple if $debug_client_commands
    fail_privilege unless @flags.can_write?
    @space.region_write @peer_port, tuple, self
    send_ack reqnum
  end


  def remember_peer(reqnum, command)
    $log.debug "client remember_peer" if $debug_client_commands
    i = @address_book.index(nil) || @address_book.length
    if i > PEER_HANDLE_MAX
      send_error reqnum, ERRORSUB_NO_SPACE, "too many remembered peers"
    else
      @address_book[i] = @peer_port
      send reqnum, "CN", HANDLE_RESP, (i + FIRST_USER_HANDLE)
    end
  end


  def forget_peer(reqnum, command, peer)
    $log.debug "client forget_peer(%p)", peer if $debug_client_commands
    peer -= FIRST_USER_HANDLE
    @address_book[peer] = nil unless peer < 0 || peer >= @address_book.length
    send_ack reqnum
  end


  def write_to(reqnum, command, peer, tuple)
    $log.debug "client write_to(%p, %p)", peer, tuple if $debug_client_commands
    fail_privilege unless @flags.can_write?
    port = get_peer_port peer
    @space.region_write port, tuple, self if port
    send_ack reqnum
  end


  def forward_to(reqnum, command, peer, tuple)
    $log.debug "client forward_to(%p, %p)",
      peer, tuple if $debug_client_commands
    fail_privilege unless @flags.can_write?
    port = get_peer_port peer
    if port
      tuple.forwarder = @private_port
      @space.region_write port, tuple, self
    end
    send_ack reqnum
  end


  def pass_access_to(reqnum, command, peer, tuple, fd)
    $log.debug "client pass_access_to(%p, %p, %p)",
      peer, tuple, fd if $debug_client_commands

    # NOTE: Check for privilege now *after* doing recv_io, so that the passed
    #       file descriptor won't be stuck in limbo.
    unless @flags.can_write? && @flags.can_pass_access?
      fd.close rescue nil
      fail_privilege
    end

    port = get_peer_port peer
    if port
      if Port.global? port
	send_error reqnum, ERRORSUB_NOT_SUPPORTED,
	  "passing file descriptor not supported in global region"
	fd.close rescue nil
	return
      else
	tuple.access_fd = fd
	@space.region_write port, tuple, self
      end
    else
      fd.close rescue nil
    end
    send_ack reqnum
  end


  # executes: read, readp, take, takep, take_priv, takep_priv
  def execute_singleton_command(reqnum, command, template)
    if $debug_client_commands
      $log.debug "client %s(%p)", CLIENT_COMMANDS[command], template
    end
    case COMMAND_PRIV_NEEDED[command]
    when :read
      fail_privilege unless @flags.can_read?
    when :take
      fail_privilege unless @flags.can_take?
    end

    port = (COMMAND_ON_PRIVATE_REGION[command] ? @private_port : @public_port)
    request, tuple = @space.region_singleton_operation REGION_METHOD[command],
      port, template, self
    handle_singleton_result reqnum, command, request, tuple
  end


  # executes: read_all, take_all, monitor, consume
  def execute_iteration_command(reqnum, command, template)
    if $debug_client_commands
      $log.debug "client %s(%p)", CLIENT_COMMANDS[command], template
    end
    case COMMAND_PRIV_NEEDED[command]
    when :read
      fail_privilege unless @flags.can_read?
    when :take
      fail_privilege unless @flags.can_take?
    end

    port = (COMMAND_ON_PRIVATE_REGION[command] ? @private_port : @public_port)
    request, tuple = @space.region_iteration_operation REGION_METHOD[command],
      port, template, self
    handle_iteration_result reqnum, command, template, request, tuple
  end


  # executes: monitor_stream, consume_stream
  def execute_stream_command(reqnum, command, template)
    if $debug_client_commands
      $log.debug "client %s(%p)", CLIENT_COMMANDS[command], template
    end
    case COMMAND_PRIV_NEEDED[command]
    when :read
      fail_privilege unless @flags.can_read?
    when :take
      fail_privilege unless @flags.can_take?
    end

    port = (COMMAND_ON_PRIVATE_REGION[command] ? @private_port : @public_port)

    # First, set up the request in @active_command.
    request, tuple = @space.region_stream_operation REGION_METHOD[command],
      port, template, self
    handle_iteration_result reqnum, command, template, request, tuple

    # Stream over all existing matching tuples.
    #
    # This process is separated from the setup so that we can take
    # advantage of the existing mechanisms (e.g., Channel#region_result
    # callback) for satisfying blocking operations.
    #
    # Note: This call is a no-op on global regions, since the global server
    #       will automatically take care of starting up the streaming.
    #       LocalSpace takes care of making this a no-op so that the code
    #       can stay simple here.
    @space.region_stream_operation REGION_STREAM_START_METHOD[command],
      port, template, self
  end


  # Note: This should not be called (or allowed) for stream operations.
  def next_value(reqnum, _command)
    $log.debug "client next_value(%d)", reqnum if $debug_client_commands
    command, state = @active_command
    $log.debug "client next_value: state=%p", state if $debug_client_commands
    request, tuple = @space.region_iteration_operation REGION_METHOD[command],
      @public_port, state.template, self, state.cursor
    handle_iteration_result reqnum, command, state.template, request, tuple
  end


  def cancel(reqnum, command)
    $log.debug "client cancel(%d)", reqnum if $debug_client_commands
    cancel_active_command()
    @active_command = nil
  end


  def create_new_binding(reqnum, command)
    $log.debug "client create_new_binding" if $debug_client_commands
    @active_command = [command,
      lambda do |client_sock|
	if client_sock
	  send_access_right reqnum, client_sock
	else
	  send_error reqnum, ERRORSUB_NO_SPACE,
	    "couldn't create new channel binding"
	end
	@active_command = nil
      end ]

    @space.create_new_binding self
  end


  def duplicate_channel(reqnum, command)
    $log.debug "client duplicate_channel" if $debug_client_commands
    @active_command = [command,
      lambda do |client_sock|
	if client_sock
	  send_access_right reqnum, client_sock
	else
	  send_error reqnum, ERRORSUB_NO_SPACE, "couldn't duplicate channel"
	end
	@active_command = nil
      end ]

    @space.duplicate_channel self
  end


  def create_global_commons_channel(reqnum, command)
    $log.debug "client create_global_commons_channel" if $debug_client_commands
    @active_command = [command,
      lambda do |client_sock|
	if client_sock
	  send_access_right reqnum, client_sock
	else
	  send_error reqnum, ERRORSUB_NO_SPACE,
	    "couldn't create global commons channel"
	end
	@active_command = nil
      end ]

    flags = ChannelFlags.new
    flags.allow_commons_privileges!
    @space.open_port self, flags, Port::COMMONS_PORTNUM, true
  end


  # {portnum} should be a port number (not the full port value).
  #  
  # This opens a public port with this port number in the scope implied
  # by
  # (local or global) as the current channel.  The corresponding private
  # port will be created at the same time.
  #
  # {portnum} can be 0 to open the next available port number.
  #
  # Technically, we could allow anyone to open Port::COMMONS_PORTNUM with
  # this method, even without can_open_any_port privileges, but we disallow
  # it because a commons port opened with this method may have greater
  # privileges than one opened with create_global_commons_channel.
  # You can think of the can_open_any_port privileges as granting permission
  # to invoke the open_port *method* regardless of the actual port number
  # requested.
  def open_port(reqnum, command, portnum, want_global)
    $log.debug "client open_port(%d)", portnum if $debug_client_commands
    fail_privilege unless @flags.can_open_any_port?

    @active_command = [command,
      lambda do |client_sock|
	if client_sock
	  send_access_right reqnum, client_sock
	else
	  send_error reqnum, ERRORSUB_NO_SPACE, "couldn't open port"
	end
	@active_command = nil
      end ]

    # XXX think some more about what flags should be used in this call
    @space.open_port self, @flags, portnum, want_global
  end


  #--------------------------------------------------------------------------

  def decode_command(reqnum, payload, fd)
    code = payload.unpack("C").first
    case code
    when HELLO_CMD
      return payload.unpack("CCa*")

    when WRITE_CMD, REPLY_CMD
      values_mio = payload[1 ... payload.length]
      return [ code, Tuple.new(@private_port, values_mio) ]

    when REMEMBER_CMD, CREATE_NEW_BINDING_CMD, DUPLICATE_CHANNEL_CMD,
	CREATE_GLOBAL_COMMONS_CHANNEL_CMD
      return [ code ]

    when FORGET_CMD
      return payload.unpack("CN")

    when WRITE_TO_CMD, FORWARD_TO_CMD, PASS_ACCESS_TO_CMD
      peer = payload[1..4].unpack("N").first
      port = (code == FORWARD_TO_CMD ? @peer_port : @private_port)
      values_mio = payload[5 ... payload.length]
      retval = [ code, peer, Tuple.new(port, values_mio) ]
      retval << fd if code == PASS_ACCESS_TO_CMD
      return retval

    when READ_CMD, READP_CMD, TAKE_CMD, TAKEP_CMD, TAKE_PRIV_CMD,
	TAKEP_PRIV_CMD, READ_ALL_CMD, TAKE_ALL_CMD, MONITOR_CMD, CONSUME_CMD,
        MONITOR_STREAM_CMD, CONSUME_STREAM_CMD
      values_mio = payload[1 ... payload.length]
      template = Template.new @private_port, values_mio
      template.reqnum = reqnum
      return [ code, template ]

    when NEXT_CMD, CANCEL_CMD
      return [ code ]

    when OPEN_PORT_CMD
      retval = payload.unpack("CwC")
      retval[2] = (retval[2] != 0)  # turn want_global into a boolean
      return retval

    else
      return [ INVALID_CMD,
	"protocol error: unknown command code #{code} from client",
	payload ]
    end
  end


  def send_ack(reqnum)
    send reqnum, "C", ACK_RESP
  end


  # {tuple} should be instance of Tuple or nil
  def send_tuple(reqnum, tuple)
    case
    when tuple && tuple.access_fd
      @peer_port = tuple.sender
      send_with_fd reqnum, tuple.access_fd,
	"Ca*", TUPLE_WITH_RIGHTS_RESP, tuple.values_mio
      tuple.access_fd = nil
    when tuple
      @peer_port = tuple.sender
      send reqnum, "Ca*", TUPLE_RESP, tuple.values_mio
    else
      send reqnum, "C", TUPLE_NIL_RESP
    end
    true
  end


  def send_access_right(reqnum, fd)
    send_with_fd reqnum, fd, "C", ACCESS_RIGHT_RESP
  end


  def send_error(reqnum, code, msg)
    send reqnum, "CCa*", ERROR_RESP, code, msg
  end


  def send(reqnum, format, *values)
    send_with_fd reqnum, nil, format, *values
  end


  def send_with_fd(reqnum, fd, format, *values)
    payload = [ reqnum ].pack("C") + values.pack(format)
    length = [ payload.length ].pack "n"
    message = length + payload
    @write_queue << WriteBuffer[message, fd]
  end


  def get_peer_port(peer)
    if peer < 0
      nil
    elsif peer < FIRST_USER_HANDLE
      @space.lookup_service peer
    elsif peer < FIRST_USER_HANDLE + @address_book.length
      @address_book[peer - FIRST_USER_HANDLE]
    else
      nil
    end
  end


  public #==================================================================

  # Called by the select loop in LocalSpace -------------------------------

  def need_io_read
    @sock != nil
  end


  def need_io_write
    @sock && !@write_queue.empty?
  end


  # I/O events from LocalSpace --------------------------------------------

  # NOTE: This only works with Unix domain sockets, not general TCP sockets.
  def read_data(timestamp)
    return unless @sock  # still connected

    shutdown_connection = false
    @messages.clear  # [ [payload, file] ]
    @read_buffer.payload ||= ""

    begin
      if @read_buffer.want_io
	fd = @sock.recv_io
	raise EOFError, "client protocol error: no file descriptor" unless fd
	@messages << [@read_buffer.payload, fd]
	@read_buffer.length = nil
	@read_buffer.payload = ""
	@read_buffer.want_io = nil
      else
	data = @sock.read_nonblock READ_SIZE
        $log.debug "Channel#read_data from %p: %p",
          @sock, data if $debug_client_io_bytes

	start = 0
	while start < data.length
	  data_left = data.length - start

	  desired_length = @read_buffer.length || CLIENT_MESSAGE_LENGTH_SIZE
	  fill_amount = desired_length - @read_buffer.payload.length
	  usable_amount = [fill_amount, data_left].min

	  @read_buffer.payload << data[start, usable_amount]
	  start += usable_amount

	  if usable_amount == fill_amount
	    if @read_buffer.length
              if $debug_client_io_bytes
                $log.debug "Channel#read_data: @read_buffer.length = %d",
                  @read_buffer.length
                $log.debug "Channel#read_data: @read_buffer.payload = %p",
                  @read_buffer.payload
              end

              code = @read_buffer.payload.unpack("CC")[1]
	      if code == ChannelMessageCodes::PASS_ACCESS_TO_CMD
		if data.length - start > 0
		  raise EOFError, "client protocol error: no file descriptor"
		end
		@read_buffer.want_io = true
	      else
		@messages << [@read_buffer.payload, nil]
		@read_buffer.length = nil
		@read_buffer.payload = ""
	      end
	    else
	      @read_buffer.length = @read_buffer.payload.unpack("n").first
	      @read_buffer.payload = ""
              if $debug_client_io_bytes
                $log.debug "Channel#read_data: message length = %d",
                  @read_buffer.length
              end
	      if @read_buffer.length == 0
		raise EOFError, "client protocol error: message length == 0"
	      end
	    end
	  end
	end
      end

    rescue Errno::EINTR  # might be raised by read_nonblock
      # do nothing, since we'll automatically retry in the next select() round

    # Ruby 1.9.2 preview 2 uses IO::WaitReadable, but earlier versions use
    # plain Errno::EWOULDBLOCK, so technically we could get rid of
    # IO::WaitReadable.
    rescue Errno::EWOULDBLOCK, IO::WaitReadable
      # IO::WaitReadable shouldn't normally happen since the socket was
      # ready by the time we performed the read_nonblock; however, a false
      # readiness notification can lead to this exception.
      $log.info "Channel#read_data from %p: IO::WaitReadable", @sock
      # do nothing, since we'll automatically retry in the next select() round

    # in `recv_io': file descriptor was not passed
    # (msg_controllen : 0 != 16) (SocketError)
    rescue SocketError, IOError, EOFError, SystemCallError
      if $debug_client_io_bytes || !$!.kind_of?(EOFError)
        $log.info "Channel#read_data from %p: %p", @sock, $!
      end

      # Don't immediately shutdown because there may be messages read from
      # the client that must still be processed.  For example, a client can
      # perform a series of tuple space write operations and then
      # disconnect, and the resulting legitimate EOF would trigger a
      # shutdown of the connection which should be delayed until all
      # operations are processed.
      #
      # Note: Because we reset the connection on EOF, we don't allow half
      #       closing of sockets.
      shutdown_connection = true
    end

    reqnum = nil
    begin
      @messages.each do |payload, file|
        reqnum = payload.unpack("C").first
        handle_client_message payload, file
      end

    rescue ChannelPrivilegeError
      $log.debug "Channel#handle_client_message raised %p", $!
      send_error reqnum, ERRORSUB_NO_PRIVILEGE, $!.to_s
      shutdown()

    rescue ChannelProtocolUnsupportedError
      $log.debug "Channel#handle_client_message raised %p", $!
      send_error reqnum, ERRORSUB_PROTOCOL_NOT_SUPPORTED, $!.to_s
      shutdown()

    rescue ChannelError
      $log.info "Channel#handle_client_message raised %p", $!
      $log.info "Channel active_command=%p", @active_command
      shutdown()

    else
      shutdown() if shutdown_connection
    end
  end


  # NOTE: This only works with Unix domain sockets, not general TCP sockets.
  def write_data(timestamp)
    return unless @sock  # still connected

    buffer = @write_queue.first

    begin
      if buffer.payload.length > 0
	n = @sock.write_nonblock buffer.payload
	data_written = buffer.payload.slice! 0, n
        if $debug_client_io_bytes
          $log.debug "Channel#write_data to %p: wrote %d bytes, %d left: %p",
            @sock, n, buffer.payload.length, data_written
        end
      end

      if buffer.payload.length == 0 && buffer.file
        $log.debug "Channel#write_data passing file %p over %p",
          buffer.file, @sock if $debug_client_io_bytes

	@sock.send_io buffer.file
        $log.debug "Channel#write_data sent file %p over %p",
          buffer.file, @sock if $debug_client_io_bytes

        # XXX Should we close the file here??  Is there a race condition,
        #     with the client possibly receiving a closed file descriptor?
	buffer.file.close rescue nil
	buffer.file = nil
      end

      @write_queue.shift if buffer.payload.length == 0 && buffer.file == nil

    rescue Errno::EINTR  # might be raised by write_nonblock
      # do nothing, since we'll automatically retry in the next select() round

    # Ruby 1.9.2 preview 2 uses IO::WaitWritable, but earlier versions use
    # plain Errno::EWOULDBLOCK, so technically we could get rid of
    # IO::WaitWritable.
    rescue Errno::EWOULDBLOCK, IO::WaitWritable
      # IO::WaitWritable shouldn't normally happen since the socket was
      # ready by the time we performed the write_nonblock; however, a false
      # readiness notification can lead to this exception.
      $log.info "Channel#write_data to %p: IO::WaitWritable", @sock
      # do nothing, since we'll automatically retry in the next select() round

    # syswrite may throw "not opened for writing (IOError)";
    rescue SocketError, IOError, SystemCallError # including Errno::EPIPE
      if $debug_client_io_bytes || !$!.kind_of?(Errno::EPIPE)
        $log.info "Channel#write_data to %p: %p", @sock, $!
      end
      shutdown()
    end
  end


  # Called by self on EOF or some I/O error with the client, or by
  # LocalSpace when shutting down the local server.
  def shutdown
    return unless @sock
    $log.info "channel %d @ %#x: shutting down", @client_id, object_id
    cancel_active_command()
    @space.unregister_channel self

    @write_queue.each do |buffer|
      if buffer.file
        buffer.file.close rescue nil
        buffer.file = nil
      end
    end
    @write_queue.clear

    @sock.close rescue nil
    @sock = nil
  end


  # Events from Region ----------------------------------------------------

  def region_result(port, operation, template, tuple)
    $log.debug "Channel#region_result(%p, %p)",
      operation, tuple if $debug_client_commands
    send_tuple template.reqnum, tuple

    # Don't update @active_command for stream commands.  Instead, just
    # leave the request in @active_command so that we properly reject
    # NEXT_CMD in handle_client_message.
    return if operation == :monitor_stream ||  operation == :consume_stream

    # We should set @active_command to nil if
    #
    #  * a read_all or take_all completes (that is, tuple == nil), or
    #  * a normally non-blocking operation (e.g., readp) that had blocked
    #    waiting on results from the global server completes (regardless
    #    of whether a tuple was returned).
    @active_command = nil

    if tuple
      case operation
      when :read_all, :take_all, :monitor, :consume, :next
	state = IterationState[template, tuple.seqnum, nil]
	@active_command = [OPERATION_TO_COMMAND[operation], state]
      end
    end

    $log.debug "Channel#region_result: active_command=%p",
      @active_command if $debug_client_commands
  end


  # Events from LocalSpace ------------------------------------------------

  def binding_created(new_channel, client_sock)
    @active_command[1].call client_sock
  end


  def channel_duplicated(new_channel, client_sock)
    @active_command[1].call client_sock
  end


  def port_opened(new_channel, client_sock)
    @active_command[1].call client_sock
  end


  # -----------------------------------------------------------------------

  def inspect
    sprintf "\#<Marinda::Channel:%#x @public_port=%#x, " +
      "@private_port=%#x>", object_id, @public_port, @private_port
  end

end

end  # module Marinda
