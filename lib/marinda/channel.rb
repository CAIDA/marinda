#############################################################################
## A single connection between a client and the local server.
##
## A Channel object exists in the local server process.  For the
## object on the client side of the connection, see Marinda::Client.
##
## A Channel object is associated with a pair of public and private regions.
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
## $Id: channel.rb,v 1.45 2009/03/16 23:33:59 youngh Exp $
#############################################################################

require 'ostruct'
require 'yaml'
require 'thread'
require 'socket'

require 'marinda/msgcodes'
require 'marinda/flagsets'
require 'marinda/port'
require 'marinda/tuple'

module Marinda

class Channel

  class ChannelError < RuntimeError; end
  class ChannelPrivilegeError < ChannelError; end

  include ChannelMessageCodes

  CVS_ID = "$Id: channel.rb,v 1.45 2009/03/16 23:33:59 youngh Exp $"
  PEER_HANDLE_MAX = 2**31 - 1   # max value storable with Array#pack("N")
  OPERATION_TO_COMMAND = {
    :monitor => MONITOR_CMD, :read_all => READ_ALL_CMD, :next => NEXT_CMD
  }
  REGION_METHOD = {
    READ_CMD => :read, READP_CMD => :readp,
    TAKE_CMD => :take, TAKEP_CMD => :takep,
    TAKE_PRIV_CMD => :take, TAKEP_PRIV_CMD => :takep,
    MONITOR_CMD => :monitor, READ_ALL_CMD => :read_all
  }
  COMMAND_PRIV_NEEDED = {
    READ_CMD => :read, READP_CMD => :read,
    TAKE_CMD => :take, TAKEP_CMD => :take,
    TAKE_PRIV_CMD => :none, TAKEP_PRIV_CMD => :none,
    MONITOR_CMD => :read, READ_ALL_CMD => :read
  }
  COMMAND_ON_PRIVATE_REGION = {
    TAKE_PRIV_CMD => true, TAKEP_PRIV_CMD => true,
  }

  # The iteration state stored in @active_command when monitor or read_all
  # returns one tuple value and is ready for the next iteration.
  IterationState = Struct.new :template, :cursor, :request
  
  attr_reader :alive, :flags, :public_port, :private_port

  private #================================================================

  def initialize(flags, space, public_port, private_port, sock, clientio)
    $log.info "created channel %#x (pubp=%#x, privp=%#x)", object_id,
      public_port, private_port
    @alive = true   # true if the channel has not shut down
    @protocol = 0
    @flags = flags  # ChannelFlags
    @flags.is_global = Port.global? public_port
    @flags.is_commons = Port.commons? public_port
    @space = space
    @public_port = public_port
    @private_port = private_port
    @peer_port = @public_port
    @address_book = []  # handle - FIRST_USER_HANDLE => private peer port
    @sock = sock
    @clientio = clientio

    # The code (e.g., WRITE_CMD) and state of the currently running command.
    #
    # It is a protocol error for a client to issue multiple commands without
    # waiting for responses.  The only exception is the CANCEL_CMD, which
    # can be issued before a monitor or read_all response.  Normally, a client
    # won't issue multiple commands, since Marinda::Client enforces this
    # protocol.  However, Channel should also enforce this protocol to deal
    # with malicious/buggy clients.
    #
    # See further comments at client_message.
    #
    # This variable will be either nil or [command_code, state], where
    # {command_code} is WRITE_CMD, etc, and {state} is any data needed
    # by the active command to resume processing after some event it is
    # waiting for.
    @active_command = nil

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
    @dispatch[MONITOR_CMD] = method :execute_iteration_command
    @dispatch[READ_ALL_CMD] = method :execute_iteration_command
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

  def handle_client_message(sender, payload, fd)
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
      fail_protocol unless [NEXT_CMD, CANCEL_CMD].include? command
      fail_protocol if command == NEXT_CMD && @active_command[1].request
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
      @active_command = [command, IterationState[template, tuple.seqnum, nil]]
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

    when MONITOR_CMD, READ_ALL_CMD, NEXT_CMD
      @space.region_cancel @public_port, state.request if state.request

    when CREATE_NEW_BINDING_CMD, DUPLICATE_CHANNEL_CMD,
	CREATE_GLOBAL_COMMONS_CHANNEL_CMD, OPEN_PORT_CMD
      # can't cancel: state == lambda

    else
      fail "INTERNAL ERROR: unexpected command '#{command}' in @active_command"
    end
  end


  def hello(reqnum, command, client_protocol, client_banner)
    @protocol = [ PROTOCOL_VERSION, client_protocol ].min
    send reqnum, "CCNa*", HELLO_RESP, @protocol, @flags.flags,
      "Ruby server: #{CVS_ID}"
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


  # executes: monitor, read_all
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
    cancel_active_command
    @active_command = nil
  end


  def create_new_binding(reqnum, command)
    $log.debug "client create_new_binding" if $debug_client_commands
    @space.create_new_binding self    
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
  end


  def duplicate_channel(reqnum, command)
    $log.debug "client duplicate_channel" if $debug_client_commands
    @space.duplicate_channel self
    @active_command = [command,
      lambda do |client_sock|
	if client_sock
	  send_access_right reqnum, client_sock
	else
	  send_error reqnum, ERRORSUB_NO_SPACE, "couldn't duplicate channel"
	end
	@active_command = nil
      end ]
  end


  def create_global_commons_channel(reqnum, command)
    $log.debug "client create_global_commons_channel" if $debug_client_commands
    flags = ChannelFlags.new
    flags.allow_commons_privileges!
    @space.open_port self, flags, Port::COMMONS_PORTNUM, true
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

    # XXX think some more about what flags should be used in this call
    @space.open_port self, @flags, portnum, want_global
    @active_command = [command,
      lambda do |client_sock|
	if client_sock
	  send_access_right reqnum, client_sock
	else
	  send_error reqnum, ERRORSUB_NO_SPACE, "couldn't open port"
	end
	@active_command = nil
      end ]
  end


  #--------------------------------------------------------------------------

  def decode_command(reqnum, payload, fd)
    code = payload.unpack("C").first
    case code
    when HELLO_CMD
      return payload.unpack("CCa*")

    when WRITE_CMD, REPLY_CMD
      values = YAML.load payload[1 ... payload.length]
      return [ code, Tuple.new(@private_port, values) ]

    when REMEMBER_CMD, CREATE_NEW_BINDING_CMD, DUPLICATE_CHANNEL_CMD,
	CREATE_GLOBAL_COMMONS_CHANNEL_CMD
      return [ code ]

    when FORGET_CMD
      return payload.unpack("CN")

    when WRITE_TO_CMD, FORWARD_TO_CMD, PASS_ACCESS_TO_CMD
      peer = payload[1..4].unpack("N").first
      port = (code == FORWARD_TO_CMD ? @peer_port : @private_port)
      values = YAML.load payload[5 ... payload.length]
      retval = [ code, peer, Tuple.new(port, values) ]
      retval << fd if code == PASS_ACCESS_TO_CMD
      return retval

    when READ_CMD, READP_CMD, TAKE_CMD, TAKEP_CMD, TAKE_PRIV_CMD,
	TAKEP_PRIV_CMD, MONITOR_CMD, READ_ALL_CMD
      values = YAML.load payload[1 ... payload.length]
      template = Template.new @private_port, values
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
	"Ca*", TUPLE_WITH_RIGHTS_RESP, YAML.dump(tuple.values)
      tuple.access_fd = nil
    when tuple
      @peer_port = tuple.sender
      send reqnum, "Ca*", TUPLE_RESP, YAML.dump(tuple.values)
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
    @clientio.write_message @space, self, @sock, message, fd, nil
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

  # Called by self or LocalSpace.
  def shutdown
    $log.info "Channel %#x: shutting down", object_id

    cancel_active_command
    @clientio.remove_client @space, self, @sock
    @space.unregister_channel self

    @alive = false
    @space = nil
    @public_port = nil
    @private_port = nil
    @peer_port = nil
    @address_book = nil
    @sock.close rescue nil  # XXX close it here?
    @sock = nil
    @clientio = nil
  end


  # NOTE: Because Channel isn't a thread, Channel directly performs an
  #       operation whenever a message arrives rather than defering the
  #       execution via internal message queuing.
  #
  #       All messages are supposed to be delivered indirectly via
  #       LocalSpace or GlobalSpace (which are threads) rather than directly
  #       from the source of the messages (e.g., from ClientIO or Region).
  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  # Events from ClientIO ---------------------------------------------------

  def client_message(sender, payload, fd)
    begin
      handle_client_message sender, payload, fd
    rescue ChannelPrivilegeError
      $log.debug "Channel#client_message: %p", $!
      reqnum = payload.unpack("C").first
      send_error reqnum, ERRORSUB_NO_PRIVILEGE, $!.to_s
    rescue ChannelError
      $log.info "Channel#client_message: %p", $!
      $log.info "Channel#client_message: active_command=%p", @active_command
      shutdown
    end
  end


  def message_written(sender, request)
    # nothing to do
  end


  def message_write_failed(sender, request)
    # XXX ensure files/channels of all unsent messsages are closed somehow
    $log.debug "Channel#message_write_failed: shutting down channel %#x",
      self.object_id
    $log.debug "Channel#message_write_failed: active_command=%p",
      @active_command
    shutdown
  end


  # Events from Region ----------------------------------------------------

  def region_result(port, operation, template, tuple)
    $log.debug "Channel#region_result(%p, %p)",
      operation, tuple if $debug_client_commands
    send_tuple template.reqnum, tuple

    @active_command = nil
    if tuple
      case operation
      when :monitor, :read_all, :next
	state = IterationState[template, tuple.seqnum, nil]
	@active_command = [OPERATION_TO_COMMAND[operation], state]
      end
    end

    $log.debug "Channel#region_result: active_command=%p",
      @active_command if $debug_client_commands
  end


  # Events from LocalSpace ------------------------------------------------

  def binding_created(sender, new_channel, client_sock)
    @active_command[1].call client_sock
  end


  def channel_duplicated(sender, new_channel, client_sock)
    @active_command[1].call client_sock
  end


  def port_opened(sender, new_channel, client_sock)
    @active_command[1].call client_sock
  end


  # -----------------------------------------------------------------------

  def inspect
    sprintf "\#<Marinda::Channel:%#x @public_port=%#x, " +
      "@private_port=%#x>", object_id, @public_port, @private_port
  end

end

end  # module Marinda
