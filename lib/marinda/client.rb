#############################################################################
## A single connection between a client and the local server.
##
## A Client object exists in the client process.  For the object on the
## server side of the connection, see Marinda::Channel.
##
## A Client object is associated with a pair of public and private regions.
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

require 'socket'
require 'yaml'

require 'marinda/msgcodes'
require 'marinda/flagsets'
require 'marinda/version'

module Marinda

class MarindaError < StandardError; end
class OperationError < MarindaError; end
class PrivilegeError < MarindaError; end
class ProtocolError < MarindaError; end
class ConnectionBroken < IOError; end

class Client

  include ChannelFlagReaders
  include ChannelMessageCodes

  # An arbitrarily chosen value representing the largest legal reqnum value
  # before wrapping.  This must be storable with Array#pack("C").
  REQNUM_MAX = 2**8 - 1

  attr_accessor :remote_banner, :debug

  def initialize(sock)
    # This attribute contains the request number used in the last *sent*
    # message rather than containing the next *available* reqnum.   Thus,
    # we must increment the attribute before sending a new message.  The
    # tuple space will use the same reqnum as a message in any response.
    #
    # A request number is only needed for one purpose, to properly discard
    # any response to a cancelled blocking operation.  Without operation
    # cancellation, a reqnum is not needed to match up a response to a
    # request since there can only be 1 operation in progress--that is,
    # a client normally sends a request and then blocks reading the
    # response.
    #
    # Because reqnum is only used for discarding responses, the range of
    # reqnum need not be large.
    @reqnum = 0  # wraps at REQNUM_MAX

    @flags = 0
    @protocol = 0
    @remote_banner = nil
    @sock = sock
    # XXX `setsockopt': Operation not supported on socket (Errno::EOPNOTSUPP)
    # @sock.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true
    @global_commons_channel = nil  # cached
    @debug = false
  end

  private #===============================================================

  # Passes out 'Broken pipe - send(2) (Errno::EPIPE)' as ConnectionBroken.
  def sock_send(message)
    begin
      length = message.length
      while length > 0
	sent = @sock.send message, 0
	length -= sent
	message = message[sent .. -1] if length > 0
      end
    rescue Errno::EPIPE
      raise ConnectionBroken, "lost connection: #{$!.class.name}: #{$!}"
    end
  end


  # Raises an exception if unable to receive the full requested amount.
  def sock_recv(length)
    begin
      retval = ""
      while length > 0
	data = @sock.recv length
	raise ConnectionBroken, "premature EOF" if data.empty?
	retval << data
	length -= data.length
      end
      retval
    rescue Errno::ECONNRESET
      raise ConnectionBroken, "lost connection: #{$!.class.name}: #{$!}"
    end
  end


  # Passes out 'Broken pipe - send(2) (Errno::EPIPE)' as ConnectionBroken.
  def sock_send_io(file)
    begin
      @sock.send_io file
    rescue Errno::EPIPE
      raise ConnectionBroken, "lost connection: #{$!.class.name}: #{$!}"
    end
  end


  # Returns an IO object on success or raises an exception on any error.
  #
  # If you know, a priori, that the file descriptor being received represents
  # a certain type of IO object (e.g., File or UNIXSocket), then you must
  # manually convert the returned IO object to a subclass using one of the
  # methods to_file or to_klass.  These methods tie the lifetime of the
  # original IO object to the lifetime of the newly-created object returned
  # by to_file and to_klass, so that garbage collection doesn't silently
  # close the underlying file descriptor at the wrong moment.
  #
  # UNIXSocket.recv_io can take a klass parameter, but this doesn't fully
  # eliminate the need for the to_klass hack because, oftentimes, the
  # caller doesn't know what class the IO object should be until sometime
  # after calling recv_io.
  def sock_recv_io
    begin
      io = @sock.recv_io
      if io
	class << io
	  def to_file
	    to_klass File
	  end

	  def to_klass(klass)
	    retval = klass.for_fd fileno
	    retval.instance_variable_set :@___original_file, self
	    retval
	  end
	end
	$stderr.printf "sock_recv_io: io=%p\n", io if @debug
	return io
      else
	raise OperationError, "failed to receive file descriptor"
      end
    rescue Errno::ECONNRESET
      raise ConnectionBroken, "lost connection: #{$!.class.name}: #{$!}"
    end
  end


  def send(format, *values)
    @reqnum = (@reqnum == REQNUM_MAX ? 1 : @reqnum + 1)
    $stderr.puts "send: reqnum=" + @reqnum.to_s if @debug
    send_with_reqnum @reqnum, format, values
  end


  def send_cont(format, *values)
    $stderr.puts "send_cont: reqnum=" + @reqnum.to_s if @debug
    send_with_reqnum @reqnum, format, values
  end


  def send_with_reqnum(reqnum, format, values)
    payload = [ reqnum ].pack("C") + values.pack(format)
    length = [ payload.length ].pack "n"
    message = length + payload
    sock_send message
  end


  def receive(*expected)
    payload, file = receive_response
    decode_response(payload, file, expected)
  end


  def receive_response
    loop do
      length_n = sock_recv 2
      length = length_n.unpack("n").first
      payload = sock_recv length

      recv_reqnum, code = payload.unpack("CC")
      $stderr.puts "received reqnum=" + recv_reqnum.to_s if @debug

      if code == TUPLE_WITH_RIGHTS_RESP || code == ACCESS_RIGHT_RESP
	file = sock_recv_io
      else
	file = nil
      end

      if recv_reqnum == @reqnum
	return payload[1..-1], file
      else
	file.close rescue nil if file
      end
    end
  end


  def decode_response(payload, file, expected)
    $stderr.printf "decode_response(payload=%p, file=%p, expected=%p)\n",
      payload, file, expected if @debug
    code = payload.unpack("C").first
    unless code == ERROR_RESP || expected.include?(code)
      file.close rescue nil if file
      raise ProtocolError, "unexpected response from server: #{code}"
    end

    case code
    when HELLO_RESP
      return payload.unpack("CCNa*")  # command, protocol, flags, message

    when ACK_RESP
      return payload.unpack("C")   # command

    when ERROR_RESP
      error = payload.unpack("CCa*")   # command, subcode, message
      raise OperationError, "#{error[1]}: #{error[2]}"

    when TUPLE_RESP, TUPLE_WITH_RIGHTS_RESP
      tuple = YAML.load payload[1 ... payload.length]
      tuple << file if file
      return [ code, tuple ]

    when TUPLE_NIL_RESP
      return [ TUPLE_RESP, nil ]

    when ACCESS_RIGHT_RESP
      return [ ACCESS_RIGHT_RESP, file ]

    when HANDLE_RESP
      return payload.unpack("CN")
    end
  end


  def receive_ack
    response = receive ACK_RESP
    return response[1]
  end


  def receive_tuple
    response = receive TUPLE_RESP, TUPLE_NIL_RESP, TUPLE_WITH_RIGHTS_RESP
    return response[1]
  end


  def receive_channel
    file = receive_access_right
    Client.new(file.to_klass(UNIXSocket))
  end


  def receive_access_right
    response = receive ACCESS_RIGHT_RESP
    return response[1]
  end


  def receive_handle
    response = receive HANDLE_RESP
    return response[1]
  end


  # Execute the iteration of read_all, take_all, monitor, consume,
  # monitor_stream, and consume_stream.
  def execute_iteration(needs_next, block)
    needs_cancel = false
    begin
      loop do
	tuple = receive_tuple
	break unless tuple

	needs_cancel = true
	block.call tuple
	needs_cancel = false

	send_cont "C", NEXT_CMD if needs_next
      end
    ensure
      send_cont "C", CANCEL_CMD if needs_cancel
    end
  end


  public #================================================================

  def hello
    send "CCa*", HELLO_CMD, PROTOCOL_VERSION,
      "Marinda Ruby client v#{Marinda::VERSION}"

    response = receive HELLO_RESP
    @protocol, @flags, @remote_banner = response[1..3]
    return "#{@remote_banner} (proto: #{@protocol})"
  end


  def write(tuple)
    raise PrivilegeError unless can_write?
    send "Ca*", WRITE_CMD, YAML.dump(tuple)
    receive_ack
    true
  end


  def reply(tuple)
    raise PrivilegeError unless can_write?
    send "Ca*", REPLY_CMD, YAML.dump(tuple)
    receive_ack
    true
  end


  def remember_peer
    send "C", REMEMBER_CMD
    receive_handle
  end


  def forget_peer(peer)
    send "CN", FORGET_CMD, peer
    receive_ack
    true
  end


  def write_to(peer, tuple)
    raise PrivilegeError unless can_write?
    send "CNa*", WRITE_TO_CMD, peer, YAML.dump(tuple)
    receive_ack
    true
  end


  def forward_to(peer, tuple)
    raise PrivilegeError unless can_write? and can_forward?
    send "CNa*", FORWARD_TO_CMD, peer, YAML.dump(tuple)
    receive_ack
    true
  end


  def pass_access_to(peer, file, tuple)
    fail "attempt to pass access rights in global tuple space" if is_global?
    raise PrivilegeError unless can_write? and can_pass_access?
    send "CNa*", PASS_ACCESS_TO_CMD, peer, YAML.dump(tuple)
    file = file.fileno if file.kind_of? IO
    sock_send_io file
    receive_ack
    true
  end


  def read(template)
    raise PrivilegeError unless can_read?
    send "Ca*", READ_CMD, YAML.dump(template)
    receive_tuple
  end


  def readp(template)
    raise PrivilegeError unless can_read?
    send "Ca*", READP_CMD, YAML.dump(template)
    receive_tuple
  end


  def take(template)
    raise PrivilegeError unless can_take?
    send "Ca*", TAKE_CMD, YAML.dump(template)
    receive_tuple
  end


  def takep(template)
    raise PrivilegeError unless can_take?
    send "Ca*", TAKEP_CMD, YAML.dump(template)
    receive_tuple
  end


  def take_priv(template)
    send "Ca*", TAKE_PRIV_CMD, YAML.dump(template)
    receive_tuple
  end


  def takep_priv(template)
    send "Ca*", TAKEP_PRIV_CMD, YAML.dump(template)
    receive_tuple
  end


  def read_all(template, &block)
    raise PrivilegeError unless can_read?
    send "Ca*", READ_ALL_CMD, YAML.dump(template)
    execute_iteration true, block
  end


  def take_all(template, &block)
    raise PrivilegeError unless can_take?
    send "Ca*", TAKE_ALL_CMD, YAML.dump(template)
    execute_iteration true, block
  end


  def monitor(template, &block)
    raise PrivilegeError unless can_read?
    send "Ca*", MONITOR_CMD, YAML.dump(template)
    execute_iteration true, block
  end


  def consume(template, &block)
    raise PrivilegeError unless can_take?
    send "Ca*", CONSUME_CMD, YAML.dump(template)
    execute_iteration true, block
  end


  def monitor_stream(template, &block)
    raise PrivilegeError unless can_read?
    send "Ca*", MONITOR_STREAM_CMD, YAML.dump(template)
    execute_iteration false, block
  end


  def consume_stream(template, &block)
    raise PrivilegeError unless can_take?
    send "Ca*", CONSUME_STREAM_CMD, YAML.dump(template)
    execute_iteration false, block
  end


  def new_binding
    send "C", CREATE_NEW_BINDING_CMD
    channel = receive_channel
    channel.hello
    channel
  end


  def duplicate
    send "C", DUPLICATE_CHANNEL_CMD
    channel = receive_channel
    channel.hello
    channel
  end


  def global_commons
    unless @global_commons_channel
      send "C", CREATE_GLOBAL_COMMONS_CHANNEL_CMD
      channel = receive_channel
      channel.hello
      @global_commons_channel = channel
    end
    @global_commons_channel 
  end


  # {portnum} should be a port number (not the full port value).
  # This opens a public port with this port number in the same scope
  # (local or global) as the current channel unless {want_global} is true,
  # in which case the port is opened in the global scope.
  #
  # If {portnum} is nil or 0, then this creates a new region using the next
  # available port number.
  def open_port(portnum=nil, want_global=nil)
    raise PrivilegeError unless can_open_any_port?
    portnum ||= 0
    want_global = ((want_global || is_global?) ? 1 : 0)
    send "CwC", OPEN_PORT_CMD, portnum, want_global
    channel = receive_channel
    channel.hello
    channel
  end


end

end  # module Marinda
