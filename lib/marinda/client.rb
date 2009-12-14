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

# A Marinda::Client object was improperly used by a client.  For example,
# a client can't invoke another command (e.g., write) while an asynchronous
# operation (e.g., take_async) is in progress.  The only exception is a
# cancel command.  Similarly, a client can't invoke another command while
# a (synchronous) iteration/consume opeation is in progress.
class ClientError < MarindaError; end

class ConnectionBroken < IOError; end

class Client

  @@debug = false

  def self.debug=(v)
    @@debug = v
  end

  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  include ChannelFlagReaders
  include ChannelMessageCodes

  # An arbitrarily chosen value representing the largest legal reqnum value
  # before wrapping.  This must be storable with Array#pack("C").
  REQNUM_MAX = 2**8 - 1

  MESSAGE_LENGTH_SIZE = 2  # num bytes in length field of client msg
  READ_SIZE = 8192         # num bytes to read at once with sysread

  ReadBuffer = Struct.new :length, :payload

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

    # This is
    #
    #  * nil for singleton operations (e.g., write, read, take),
    #  * the command code for synchronous iteration and consume operations, 
    #    (e.g., READ_ALL_CMD, MONITOR_STREAM_CMD), and
    #  * the corresponding Method object from @dispatch for asynchronous
    #    iteration and consume operations.
    @active_command = nil
    @async_callback = nil  # the block passed to an async command

    @read_buffer = ReadBuffer.new
    @messages = []

    # Dispatch table for handling async responses.
    @dispatch = []
    @dispatch[READ_CMD] = method :async_receive_tuple
    @dispatch[READP_CMD] = method :async_receive_tuple
    @dispatch[TAKE_CMD] = method :async_receive_tuple
    @dispatch[TAKEP_CMD] = method :async_receive_tuple
    @dispatch[TAKE_PRIV_CMD] = method :async_receive_tuple
    @dispatch[TAKEP_PRIV_CMD] = method :async_receive_tuple
    @dispatch[READ_ALL_CMD] = method :async_execute_iteration
    @dispatch[TAKE_ALL_CMD] = method :async_execute_iteration
    @dispatch[MONITOR_CMD] = method :async_execute_iteration
    @dispatch[CONSUME_CMD] = method :async_execute_iteration
    @dispatch[MONITOR_STREAM_CMD] = method :async_execute_stream_iteration
    @dispatch[CONSUME_STREAM_CMD] = method :async_execute_stream_iteration

    @global_commons_channel = nil  # cached
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
	$stdout.printf "sock_recv_io: io=%p\n", io if @@debug
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
    $stdout.puts "send: reqnum=" + @reqnum.to_s if @@debug
    send_with_reqnum @reqnum, format, values
  end


  def send_cont(format, *values)
    $stdout.puts "send_cont: reqnum=" + @reqnum.to_s if @@debug
    send_with_reqnum @reqnum, format, values
  end


  def send_with_reqnum(reqnum, format, values)
    payload = [ reqnum ].pack("C") + values.pack(format)
    length = [ payload.length ].pack "n"
    message = length + payload
    sock_send message
  end


  def receive(*expected)
    loop do
      length_n = sock_recv 2
      length = length_n.unpack("n").first
      payload = sock_recv length

      recv_reqnum, code = payload.unpack("CC")
      $stdout.puts "received reqnum=" + recv_reqnum.to_s if @@debug

      if code == TUPLE_WITH_RIGHTS_RESP || code == ACCESS_RIGHT_RESP
	file = sock_recv_io
      else
	file = nil
      end

      if recv_reqnum == @reqnum
	return decode_response payload[1..-1], file, expected
      else
	file.close rescue nil if file
      end
    end
  end


  def decode_response(payload, file, expected)
    $stdout.printf "decode_response(payload=%p, file=%p, expected=%p)\n",
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

        # Note: Guard against block.call having called Client#cancel, which
        #       will cause @active_command to be nil.
	send_cont "C", NEXT_CMD if needs_next && @active_command
      end
    ensure
      # Note: Guard against block.call having called Client#cancel.
      send_cont "C", CANCEL_CMD if needs_cancel && @active_command
      @active_command = nil
    end
  end


  def async_receive_tuple(payload, file)
    code, tuple = decode_response payload, file,
      [TUPLE_RESP, TUPLE_NIL_RESP, TUPLE_WITH_RIGHTS_RESP]
    begin
      @async_callback.call tuple
    ensure
      @active_command = nil
      @async_callback = nil
    end
  end


  def async_execute_iteration(payload, file)
    async_execute_general_iteration payload, file, true
  end


  def async_execute_stream_iteration(payload, file)
    async_execute_general_iteration payload, file, false
  end


  # Execute the iteration of read_all, take_all, monitor, consume,
  # monitor_stream, and consume_stream.
  def async_execute_general_iteration(payload, file, needs_next)
    code, tuple = decode_response payload, file,
      [TUPLE_RESP, TUPLE_NIL_RESP, TUPLE_WITH_RIGHTS_RESP]
    if tuple
      needs_cancel = true
      begin
        @async_callback.call tuple
        needs_cancel = false

        # Note: Guard against @async_callback having called Client#cancel,
        #       which will cause @active_command to be nil.
        send_cont "C", NEXT_CMD if needs_next && @active_command

      ensure
        # Note: Guard against @async_callback having called Client#cancel.
        if needs_cancel && @active_command
          send_cont "C", CANCEL_CMD
          @active_command = nil
          @async_callback = nil
        end
      end
    else
      @active_command = nil
      @async_callback = nil
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
    raise ClientError if @active_command
    raise PrivilegeError unless can_write?
    send "Ca*", WRITE_CMD, YAML.dump(tuple)
    receive_ack
    true
  end


  def reply(tuple)
    raise ClientError if @active_command
    raise PrivilegeError unless can_write?
    send "Ca*", REPLY_CMD, YAML.dump(tuple)
    receive_ack
    true
  end


  def remember_peer
    raise ClientError if @active_command
    send "C", REMEMBER_CMD
    receive_handle
  end


  def forget_peer(peer)
    raise ClientError if @active_command
    send "CN", FORGET_CMD, peer
    receive_ack
    true
  end


  def write_to(peer, tuple)
    raise ClientError if @active_command
    raise PrivilegeError unless can_write?
    send "CNa*", WRITE_TO_CMD, peer, YAML.dump(tuple)
    receive_ack
    true
  end


  def forward_to(peer, tuple)
    raise ClientError if @active_command
    raise PrivilegeError unless can_write? and can_forward?
    send "CNa*", FORWARD_TO_CMD, peer, YAML.dump(tuple)
    receive_ack
    true
  end


  def pass_access_to(peer, file, tuple)
    raise ClientError if @active_command
    fail "attempt to pass access rights in global tuple space" if is_global?
    raise PrivilegeError unless can_write? and can_pass_access?
    send "CNa*", PASS_ACCESS_TO_CMD, peer, YAML.dump(tuple)
    file = file.fileno if file.kind_of? IO
    sock_send_io file
    receive_ack
    true
  end


  def read(template)
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    send "Ca*", READ_CMD, YAML.dump(template)
    receive_tuple
  end


  def readp(template)
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    send "Ca*", READP_CMD, YAML.dump(template)
    receive_tuple
  end


  def take(template)
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    send "Ca*", TAKE_CMD, YAML.dump(template)
    receive_tuple
  end


  def takep(template)
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    send "Ca*", TAKEP_CMD, YAML.dump(template)
    receive_tuple
  end


  def take_priv(template)
    raise ClientError if @active_command
    send "Ca*", TAKE_PRIV_CMD, YAML.dump(template)
    receive_tuple
  end


  def takep_priv(template)
    raise ClientError if @active_command
    send "Ca*", TAKEP_PRIV_CMD, YAML.dump(template)
    receive_tuple
  end


  def read_all(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    @active_command = READ_ALL_CMD
    send "Ca*", READ_ALL_CMD, YAML.dump(template)
    execute_iteration true, block
  end


  def take_all(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    @active_command = TAKE_ALL_CMD
    send "Ca*", TAKE_ALL_CMD, YAML.dump(template)
    execute_iteration true, block
  end


  def monitor(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    @active_command = MONITOR_CMD
    send "Ca*", MONITOR_CMD, YAML.dump(template)
    execute_iteration true, block
  end


  def consume(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    @active_command = CONSUME_CMD
    send "Ca*", CONSUME_CMD, YAML.dump(template)
    execute_iteration true, block
  end


  def monitor_stream(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    @active_command = MONITOR_STREAM_CMD
    send "Ca*", MONITOR_STREAM_CMD, YAML.dump(template)
    execute_iteration false, block
  end


  def consume_stream(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    @active_command = CONSUME_STREAM_CMD
    send "Ca*", CONSUME_STREAM_CMD, YAML.dump(template)
    execute_iteration false, block
  end


  def cancel
    if @active_command
      send_cont "C", CANCEL_CMD
      @active_command = nil
      @async_callback = nil
    end
  end


  def new_binding
    raise ClientError if @active_command
    send "C", CREATE_NEW_BINDING_CMD
    channel = receive_channel
    channel.hello
    channel
  end


  def duplicate
    raise ClientError if @active_command
    send "C", DUPLICATE_CHANNEL_CMD
    channel = receive_channel
    channel.hello
    channel
  end


  def global_commons
    raise ClientError if @active_command
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
    raise ClientError if @active_command
    raise PrivilegeError unless can_open_any_port?
    portnum ||= 0
    want_global = ((want_global || is_global?) ? 1 : 0)
    send "CwC", OPEN_PORT_CMD, portnum, want_global
    channel = receive_channel
    channel.hello
    channel
  end


  #=========================================================================
  # Asynchronous operations
  #-------------------------------------------------------------------------

  def read_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    send "Ca*", READ_CMD, YAML.dump(template)
    @active_command = @dispatch[READ_CMD]
    @async_callback = block
  end


  def readp_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    send "Ca*", READP_CMD, YAML.dump(template)
    @active_command = @dispatch[READP_CMD]
    @async_callback = block
  end


  def take_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    send "Ca*", TAKE_CMD, YAML.dump(template)
    @active_command = @dispatch[TAKE_CMD]
    @async_callback = block
  end


  def takep_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    send "Ca*", TAKEP_CMD, YAML.dump(template)
    @active_command = @dispatch[TAKEP_CMD]
    @async_callback = block
  end


  def take_priv_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    send "Ca*", TAKE_PRIV_CMD, YAML.dump(template)
    @active_command = @dispatch[TAKE_PRIV_CMD]
    @async_callback = block
  end


  def takep_priv_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    send "Ca*", TAKEP_PRIV_CMD, YAML.dump(template)
    @active_command = @dispatch[TAKEP_PRIV_CMD]
    @async_callback = block
  end


  def read_all_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    send "Ca*", READ_ALL_CMD, YAML.dump(template)
    @active_command = @dispatch[READ_ALL_CMD]
    @async_callback = block
  end


  def take_all_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    send "Ca*", TAKE_ALL_CMD, YAML.dump(template)
    @active_command = @dispatch[TAKE_ALL_CMD]
    @async_callback = block
  end


  def monitor_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    send "Ca*", MONITOR_CMD, YAML.dump(template)
    @active_command = @dispatch[MONITOR_CMD]
    @async_callback = block
  end


  def consume_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    send "Ca*", CONSUME_CMD, YAML.dump(template)
    @active_command = @dispatch[CONSUME_CMD]
    @async_callback = block
  end


  def monitor_stream_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_read?
    send "Ca*", MONITOR_STREAM_CMD, YAML.dump(template)
    @active_command = @dispatch[MONITOR_STREAM_CMD]
    @async_callback = block
  end


  def consume_stream_async(template, &block)
    raise ArgumentError, "missing block" unless block
    raise ClientError if @active_command
    raise PrivilegeError unless can_take?
    send "Ca*", CONSUME_STREAM_CMD, YAML.dump(template)
    @active_command = @dispatch[CONSUME_STREAM_CMD]
    @async_callback = block
  end


  #-------------------------------------------------------------------------
  # Management of asynchronous operations by event loop
  #-------------------------------------------------------------------------

  def io
    @sock
  end


  def want_read
    # XXX may need to be careful; only want read for asynchronous commands
    @active_command != nil
  end


  def want_write
    false
  end


  def read_data
    return unless @sock  # still connected

    @messages.clear
    @read_buffer.payload ||= ""

    begin
      data = @sock.read_nonblock READ_SIZE
      if @@debug
        $stdout.printf "Client#read_data from %p: %p\n", @sock, data
      end

      start = 0
      while start < data.length
	data_left = data.length - start

	desired_length = @read_buffer.length || MESSAGE_LENGTH_SIZE
	fill_amount = desired_length - @read_buffer.payload.length
	usable_amount = [fill_amount, data_left].min

	@read_buffer.payload << data[start, usable_amount]
	start += usable_amount

	if usable_amount == fill_amount
	  if @read_buffer.length
            if @@debug
              $stdout.printf "Client#read_data: @read_buffer.length = %d\n",
                @read_buffer.length
              $stdout.printf "Client#read_data: @read_buffer.payload = %p\n",
                @read_buffer.payload
            end
	    @messages << @read_buffer.payload
	    @read_buffer.length = nil
	    @read_buffer.payload = ""
	  else
	    @read_buffer.length = @read_buffer.payload.unpack("n").first
	    @read_buffer.payload = ""
            if @@debug
              $stdout.printf "Client#read_data: message length = %d\n",
                @read_buffer.length
            end
	    if @read_buffer.length == 0
	      raise EOFError, "client protocol error: message length == 0"
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
      $stdout.printf "Client#read_data from %p: IO::WaitReadable", @sock
      # do nothing, since we'll automatically retry in the next select() round

    # can get Errno::ECONNRESET
    rescue SocketError, IOError, EOFError, SystemCallError
      if @@debug
        $stdout.printf "Client#read_data from %p: %p", @sock, $!
      end
      msg = "lost connection: #{$!.class.name}: #{$!}"
      shutdown()
      raise ConnectionBroken, msg
    end

    @messages.each do |payload|
      recv_reqnum, code = payload.unpack("CC")
      $stdout.puts "received reqnum=" + recv_reqnum.to_s if @@debug

      if code == TUPLE_WITH_RIGHTS_RESP || code == ACCESS_RIGHT_RESP
        fail "UNIMPLEMENTED: receiving fd asynchronously"
	file = sock_recv_io()
      else
	file = nil
      end

      if recv_reqnum == @reqnum
        @active_command.call payload[1..-1], file
      else
	file.close rescue nil if file
      end
    end
  end


  def shutdown
    return unless @sock
    @sock.close rescue nil
    @sock = nil
  end


  def write_data
    fail "INTERNAL ERROR: Marinda::Client#write_data called"
  end

end


#############################################################################

class ClientEventLoop

  @@debug = false

  def self.debug=(v)
    @@debug = v
  end

  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  def initialize
    @running = false
    @sources = {}  # source => true
    @active_sources = {}  # fd => source

    # IO sets to pass to Kernel::select, and IO sets returned by
    # select.  We use instance variables so that it is easier to inspect
    # a core dump with gdb.
    @read_set = []
    @write_set = []
    @readable = nil
    @writable = nil
  end


  # A source should implement the following methods:
  #
  #   * io() => return IO object or nil if source has failed/finished,
  #   * want_read() => true if the source wants to perform a read,
  #   * want_write() => true if the source wants to perform a write,
  #   * read_data() => perform nonblocking read, and
  #   * write_data() => perform nonblocking write.
  #
  def add_source(source)
    @sources[source] = true
  end


  def remove_source(source)
    @sources.delete source
  end


  def start
    @running = true
    while @running
      @read_set.clear
      @write_set.clear

      defunct_sources = []
      @active_sources.clear

      @sources.each_key do |source|
        if source.io
          if source.want_read || source.want_write
            @read_set << source.io if source.want_read
            @write_set << source.io if source.want_write
            @active_sources[source.io] = source
          end
        else
          defunct_sources << source
        end
      end

      defunct_sources.each do |source|
        @sources.delete source
      end

      if @@debug
        $stdout.printf "Client: waiting for I/O ...\n"
        $stdout.printf "select read_set (%d fds): %p\n",
          @read_set.length, @read_set
        $stdout.printf "select write_set (%d fds): %p\n",
          @write_set.length,@write_set
      end

      if @read_set.empty? && @write_set.empty?
        @running = false
        break 
      end

      # XXX select can throw "closed stream (IOError)" if a write file
      #     descriptor has been closed previously; we may want to catch
      #     this exception and remove the closed descriptor.  Strictly
      #     speaking, users shouldn't be passing in closed file descriptors.
      @readable, @writable = select @read_set, @write_set
      if @@debug
        $stdout.printf "select returned %d readable, %d writable\n",
          (@readable ? @readable.length : 0), (@writable ? @writable.length : 0)
      end

      if @readable
        @readable.each do |fd|
          $stdout.printf "readable %p\n", fd if @@debug
          @active_sources[fd].read_data()
        end
      end

      if @writable
        @writable.each do |fd|
          $stdout.printf "writable %p\n", fd if @@debug
          @active_sources[fd].write_data()
        end
      end
    end
  end


  def suspend
    @running = false
  end

end

end  # module Marinda
