#############################################################################
## A class for multiplexing I/O to clients and for providing an event-based
## interface to I/O.
##
## ClientIO has some knowledge of the client communication protocol, so a
## change in the protocol may affect the implementation of this class.
## Normally, all that ClientIO would need to know about the protocol is
## the structure at the lowest level, namely that messages are sent in
## length-prefixed records, as follows:
##
##     [ length | payload ]
##
## However, in order to support passing of file descriptors, ClientIO has to
## know enough about the payload structure to be able to extract out the
## client-issued command code.  So the level of detail that ClientIO needs
## to know presently is as follows:
##
##     [ length | reqnum | command-code | remaining-payload ]
##
## If the structure of the payload changes in such a way that the command
## code no longer resides at the same offset, then ClientIO must be updated
## accordingly.
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
## $Id: clientio.rb,v 1.6 2009/04/02 23:27:05 youngh Exp $
#############################################################################

require 'ostruct'
require 'thread'
require 'monitor'
require 'socket'

require 'marinda/list'
require 'marinda/msgcodes'

module Marinda

class ClientIO

  private

  CLIENT_MESSAGE_LENGTH_SIZE = 2  # num bytes in length field of client msg
  READ_SIZE = 4096   # num bytes to read at once with sysread

  ReadBuffer = Struct.new :length, :payload, :want_io
  WriteBuffer = Struct.new :payload, :file, :request

  # SockState contains state for both read and write directions of sockets.
  # The attribute :read_buffer only applies to reading, and the attribute
  # :write_queue only applies to writing.
  class SockState
    attr_accessor :channel, :read_buffer, :write_queue

    def initialize(channel)
      @channel = channel
      @read_buffer = ReadBuffer.new
      @write_queue = List.new  # [WriteBuffer]
    end
  end


  # {space} should be the main event-dispatching thread--namely, LocalSpace.
  def initialize(space)
    @space = space

    @inbox = List.new
    @inbox.extend MonitorMixin

    @sock_state = {}  # sock => SockState

    sock_pair = UNIXSocket.pair Socket::SOCK_STREAM, 0
    @control_read = sock_pair[0]
    @control_write = sock_pair[1]
    @control_write.extend MonitorMixin

    @read_set = [ @control_read ]
    @write_set = []

    @dispatch = {}
    @dispatch[:add_client] = method :handle_add_client
    @dispatch[:remove_client] = method :handle_remove_client
    @dispatch[:write_message] = method :handle_write_message

    @thread = Thread.new(&method(:execute_toplevel))
  end


  def execute_toplevel
    begin
      execute()
    rescue
      msg = sprintf "ClientIO: thread exiting on uncaught exception at " +
        "top-level: %p; backtrace: %s", $!, $!.backtrace.join(" <= ")
      $log.err "%s", msg
      log_failure msg rescue nil
      exit 1
    end
  end


  def log_failure(msg)
    path = (ENV['HOME'] || "/tmp") +
      "/marinda-clientio-failed.#{Time.now.to_i}.#{$$}"
    File.open(path, "w") do |file|
      file.puts msg
    end
  end


  def execute
    loop do
      if $debug_client_io_select
        $log.debug "ClientIO waiting for I/O ..."
        $log.debug "client select read_set (%d fds): %p",
          @read_set.length, @read_set
        $log.debug "client select write_set (%d fds): %p",
          @write_set.length,@write_set
      end

      # XXX select can throw "closed stream (IOError)" if a write file
      #     descriptor has been closed previously; we may want to catch
      #     this exception and remove the closed descriptor.  Strictly
      #     speaking, users shouldn't be passing in closed file descriptors.
      readable, writable = select @read_set, @write_set
      if $debug_client_io_select
        $log.debug "client select returned %d readable, %d writable",
          (readable ? readable.length : 0), (writable ? writable.length : 0)
      end

      if readable
        readable.each do |sock|
          $log.debug "client readable %p", sock if $debug_client_io_select
          if sock.equal? @control_read
            handle_messages
          else
            read_data sock
          end
        end
      end

      if writable
        writable.each do |sock|
          $log.debug "client writable %p", sock if $debug_client_io_select
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
      $log.debug "client handle message %s",
        inspect_message(message) if $debug_client_commands
      handler = @dispatch[message[0]]
      if handler
	handler.call(*message)
      else
	$log.err "ClientIO#handle_messages: ignoring unknown message type: %p",
          message
      end
    end
  end


  def handle_add_client(command, channel, sock)
    if @sock_state.has_key?(sock)
      $log.err "ClientIO#handle_add_client: attempt to add an " +
        "already-registered socket %p", sock
    else
      @sock_state[sock] = SockState.new channel
      @read_set << sock
    end
  end


  def handle_remove_client(command, channel, sock)
    # in order to be idempotent, don't check that sock is registered

    @sock_state.delete sock
    @read_set.delete sock
    @write_set.delete sock

    @space.client_removed self, channel
  end


  def handle_write_message(command, channel, sock, payload, file, request)
    state = @sock_state[sock]
    unless state
      $log.err "ClientIO#handle_write_message: attempt to write to an " +
        "unregistered/closed sock %p, payload=%p, file=%p", sock, payload, file
      # XXX maybe close any file/channel in {file}
      @space.message_write_failed self, channel, request
      return
    end

    payload ||= ""
    if payload.length > 0 || file
      @write_set << sock if state.write_queue.empty?
      state.write_queue << WriteBuffer[payload, file, request]
    else
      @space.message_written self, channel, request
    end
  end


  #--------------------------------------------------------------------------

  def read_data(sock)
    state = @sock_state[sock]
    return unless state    # connection was lost

    messages = []  # list of [payload, file]

    buffer = state.read_buffer
    buffer.payload ||= ""

    begin
      # NOTE: To prevent blocking, only do 1 recv_io/sysread call per
      #       read_data() call.
      if buffer.want_io
	fd = sock.recv_io
	raise EOFError, "client protocol error: no file descriptor" unless fd
	messages << [buffer.payload, fd]
	buffer.length = nil
	buffer.payload = ""
	buffer.want_io = nil
      else
	data = sock.sysread READ_SIZE
        $log.debug "client read_data from %p: %p",
          sock, data if $debug_client_io_bytes

	start = 0
	while start < data.length
	  data_left = data.length - start

	  desired_length = buffer.length || CLIENT_MESSAGE_LENGTH_SIZE
	  fill_amount = desired_length - buffer.payload.length
	  usable_amount = [fill_amount, data_left].min

	  buffer.payload << data[start, usable_amount]
	  start += usable_amount

	  if usable_amount == fill_amount
	    if buffer.length
              if $debug_client_io_bytes
                $log.debug "client read_data: buffer.length = %d", buffer.length
                $log.debug "client read_data: buffer.payload = %p",
                  buffer.payload
              end

              code = payload.unpack("C").first
	      if code == ChannelMessageCodes::PASS_ACCESS_TO_CMD
		if data.length - start > 0
		  raise EOFError, "client protocol error: no file descriptor"
		end
		buffer.want_io = true
	      else
		messages << [buffer.payload, nil]
		buffer.length = nil
		buffer.payload = ""
	      end
	    else
	      buffer.length = buffer.payload.unpack("n").first
	      buffer.payload = ""
              if $debug_client_io_bytes
                $log.debug "client read_data: message length = %d",
                  buffer.length
              end
	      if buffer.length == 0
		raise EOFError, "client protocol error: message length == 0"
	      end
	    end
	  end
	end
      end

      # in `recv_io': file descriptor was not passed 
      # (msg_controllen : 0 != 16) (SocketError)
    rescue SocketError, IOError, EOFError, SystemCallError
      $log.info "read_data from %p: %p", sock, $!
      reset_connection sock
      messages << [nil, nil]
    end

    messages.each do |payload, file|
      @space.client_message self, state.channel, payload, file
    end
  end


  # NOTE: This only works with Unix domain sockets, not general TCP sockets.
  def write_data(sock)
    state = @sock_state[sock]
    return unless state    # connection was lost

    buffer = state.write_queue.first

    begin
      # NOTE: To prevent blocking, only do 1 syswrite call per write_data().
      if buffer.payload.length > 0
	n = sock.syswrite buffer.payload
	data_written = buffer.payload.slice! 0, n
        if $debug_client_io_bytes
          $log.debug "client write_data to %p: wrote %d bytes, %d left: %p",
            sock, n, buffer.payload.length, data_written
        end
      else # buffer.file != nil
	sock.send_io buffer.file
        $log.debug "client write_data sent file %p over %p",
          buffer.file, sock if $debug_client_io_bytes

	buffer.file.close rescue nil
	buffer.file = nil
      end

      # syswrite may throw "not opened for writing (IOError)"
    rescue IOError, SystemCallError # including Errno::EPIPE
      $log.info "write_data to %p: %p", sock, $!
      reset_connection sock
      @space.message_write_failed self, state.channel, buffer.request

    else
      if buffer.payload.length == 0 && buffer.file == nil
	state.write_queue.shift
	@write_set.delete sock if state.write_queue.empty?
	@space.message_written self, state.channel, buffer.request
      end
    end
  end


  def reset_connection(sock)
    @read_set.delete sock
    @write_set.delete sock

    state = @sock_state.delete sock
    # XXX may need to close files and channels being passed to the client
    state.write_queue.clear if state

    sock.close rescue nil
  end


  #--------------------------------------------------------------------------

  def enqueue_message(*message)
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
	sprintf "#<%s:%#x>", element.class.name, element.object_id
      end
    end.join(", ")
    "[" + body + "]"
  end


  public #===================================================================

  # In the following event methods, the {request} parameter can be any
  # state that {sender} needs to associate a response from ClientIO to a
  # particular internal state.  ClientIO doesn't interpret {request} in any
  # way; it simply returns {request} in responses.
  #
  # The {sender} parameter should always equal {@space}.
  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  # no immediate response, but for every message received from client, this
  # replies with [:client_message, ClientIO#self, channel, message],
  #              with {message == nil} on EOF
  def add_client(sender, channel, sock)
    fail "ASSERTION: wrong sender " + sender.inspect unless sender == @space
    enqueue_message :add_client, channel, sock
  end

  # response: [:client_removed, ClientIO#self, channel]
  def remove_client(sender, channel, sock)
    fail "ASSERTION: wrong sender " + sender.inspect unless sender == @space
    enqueue_message :remove_client, channel, sock
  end

  # response: [:message_written, ClientIO#self, channel, request]
  #           [:message_write_failed, ClientIO#self, channel, request]
  def write_message(sender, channel, sock, payload, file, request)
    fail "ASSERTION: wrong sender " + sender.inspect unless sender == @space
    enqueue_message :write_message, channel, sock, payload, file, request
  end

end

end  # module Marinda
