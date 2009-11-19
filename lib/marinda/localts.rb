#############################################################################
## The local tuple space, which implements the heart of the local server.
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
## $Id: localts.rb,v 1.26 2009/03/17 01:01:40 youngh Exp $
#############################################################################

require 'thread'

require 'marinda/port'
require 'marinda/region'
require 'marinda/channel'
require 'marinda/clientio'

module Marinda

class LocalSpace

  private #===============================================================

  include Socket::Constants

  def initialize(node_id, mux)
    @node_id = node_id
    @mux = mux
    @clientio = ClientIO.new self

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

    @inbox = Queue.new

    @dispatch = {}

    # Events from LocalServer .............................................
    @dispatch[:add_client] = method :handle_add_client
    @dispatch[:shutdown] = method :handle_shutdown

    # Events from ClientIO ................................................
    @dispatch[:client_removed] = method :handle_client_removed
    @dispatch[:client_message] = method :forward_message
    @dispatch[:message_written] = method :forward_message
    @dispatch[:message_write_failed] = method :forward_message

    # Events from GlobalSpaceMux ..........................................
    @dispatch[:global_private_region_created] =
      method :handle_global_private_region_created
    @dispatch[:global_region_pair_created] =
      method :handle_global_region_pair_created
    @dispatch[:mux_result] = method :handle_mux_result

    # Events from Channel .................................................
    @dispatch[:create_new_binding] = method :handle_create_new_binding
    @dispatch[:duplicate_channel] = method :handle_duplicate_channel
    @dispatch[:open_port] = method :handle_open_port

    # Events from self .................................................
    @dispatch[:binding_created] = method :forward_message
    @dispatch[:channel_duplicated] = method :forward_message
    @dispatch[:port_opened] = method :forward_message

    # Events from Region ..................................................
    @dispatch[:region_result] = method :forward_message

    @thread = Thread.new(&method(:execute_toplevel))
  end


  def execute_toplevel
    begin
      execute()
    rescue
      msg = sprintf "LocalSpace: thread exiting on uncaught exception at " +
        "top-level: %p; backtrace: %s", $!, $!.backtrace.join(" <= ")
      $log.err "%s", msg
      log_failure msg rescue nil
      exit 1
    end
  end


  def log_failure(msg)
    path = (ENV['HOME'] || "/tmp") +
      "/marinda-localts-failed.#{Time.now.to_i}.#{$$}"
    File.open(path, "w") do |file|
      file.puts msg
    end
  end


  def execute
    loop do
      message = @inbox.deq
      handler = @dispatch[message[0]]
      if handler
	handler.call(*message)
      else
	$log.err "LocalSpace#execute: ignoring unknown message: %p", message
      end
    end
  end


  def handle_add_client(command, sock)
    flags = ChannelFlags.new
    flags.allow_commons_privileges!
    private_port = allocate_private_port
    @regions[private_port] = Region.new self, private_port

    channel = Channel.new flags, self, @commons_port, private_port,
      sock, @clientio
    @channels[private_port] << channel
    @clientio.add_client self, channel, sock
  end


  def handle_shutdown(command)
    fail "UNIMPLEMENTED"
  end


  def handle_client_removed(command, sender, channel)
    # nothing to do
  end


  def forward_message(command, sender, channel, *args)
    if channel.alive
      channel.__send__ command, sender, *args
    else
      $log.info "LocalSpace#forward_message: discarding message %p to " +
        "dead channel %#x\n", command, channel.object_id

      case command
      when :binding_created, :channel_duplicated, :port_opened
	new_channel, client_sock = args
	if new_channel
	  new_channel.shutdown
	  client_sock.close rescue nil
	end

      when :region_result
	operation, template, tuple = args
        case operation
        when :take, :takep, :take_all, :consume, :consume_stream
	  if tuple.access_fd
	    tuple.access_fd.close rescue nil
	    tuple.access_fd = nil
	  end
	  # XXX possibly return tuple into region
	end
      end
    end
  end


  # An event generated by GlobalSpaceMux#create_private_region.
  def handle_global_private_region_created(command, sender, request_data,
                                           private_port)
    request_data.call private_port
  end

  # An event generated by GlobalSpaceMux#create_region_pair.
  def handle_global_region_pair_created(command, sender, request_data,
                                        private_port)
    request_data.call private_port
  end


  def handle_mux_result(command, port, request, result)
    if request.channel.alive
      request.channel.region_result port, request.operation,
        request.template, result
    # else XXX possibly return taken tuple into region
    end
  end


  def handle_create_new_binding(command, channel)
    flags = channel.flags.privileges
    public_port = channel.public_port
    if @mux && Port.global?(public_port)
      @mux.create_private_region self, lambda { |private_port|
	result = (private_port ?
		  create_channel(flags, public_port, private_port) : nil)
	result ||= [nil, nil]
	@inbox.enq [:binding_created, self, channel, *result]
      }
    else
      private_port = allocate_private_port
      result = create_channel flags, public_port, private_port
      if result
	@regions[private_port] = Region.new self, private_port
      else
	result = [nil, nil]
      end
      @inbox.enq [:binding_created, self, channel, *result]
    end
  end


  def handle_duplicate_channel(command, channel)
    flags = channel.flags.privileges
    result = create_channel flags, channel.public_port, channel.private_port
    result ||= [nil, nil]
    @inbox.enq [:channel_duplicated, self, channel, *result]
  end


  # {portnum} should be a port number (not the full port value).  A zero value
  # causes the next available port number to be used.
  #
  # This opens a public port with this port number in the scope implied by
  # {want_global}.  The corresponding private port will be created at the
  # same time.
  def handle_open_port(command, channel, flags, portnum, want_global)
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
	@inbox.enq [:port_opened, self, channel, *result]
      }
    else
      private_port = allocate_private_port
      result = create_channel flags, public_port, private_port
      if result
	@regions[public_port] ||= Region.new self, public_port
	@regions[private_port] = Region.new self, private_port
      else
	result = [nil, nil]
      end
      @inbox.enq [:port_opened, self, channel, *result]
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

    channel = Channel.new flags, self, public_port, private_port,
      sockets[0], @clientio
    @channels[private_port] << channel
    @clientio.add_client self, channel, sockets[0]
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


  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

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


  public #=================================================================

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


  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

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


  # Events from LocalServer -----------------------------------------------

  # no response
  def add_client(sock)
    @inbox.enq [:add_client, sock]
  end

  def shutdown
    @inbox.enq [:shutdown]
  end


  # Events from ClientIO --------------------------------------------------

  def client_removed(sender, channel)
    @inbox.enq [:client_removed, sender, channel]
  end

  def client_message(sender, channel, payload, fd)
    @inbox.enq [:client_message, sender, channel, payload, fd]
  end

  def message_written(sender, channel, request)
    @inbox.enq [:message_written, sender, channel, request]
  end

  def message_write_failed(sender, channel, request)
    @inbox.enq [:message_write_failed, sender, channel, request]
  end


  # Events from GlobalSpaceMux --------------------------------------------

  def global_private_region_created(sender, request_data, private_port)
    @inbox.enq [:global_private_region_created, sender,
                request_data, private_port]
  end

  def global_region_pair_created(sender, request_data, private_port)
    @inbox.enq [:global_region_pair_created, sender, request_data, private_port]
  end

  def mux_result(port, request, result)
    @inbox.enq [:mux_result, port, request, result]
  end

  # Events from Channel ---------------------------------------------------

  # response: [:binding_created, LocalSpace#self, new_channel, client_sock]
  #           where new_channel and client_sock will be nil on error
  def create_new_binding(channel)
    @inbox.enq [:create_new_binding, channel]
  end

  # response: [:channel_duplicated, LocalSpace#self, new_channel, client_sock]
  #           where new_channel and client_sock will be nil on error
  def duplicate_channel(channel)
    @inbox.enq [:duplicate_channel, channel]
  end

  # response: [:port_opened, LocalSpace#self, new_channel, client_sock]
  #           where new_channel and client_sock will be nil on error
  def open_port(channel, flags, portnum, want_global)
    @inbox.enq [:open_port, channel, flags, portnum, want_global]
  end


  # Events from Region ---------------------------------------------------

  def region_result(port, channel, operation, template, tuple)
    @inbox.enq [:region_result, port, channel, operation, template, tuple]
  end

end

end  # module Marinda
