#############################################################################
## The global tuple space, which implements the heart of the global server.
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
## $Id: globalts.rb,v 1.15 2009/03/17 01:01:08 youngh Exp $
#############################################################################

require 'thread'

require 'marinda/port'
require 'marinda/region'

module Marinda

class GlobalSpace

  private #===============================================================

  def initialize(demux, state)
    @demux = demux
    @global_state = state
    @services = {}
    @last_public_portnum = @global_state.get_last_public_portnum()
    @last_private_portnum = @global_state.get_last_private_portnum()
    @commons_port = Port.make_public_global
    @commons_region = Region.new @demux, @commons_port
    @regions = {   # port => global public & private regions
      @commons_port => @commons_region
    }
  end


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


  def find_region(port)
    @regions[port]
  end


  public #================================================================

  def checkpoint_state(txn, checkpoint_id)
    # There's no need to save the values of @last_public_portnum or
    # @last_private_portnum because these values are already persisted
    # whenever they change.

    @regions.each_value do |region|
      region.checkpoint_state txn, checkpoint_id
    end
  end


  # The required block should take |session_id, request| and store the request
  # into GlobalSpaceDemux.@ongoing_requests.  This ties together the
  # RegionRequest objects restored in TemplateBag to the GlobalSpaceDemux.
  #
  # The {sessions} argument should map session_id to a Context object restored
  # in GlobalSpaceDemux.
  def restore_state(checkpoint_id, sessions, &block)
    # Don't do any essential initializations of this class within this method
    # because this method won't be called if there isn't a checkpoint to
    # restore.  For example, don't restore @last_public_portnum or
    # @last_private_portnum, or create the commons region here.

    @global_state.db.execute("SELECT * FROM Regions WHERE checkpoint_id=? ",
                              checkpoint_id) do |row|
      port, tuple_seqnum, node_id, session_id = row[1..-1]

      # The commons region already exists.  All others must be created here.
      region = (@regions[port] ||= Region.new(@demux, port))
      if Port.private?(port)
        region.node_id = node_id
        region.session_id = session_id
      end

      region.restore_state @global_state, checkpoint_id,
        tuple_seqnum, sessions, &block
    end
  end


  # Returns the newly allocated region.
  def create_public_region(port=nil)
    port ||= allocate_public_port
    @regions[port] ||= Region.new @demux, port
  end


  # Returns the newly allocated region.
  def create_private_region(node_id, session_id)
    port = allocate_private_port
    region = Region.new @demux, port
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


  # Region proxy methods --------------------------------------------------

  def region_write(port, tuple, context)
    region = find_region port
    # note: a private peer region may no longer exist--tolerate this
    region.write tuple, context if region
  end

  # operation == :read, :readp, :take, :takep
  def region_singleton_operation(operation, port, template, context)
    find_region(port).__send__ operation, template, context
  end

  # operation == :monitor, :read_all
  def region_iteration_operation(operation, port, template, context, cursor=0)
    find_region(port).__send__ operation, template, context, cursor
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

end

end  # module Marinda
