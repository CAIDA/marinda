#############################################################################
## A region in either the local or global tuple space.
##
## A region actually stores tuples, whereas a tuple space is merely an
## aggregation of regions.
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
## $Id: region.rb,v 1.32 2009/03/17 01:07:20 youngh Exp $
#############################################################################

require 'thread'

require 'marinda/tuplebag'
require 'marinda/templatebag'
require 'marinda/request'

module Marinda

class Region

  attr_reader :port

  # node_id and session_id are only applicable to private regions in the
  # global tuple space.  These attributes track the creator of the region
  # for the purposes of garbage collection.
  attr_accessor :node_id, :session_id


  def initialize(worker, port)
    @worker = worker
    @port = port
    @tuples = TupleBag.new
    @templates = TemplateBag.new @worker, self
  end


  def checkpoint_state(txn, checkpoint_id)
    txn.execute("INSERT INTO Regions VALUES(?, ?, ?, ?, ?)",
                checkpoint_id, @port, @tuples.seqnum, @node_id, @session_id)

    @tuples.checkpoint_state txn, checkpoint_id, @port
    @templates.checkpoint_state txn, checkpoint_id, @port
  end


  # The required block should take |session_id, request| and store the request
  # into GlobalSpaceDemux.@ongoing_requests.  This ties together the
  # RegionRequest objects restored in TemplateBag to the GlobalSpaceDemux.
  #
  # The {sessions} argument should map session_id to a Context object restored
  # in GlobalSpaceDemux.
  def restore_state(state, checkpoint_id, tuple_seqnum, sessions, &block)
    @tuples.restore_state state, checkpoint_id, tuple_seqnum, @port
    @templates.restore_state state, checkpoint_id, @port, sessions,
      @worker, &block
  end


  # The following region methods return a RegionRequest object if the
  # operation requested blocks.  Otherwise, they return nil, and the
  # operation result is returned via a message.  The returned RegionRequest
  # object is only intended to be used by a Channel to cancel a request;
  # no other use of the object (or inspection of its contents) is allowed.
  #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


  # This operation is an exception in not producing any result.
  def write(tuple, channel)
    unless @templates.match_consume tuple, (@tuples.seqnum + 1)
      @tuples.write tuple
    end
    nil
  end


  def read(template, channel)
    tuple = @tuples.readp template
    if tuple
      return nil, tuple
    else
      request = RegionRequest.new @worker, @port, :read, template, channel
      @templates.add_reading request
      return request, nil
    end
  end


  def readp(template, channel)
    tuple = @tuples.readp template
    return nil, tuple
  end


  def take(template, channel)
    tuple = @tuples.takep template
    if tuple
      return nil, tuple
    else
      request = RegionRequest.new @worker, @port, :take, template, channel
      @templates.add_consuming request
      return request, nil
    end
  end


  def takep(template, channel)
    tuple = @tuples.takep template
    return nil, tuple
  end


  def monitor(template, channel, cursor=0)
    tuple = @tuples.readp_next template, cursor
    if tuple
      return nil, tuple
    else
      request = RegionRequest.new @worker, @port, :monitor, template, channel
      @templates.add_reading request
      return request, nil
    end
  end


  def read_all(template, channel, cursor=0)
    tuple = @tuples.readp_next template, cursor
    return nil, tuple
  end


  def cancel(request)
    if request.operation == :take
      @templates.remove_consuming request
    else # :read, :monitor, :read_all
      @templates.remove_reading request
    end
  end


  def shutdown
    @tuples.shutdown  # close any file descriptors attached to tuples
    @tuples = nil
    @templates = nil
  end


  def dump(resource="all")
    $stderr.puts "==========================================================="
    $stderr.printf "%s %s Region %#x (port=%#x):\n",
      (Port.public?(@port) ? "public" : "private"),
      (Port.global?(@port) ? "global" : "local"), object_id, @port

    @tuples.dump resource
    @templates.dump resource
  end


  def inspect
    sprintf "\#<Marinda::Region:%#x @port=%p, @tuples=%#x, @templates=%#x>",
      object_id, @port, @tuples.object_id, @templates.object_id
  end

end

end  # module Marinda
