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
#############################################################################

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
    if $use_judy
      require 'marinda/judytuplebag'
    else
      require 'marinda/tuplebag'
    end

    @worker = worker
    @port = port
    @tuples = ($use_judy ? JudyTupleBag.new(@worker, @port) :
               TupleBag.new(@worker, @port))
    @templates = TemplateBag.new @worker, @port
  end


  def checkpoint_state(txn, checkpoint_id)
    txn.execute("INSERT INTO Regions VALUES(?, ?, ?, ?, ?)",
                checkpoint_id, @port, @tuples.seqnum, @node_id, @session_id)

    @tuples.checkpoint_state txn, checkpoint_id, @port
    @templates.checkpoint_state txn, checkpoint_id, @port
  end


  # The required block should take |session_id, request| and store the request
  # into GlobalSpace.@ongoing_requests.  This ties together the
  # RegionRequest objects restored in TemplateBag to the GlobalSpace.
  #
  # The {sessions} argument should map session_id to a Context object restored
  # in GlobalSpace.
  def restore_state(state, checkpoint_id, tuple_seqnum, sessions, &block)
    @tuples.restore_state state, checkpoint_id, tuple_seqnum, @port
    @templates.restore_state state, checkpoint_id, @port, sessions,
      @worker, &block
  end


  # The following region methods (e.g., write, read) return
  #
  #  * nil, tuple: for a completed singleton operation, and
  #  * request, nil: for a blocked singleton, iteration, or stream operation.
  #
  # The returned RegionRequest object is only intended to be used for
  # cancelling a request; no other use of the object (or inspection of its
  # contents) is allowed.
  #
  # NOTE: We use 'channel' in the argument list of some methods, but the
  #       actual type depends on whether a Region exists in a local or
  #       global tuple space.  In a local tuple space, the 'channel'
  #       argument is actually a Channel.  In a global tuple space, the
  #       'channel' argument is a GlobalSpace::Context.
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


  def read_all(template, channel, cursor=0)
    tuple = @tuples.readp_next template, cursor
    return nil, tuple
  end


  def take_all(template, channel, cursor=0)
    tuple = @tuples.takep_next template, cursor
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


  def consume(template, channel, cursor=0)
    tuple = @tuples.takep_next template, cursor
    if tuple
      return nil, tuple
    else
      request = RegionRequest.new @worker, @port, :consume, template, channel
      @templates.add_consuming request
      return request, nil
    end
  end


  def monitor_stream_setup(template, channel)
    request = RegionRequest.new @worker, @port, :monitor_stream, template,
      channel
    @templates.add_reading request
    return request, nil
  end


  def consume_stream_setup(template, channel)
    request = RegionRequest.new @worker, @port, :consume_stream, template,
      channel
    @templates.add_consuming request
    return request, nil
  end


  def monitor_stream_start(template, channel)
    @tuples.monitor_stream_existing template, channel
  end


  def consume_stream_start(template, channel)
    @tuples.consume_stream_existing template, channel
  end


  def cancel(request)
    case request.operation
    when :take, :take_all, :consume, :consume_stream
      @templates.remove_consuming request
    when :read, :read_all, :monitor, :monitor_stream
      @templates.remove_reading request
    else
      fail "INTERNAL ERROR: unhandled request operation %p", request.operation
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
