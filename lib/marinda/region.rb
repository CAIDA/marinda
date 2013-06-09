#############################################################################
## A region in either the local or global tuple space.
##
## A region actually stores tuples, whereas a tuple space is merely an
## aggregation of regions.
##
## --------------------------------------------------------------------------
## Author: Young Hyun
## Copyright (C) 2007-2013 The Regents of the University of California.
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

require 'marinda/list'
require 'marinda/request'

module Marinda

# NOTE: We use 'channel' in the argument list of some methods, but the
#       actual type depends on whether a TemplateBag exists in a local or
#       global tuple space.  In a local tuple space, the 'channel'
#       argument is actually a Channel.  In a global tuple space, the
#       'channel' argument is a GlobalSpace::Context.

class Region

  attr_reader :port

  # node_id and session_id are only applicable to private regions in the
  # global tuple space.  These attributes track the creator of the region
  # for the purposes of garbage collection.
  attr_accessor :node_id, :session_id


  def initialize(worker, port)
    @worker = worker
    @port = port

    # @seqnum should equal the highest seqnum actually assigned to a tuple
    # and not the next available seqnum.  The value 0 is special--it should
    # never be assigned to a tuple, and hence serves to indicate a seqnum
    # that is lower than all possible seqnums.
    @seqnum = 0
    @tuples = Judy::JudyL.new

    @read_requests = List.new  # blocked read, read_all, monitor, monitor_stream
    @consume_requests = List.new  # blocked take, take_all, consume, ...
  end


  #=========================================================================
  # TupleBag
  #-------------------------------------------------------------------------
  private

  def find_match(template)
    template_value = MIO.decode template
    @tuples.each_with_index do |tuple, index|
      tuple_value = MIO.decode tuple
      return tuple, index if match template_value, tuple_value
    end
    return nil
  end


  def find_next_match(template, cursor)
    template_value = MIO.decode template
    loop do
      cursor = @tuples.next_index cursor
      return nil unless cursor

      tuple = @tuples[cursor]
      tuple_value = MIO.decode tuple
      return tuple, cursor if match template_value, tuple_value
    end
  end


  def match(template_value, tuple_value)
    return true if template_value.length == 0
    return false unless template_value.length == tuple_value.length
    
    template_value.each_with_index do |lhs, i|
      rhs = tuple_value[i]
      return false unless lhs == nil || lhs === rhs || rhs == nil
    end
    true
  end


  def checkpoint_tuples(txn, checkpoint_id)
    txn.prepare("INSERT INTO RegionTuples VALUES(?, ?, ?, ?)") do |insert_stmt|
      @tuples.each_with_index do |tuple, seqnum|
        insert_stmt.execute checkpoint_id, @port, seqnum, tuple
      end
    end
  end


  def restore_tuples(state, checkpoint_id, tuple_seqnum)
    @seqnum = tuple_seqnum
    state.db.execute("SELECT seqnum,tuple FROM RegionTuples
                       WHERE checkpoint_id=? AND port=?
                       ORDER BY seqnum",
                      checkpoint_id, @port) do |row|
      seqnum, tuple = row
      @tuples[seqnum] = tuple
    end
  end


  #=========================================================================
  # TemplateBag
  #-------------------------------------------------------------------------
  private

  # Returns true if the tuple matched a template (and was consumed).
  def consume_tuple(tuple)
    tuple_value = MIO.decode tuple
    node = @consume_requests.find_node { |r|
      template_value = MIO.decode r.template
      match template_value, tuple_value
    }
    return false unless node

    @consume_requests.delete_node node
    request = node.value

    # For a stream operation, move the request to the end of the request list
    # so that stream requests are satisfied in round robin fashion.  This
    # prevents any one requester from monopolizing tuples.
    @consume_requests << request if request.operation == :consume_stream

    @worker.region_result @port, request.channel, request.reqnum,
      request.operation, request.template, tuple, @seqnum
    true
  end

  # Returns true if the tuple matched any template.
  def read_tuple(tuple)
    tuple_value = MIO.decode tuple
    nodes = @read_requests.find_all_nodes { |r|
      template_value = MIO.decode r.template
      match template_value, tuple_value
    }
    return false if nodes.length == 0

    nodes.each do |node|
      request = node.value
      unless request.operation == :monitor_stream
        @read_requests.delete_node node
      end

      @worker.region_result @port, request.channel, request.reqnum,
        request.operation, request.template, tuple, @seqnum
    end
    true
  end


  # Returns true if the tuple matched a template *and* the tuple was consumed
  # (by, say, a 'take' request).  Otherwise, returns false; that is, if
  # *either* there was no match, or the tuple was not consumed (which happens
  # for 'read' requests).
  def match_consume(tuple)
    if consume_tuple tuple
      return true
    else
      read_tuple tuple
      return false
    end
  end

  def add_reading(request)
    @read_requests << request
  end

  def add_consuming(request)
    @consume_requests << request
  end

  def remove_reading(request)
    @read_requests.delete_if { |r| r.equal? request }
  end

  def remove_consuming(request)
    @consume_requests.delete_if { |r| r.equal? request }
  end


  def checkpoint_templates(txn, checkpoint_id)
    seqnum = 0  # synthesized seqnum for maintaining the order of templates
    txn.prepare("INSERT INTO RegionTemplates VALUES(?, ?, ?, ?, ?, ?)") do
      |insert_stmt|
      (@read_requests.to_a + @consume_requests.to_a).each do |request|
        seqnum += 1
        session_id = request.channel.session_id
        insert_stmt.execute checkpoint_id, @port, seqnum, session_id,
          request.operation, request.template
      end
    end
  end


  # The required block should take |session_id, request| and store the request
  # into GlobalSpace.@ongoing_requests.  This ties together the
  # RegionRequest objects restored in Region to the GlobalSpace.
  #
  # The {sessions} argument should map session_id to a Context object restored
  # in GlobalSpace.
  def restore_templates(state, checkpoint_id, sessions)
    state.db.execute("SELECT session_id,operation,template FROM RegionTemplates
                       WHERE checkpoint_id=? AND port=?
                       ORDER BY seqnum",
                      checkpoint_id, @port) do |row|
      session_id, operation_str, template = row
      operation = operation_str.to_sym
      context = sessions[session_id]
      unless context
        $log.err "Region#restore_templates: no Context found for " +
          "session_id=%#x", session_id
        exit 1
      end

      request = RegionRequest.new @worker, @port, operation, template, context
      case operation
      when :read, :read_all, :monitor, :monitor_stream
        @read_requests << request
      when :take, :take_all, :consume, :consume_stream
        @consume_requests << request
      else
        $log.err "Region#restore_templates: invalid operation %p " +
          "(session_id=%#x, template=%p)", operation, session_id, template
        exit 1
      end

      yield session_id, request
    end
  end


  public #===================================================================

  def checkpoint_state(txn, checkpoint_id)
    txn.execute("INSERT INTO Regions VALUES(?, ?, ?, ?, ?)",
                checkpoint_id, @port, @tuples.seqnum, @node_id, @session_id)

    checkpoint_tuples txn, checkpoint_id
    checkpoint_templates txn, checkpoint_id
  end


  # The required block should take |session_id, request| and store the request
  # into GlobalSpace.@ongoing_requests.  This ties together the
  # RegionRequest objects restored in TemplateBag to the GlobalSpace.
  #
  # The {sessions} argument should map session_id to a Context object restored
  # in GlobalSpace.
  def restore_state(state, checkpoint_id, tuple_seqnum, sessions, &block)
    restore_tuples state, checkpoint_id, tuple_seqnum
    restore_templates state, checkpoint_id, sessions, &block
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


  # XXX return seqnum?
  # This operation is an exception in not producing any result.
  def write(tuple, channel)
    # NOTE: We must increment @seqnum before the call to match_consume, so
    #       that match_consume can know what seqnum the tuple would have had
    #       in @tuples if saved to @tuples rather than consumed.  This allows
    #       iterating operations to properly continue at the right index in
    #       @tuples on each iteration.
    @seqnum += 1
    unless match_consume tuple
      @tuples[@seqnum] = tuple
    end
    nil
  end


  def read(reqnum, template, channel)
    tuple, _ = find_match template
    if tuple
      return nil, tuple, 0
    else
      request = RegionRequest.new @worker, @port, reqnum, :read,template,channel
      add_reading request
      return request, nil
    end
  end


  def readp(reqnum, template, channel)
    tuple, _ = find_match template
    return nil, tuple, 0
  end


  def take(reqnum, template, channel)
    tuple, index = find_match template
    if tuple
      @tuples.clear_at index
      return nil, tuple, 0
    else
      request = RegionRequest.new @worker, @port, reqnum, :take,template,channel
      add_consuming request
      return request, nil
    end
  end


  def takep(reqnum, template, channel)
    tuple, index = find_match template
    @tuples.clear_at index if tuple
    return nil, tuple, 0
  end


  def read_all(reqnum, template, channel, cursor=0)
    tuple, index = find_next_match template, cursor
    return nil, tuple, index
  end


  def take_all(reqnum, template, channel, cursor=0)
    tuple, index = find_next_match template, cursor
    @tuples.clear_at index if tuple
    return nil, tuple, index
  end


  def monitor(reqnum, template, channel, cursor=0)
    tuple, index = find_next_match template, cursor
    if tuple
      return nil, tuple, index
    else
      request = RegionRequest.new @worker, @port, reqnum,
        :monitor, template, channel
      add_reading request
      return request, nil
    end
  end


  def consume(reqnum, template, channel, cursor=0)
    tuple, index = find_next_match template, cursor
    if tuple
      @tuples.clear_at index
      return nil, tuple, index
    else
      request = RegionRequest.new @worker, @port, reqnum,
        :consume, template, channel
      add_consuming request
      return request, nil
    end
  end


  def monitor_stream_setup(reqnum, template, channel)
    request = RegionRequest.new @worker, @port, reqnum,
      :monitor_stream, template, channel
    add_reading request
    return request, nil
  end


  def consume_stream_setup(reqnum, template, channel)
    request = RegionRequest.new @worker, @port, reqnum,
      :consume_stream, template, channel
    add_consuming request
    return request, nil
  end


  def monitor_stream_start(reqnum, template, channel)
    template_value = MIO.decode template
    @tuples.each_with_index do |tuple, index|
      tuple_value = MIO.decode tuple
      if match template_value, tuple_value
        # XXX pass template and tuple?
        @worker.region_result @port, channel, reqnum, :monitor_stream,
          template, tuple, index
      end
    end
  end


  def consume_stream_start(reqnum, template, channel)
    template_value = MIO.decode template
    @tuples.each_with_index do |tuple, index|
      tuple_value = MIO.decode tuple
      if match template_value, tuple_value
        @tuples.clear_at index
        # XXX pass template and tuple?
        @worker.region_result @port, channel, reqnum, :consume_stream,
          template, tuple, index
      end
    end
  end


  def cancel(request)
    case request.operation
    when :take, :take_all, :consume, :consume_stream
      remove_consuming request
    when :read, :read_all, :monitor, :monitor_stream
      remove_reading request
    else
      fail "INTERNAL ERROR: unhandled request operation %p", request.operation
    end
  end


  def shutdown
    @tuples.clear
    @tuples = nil
    @templates = nil
  end


  def dump(resource="all")
    $stderr.puts "==========================================================="
    $stderr.printf "%s Region %#x (port=%#x):\n",
      (Port.global?(@port) ? "global" : "local"), object_id, @port

    $stderr.puts "tuples ----------------------------------------------------"
    @tuples.each { |tuple| $stderr.puts tuple }

    if resource == "all" || resource == "requests" || resource == "read"
      $stderr.puts "read requests ---------------------------------------------"
      @read_requests.each { |r| $stderr.puts r.inspect }
    end

    if resource == "all" || resource == "requests" || resource == "consume"
      $stderr.puts "consume requests ------------------------------------------"
      @consume_requests.each { |r| $stderr.puts r.inspect }
    end
  end


  def inspect
    sprintf "\#<Marinda::Region:%#x @port=%p, @tuples=%#x, @templates=%#x>",
      object_id, @port, @tuples.object_id, @templates.object_id
  end

end

end  # module Marinda
