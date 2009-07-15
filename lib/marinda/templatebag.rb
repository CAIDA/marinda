#############################################################################
## Holds the templates of outstanding Marinda operations (e.g., take).
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
## $Id: templatebag.rb,v 1.22 2009/03/17 01:09:36 youngh Exp $
#############################################################################

require 'marinda/list'

module Marinda

class TemplateBag

  def initialize(worker, port)
    @worker = worker
    @port = port
    @read_requests = List.new  # blocked read, monitor, read_all, etc. requests
    @consume_requests = List.new  # blocked take etc. requests
  end

  private #==================================================================

  # Returns true if the tuple matched a template (and was consumed).
  def consume_tuple(tuple, seqnum)
    node = @consume_requests.find_node { |r| r.template.match tuple }
    return false unless node

    @consume_requests.delete_node node
    request = node.value
    tuple.seqnum = seqnum
    @worker.region_result @port, request.channel, request.operation,
      request.template, tuple
    true
  end

  # Returns true if the tuple matched any template.
  def read_tuple(tuple, seqnum)
    nodes = @read_requests.find_all_nodes { |r| r.template.match tuple }
    return false if nodes.length == 0

    nodes.each do |node|
      @read_requests.delete_node node
      request = node.value
      tuple.seqnum = seqnum
      @worker.region_result @port, request.channel, request.operation,
	request.template, tuple
    end
    true
  end

  public #==================================================================

  def checkpoint_state(txn, checkpoint_id, port)
    seqnum = 0  # synthesized seqnum for maintaining the order of templates
    txn.prepare("INSERT INTO RegionTemplates VALUES(?, ?, ?, ?, ?, ?)") do
      |insert_stmt|
      (@read_requests.to_a + @consume_requests.to_a).each do |request|
        seqnum += 1
        session_id = request.channel.session_id
        template_yaml = YAML.dump request.template
        insert_stmt.execute checkpoint_id, port, seqnum, session_id,
          request.operation, template_yaml
      end
    end
  end


  # The required block should take |session_id, request| and store the request
  # into GlobalSpaceDemux.@ongoing_requests.  This ties together the
  # RegionRequest objects restored in TemplateBag to the GlobalSpaceDemux.
  #
  # The {sessions} argument should map session_id to a Context object restored
  # in GlobalSpaceDemux.
  def restore_state(state, checkpoint_id, port, sessions, worker)
    state.db.execute("SELECT session_id,operation,template FROM RegionTemplates
                       WHERE checkpoint_id=? AND port=?
                       ORDER BY seqnum",
                      checkpoint_id, port) do |row|
      session_id, operation_str, template_yaml = row
      operation = operation_str.to_sym
      template = YAML.load template_yaml
      context = sessions[session_id]
      unless context
        $log.err "TemplateBag#restore_state: no Context found for " +
          "session_id=%#x", session_id
        exit 1
      end

      request = RegionRequest.new worker, port, operation, template, context
      case operation
      when :read, :monitor, :read_all then @read_requests << request
      when :take then @consume_requests << request
      else
        $log.err "TemplateBag#restore_state: invalid operation %p " +
          "(session_id=%#x, template=%p)", operation, session_id, template
        exit 1
      end

      yield session_id, request
    end
  end


  # Returns true if the tuple matched a template *and* the tuple was consumed
  # (by, say, a 'take' request).  Otherwise, returns false; that is, if
  # *either* there was no match, or the tuple was not consumed (which happens
  # for 'read' requests).
  def match_consume(tuple, seqnum)
    if consume_tuple tuple, seqnum
      return true
    else
      read_tuple tuple, seqnum
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

  def dump(resource="all")
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
    sprintf "\#<Marinda::TemplateBag:%#x @read_requests=(%d)%#x, " +
      "@consume_requests=(%d)%#x>", object_id,
      @read_requests.length, @read_requests.object_id,
      @consume_requests.length, @consume_requests.object_id
  end

end

end  # module Marinda
