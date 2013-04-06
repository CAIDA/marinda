#############################################################################
## Holds the tuples of tuple space regions using a Judy array.
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

require 'mioext'

module Marinda

# NOTE: We use 'channel' in the argument list of some methods, but the
#       actual type depends on whether a TupleBag exists in a local or
#       global tuple space.  In a local tuple space, the 'channel'
#       argument is actually a Channel.  In a global tuple space, the
#       'channel' argument is a GlobalSpace::Context.

class TupleBag

  attr_reader :seqnum

  def initialize(worker, port)
    @worker = worker
    @port = port

    # @seqnum should equal the highest seqnum actually assigned to a tuple
    # and not the next available seqnum.  The value 0 is special--it should
    # never be assigned to a tuple, and hence serves to indicate a seqnum
    # that is lower than all possible seqnums.
    @seqnum = 0
    @tuples = Judy::JudyL.new
  end


  def find_match(template_mio)
    template = MIO.decode template_mio

    @tuples.each_with_index do |tuple_mio, index|
      tuple = MIO.decode tuple_mio
      return tuple, index if match template, tuple
    end
  end


  def find_next_match(template_mio, cursor)
    template = MIO.decode template_mio

    loop do
      cursor = @tuples.next_index cursor
      return nil unless cursor

      tuple = MIO.decode @tuples[cursor]
      return tuple, cursor if match template, tuple
    end
  end


  def match(template, tuple)
    return true if template.length == 0
    return false unless template.length == tuple.length
    
    template.each_with_index do |lhs, i|
      rhs = tuple[i]
      return false unless lhs == nil || lhs === rhs || rhs == nil
    end
    true
  end


  public #===================================================================

  def checkpoint_state(txn, checkpoint_id, port)
    txn.prepare("INSERT INTO RegionTuples VALUES(?, ?, ?, ?)") do |insert_stmt|
      @tuples.each_with_index do |tuple, seqnum|
        insert_stmt.execute checkpoint_id, port, seqnum, tuple
      end
    end
  end


  def restore_state(state, checkpoint_id, tuple_seqnum, port)
    @seqnum = tuple_seqnum
    state.db.execute("SELECT seqnum,tuple FROM RegionTuples
                       WHERE checkpoint_id=? AND port=?
                       ORDER BY seqnum",
                      checkpoint_id, port) do |row|
      seqnum, tuple = row
      @tuples[seqnum] = tuple
    end
  end


  # XXX fix
  def prepare_write(tuple)
    @seqnum += 1
    tuple.seqnum = @seqnum
  end

  # XXX fix
  # The caller must have called prepare_write(tuple) prior to commit_write.
  def commit_write(tuple)
    @tuples[@seqnum] = tuple
  end

  def readp(template)
    tuple, _ = find_match template
    return tuple  # == nil if not found
  end

  def readp_next(template, cursor)
    tuple, _ = find_next_match template, cursor
    return tuple  # == nil if not found
  end

  def takep(template)
    tuple, index = find_match template
    @tuples.clear_at index if tuple
    return tuple
  end

  def takep_next(template, cursor)
    tuple, index = find_next_match template, cursor
    @tuples.clear_at index if tuple
    return tuple
  end

  def monitor_stream_existing(template, channel)
    @tuples.each do |tuple|
      if template.match tuple
        @worker.region_result @port, channel, :monitor_stream, template, tuple
      end
    end
  end

  def consume_stream_existing(template, channel)
    cursor = 0
    loop do
      cursor = @tuples.next_index cursor
      return unless cursor

      tuple = @tuples[cursor]
      if template.match tuple
        @tuples.delete_at cursor
        @worker.region_result @port, channel, :consume_stream, template, tuple
      end
    end
  end

  def dump(resource="all")
    if resource == "all" || resource == "tuples"
      $stderr.puts "tuples ----------------------------------------------------"
      @tuples.each { |tuple| $stderr.puts tuple }
    end
  end

  def shutdown
    @tuples.clear
    @tuples = nil
    self
  end

  def inspect
    sprintf("\#<Marinda::JudyTupleBag:%#x @seqnum=%d, @tuples=(%d)%#x>",
	    object_id, @seqnum, @tuples.count, @tuples.object_id)
  end

end

end  # module Marinda
