#############################################################################
## Holds the tuples of tuple space regions using a Judy array.
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

require 'yaml'
require 'judy'

module Marinda

# NOTE: We use 'channel' in the argument list of some methods, but the
#       actual type depends on whether a TupleBag exists in a local or
#       global tuple space.  In a local tuple space, the 'channel'
#       argument is actually a Channel.  In a global tuple space, the
#       'channel' argument is a GlobalSpace::Context.

class JudyTupleBag

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

  public #===================================================================

  def checkpoint_state(txn, checkpoint_id, port)
    txn.prepare("INSERT INTO RegionTuples VALUES(?, ?, ?, ?)") do |insert_stmt|
      @tuples.each do |tuple|
        tuple_yaml = YAML.dump tuple
        insert_stmt.execute checkpoint_id, port, tuple.seqnum, tuple_yaml
      end
    end
  end


  def restore_state(state, checkpoint_id, tuple_seqnum, port)
    @seqnum = tuple_seqnum
    state.db.execute("SELECT tuple FROM RegionTuples
                       WHERE checkpoint_id=? AND port=?
                       ORDER BY seqnum",
                      checkpoint_id, port) do |row|
      tuple = YAML.load row[0]
      @tuples[tuple.seqnum] = tuple
    end
  end


  def write(tuple)
    @seqnum += 1
    tuple.seqnum = @seqnum
    @tuples[@seqnum] = tuple
  end

  def readp(template)
    @tuples.each do |tuple|
      return tuple if template.match tuple
    end
    nil
  end

  def readp_next(template, cursor)
    loop do
      cursor = @tuples.next_index cursor
      return nil unless cursor

      tuple = @tuples[cursor]
      return tuple if template.match tuple
    end
  end

  def takep(template)
    @tuples.each_index do |i|
      tuple = @tuples[i]
      return @tuples.delete_at i if template.match tuple
    end
    nil
  end

  def takep_next(template, cursor)
    loop do
      cursor = @tuples.next_index cursor
      return nil unless cursor

      tuple = @tuples[cursor]
      return @tuples.delete_at cursor if template.match tuple
    end
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
      @tuples.each { |tuple| $stderr.puts tuple.to_s }
    end
  end

  def shutdown
    @tuples.each do |tuple|
      fd = tuple.access_fd
      if fd
	fd.close
	tuple.access_fd = nil
      end
    end
    @tuples.free_array()
    @tuples = nil
    self
  end

  def inspect
    sprintf("\#<Marinda::JudyTupleBag:%#x @seqnum=%d, @tuples=(%d)%#x>",
	    object_id, @seqnum, @tuples.count, @tuples.object_id)
  end

end

end  # module Marinda
