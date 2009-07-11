#############################################################################
## Holds the tuples of tuple space regions.
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
## $Id: tuplebag.rb,v 1.16 2009/03/17 01:10:04 youngh Exp $
#############################################################################

require 'yaml'
require 'marinda/list'

module Marinda

class TupleBag

  attr_reader :seqnum

  def initialize
    # @seqnum should equal the highest seqnum actually assigned to a tuple
    # and not the next available seqnum.  The value 0 is special--it should
    # never be assigned to a tuple, and hence serves to indicate a seqnum
    # that is lower than all possible seqnums.
    @seqnum = 0
    @tuples = List.new
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
      @tuples.push tuple
    end
  end


  def write(tuple)
    @seqnum += 1
    tuple.seqnum = @seqnum
    @tuples << tuple
  end

  def readp(template)
    @tuples.find { |tuple| template.match tuple }
  end

  def readp_next(template, cursor)
    @tuples.find { |tuple| tuple.seqnum > cursor and template.match tuple }
  end

  def takep(template)
    node = @tuples.find_node { |tuple| template.match tuple }
    return nil unless node

    @tuples.delete_node node
    node.value
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
    @tuples = nil
    self
  end

  def inspect
    sprintf("\#<Marinda::TupleBag:%#x @seqnum=%d, @tuples=(%d)%#x>",
	    object_id, @seqnum, @tuples.length, @tuples.object_id)
  end

end

end  # module Marinda
