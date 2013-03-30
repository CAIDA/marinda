#############################################################################
## Tuple and template classes.
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

require 'marinda/flags'

module Marinda

# conceptual tuple structure:
#
#   [ seqnum, flags{8}, value_1, value_2, ... ]
#
# flags (bit position):
#
#

class Tuple

  include Flags

#  flag :system, 0x01
#  flag :sticky, 0x02

  attr_accessor :seqnum, :flags
  attr_reader :values_mio

  def self.from_mio(s)
    # 0:seqnum   1:flags   2:values_mio
    v =  MIO.decode s
    retval = Tuple.new v[2]
    retval.seqnum = v[0]
    retval.flags = v[1]
    retval
  end

  def initialize(values_mio)
    @seqnum = nil
    @flags = 0
    @values_mio = values_mio
    @values = nil
  end

  def values
    @values = MIO.decode @values_mio unless @values
    @values
  end

  def inspect
    @values = MIO.decode @values_mio unless @values
    sprintf "#<Marinda::Tuple:%#x @flags=0b%b, @values=[%s]>",
      object_id, @flags, @values.map { |v| v.inspect }.join(", ")
  end

  def to_yaml_properties
    @values = MIO.decode @values_mio unless @values
    %w{ @seqnum @flags @values }
  end

  def to_mio  # XXX store version info?
    MIO.encode [ @seqnum, @flags, @values_mio ]
  end

  def to_s
    "Marinda::Tuple[" + @values.map { |v| v.inspect }.join(", ") + "]"
  end

end


# conceptual template structure:
#  [ reqnum, value_1, value_2, ... ]

class Template

  attr_accessor :reqnum
  attr_reader :values_mio

  def self.from_mio(s)
    # 0:reqnum   1:values_mio
    v =  MIO.decode s
    retval = Template.new v[1]
    retval.reqnum = v[0]
    retval
  end

  def initialize(values_mio)
    @reqnum = nil
    @values_mio = values_mio
    @values = nil
  end

  def match(tuple)
    @values = MIO.decode @values_mio unless @values
    return true if @values.length == 0
    return false unless @values.length == tuple.values.length
    
    @values.each_with_index do |lhs, i|
      rhs = tuple.values[i]
      return false unless lhs == nil || lhs === rhs || rhs == nil
    end
    true
  end

  def inspect
    @values = MIO.decode @values_mio unless @values
    sprintf "#<Marinda::Template:%#x @reqnum=%p @values=[%s]>",
      object_id, @reqnum, @values.map { |v| v.inspect }.join(", ")
  end

  def to_yaml_properties
    @values = MIO.decode @values_mio unless @values
    %w{ @reqnum @values }
  end

  def to_mio  # XXX store version info?
    MIO.encode [ @reqnum, @values_mio ]
  end

  def to_s
    "Marinda::Template[" + @values.map { |v| v.inspect }.join(", ") + "]"
  end

end

end  # module Marinda
