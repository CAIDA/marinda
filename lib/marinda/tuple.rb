#############################################################################
## Tuple and template classes.
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
## $Id: tuple.rb,v 1.23 2009/03/17 01:11:02 youngh Exp $
#############################################################################

require 'marinda/flags'

module Marinda

# conceptual tuple structure:
#
#   [ flags{8}, sender{64}, value_1, value_2, ... ]
#
# flags (bit position):
#
#   system (0):
#         generated by a privileged system service (port 1-1024);
#         provides protection against spoofing attacks
#
#   access right (1):
#         tuple contains an access right (file descriptor) that is being
#         passed; check the tuple itself to determine the kind of right
#         and the action to take
#
#   forwarding (2):
#         tuple is being forwarded:
#
#           [ flags, original-sender, forwarder, value_1, ... ]
#
#   sticky (3):
#         a take() on the tuple by anyone other than the original sender
#         does a read() instead
#
#   auto-increment (4):
#         a read() atomically increments the integer value in the tuple
#         before returning the tuple (tuple is also updated in the tuple
#         space, as if a take() followed by a write() happened); values
#         may wrap to zero; tuple must be of the form <name, value>:
#
#                               [LAST, 1]
#            read [LAST, ?i] => [LAST, 2]
#            read [LAST, ?i] => [LAST, 3]
#
#   auto-decrement (5):
#
# NOTE: auto-increment and auto-decrement can't both be set.

class Tuple

  include Flags

  flag :system, 0x01
  flag :sticky, 0x02
  flag :auto_increment, 0x04
  flag :auto_decrement, 0x08

  attr_accessor :seqnum, :flags, :sender, :forwarder, :access_fd
  attr_reader :values

  def initialize(sender, values)
    @seqnum = nil
    @flags = 0
    @sender = sender
    @forwarder = nil
    @access_fd = nil
    @values = values
  end

  def inspect
    sprintf "#<Marinda::Tuple:%#x @flags=0b%b, @sender=%#x, " +
      "@forwarder=%#x, @access_fd=%p @values=[%s]>", object_id, @flags,
      @sender, @forwarder, @access_fd,
      @values.map { |v| v || "nil" }.join(", ")
  end

  def to_yaml_properties
    %w{ @seqnum @flags @sender @forwarder @values }  # No @access_fd.
  end

  def to_s
    "Marinda::Tuple[" + @values.map { |v| v || "nil" }.join(", ") + "]"
  end

end


# conceptual template structure:
#  [ flags{8}, sender{64}, value_1, value_2, ... ]

class Template

  attr_accessor :reqnum
  attr_reader :flags, :sender, :values

  def initialize(sender, values)
    @reqnum = nil
    @flags = 0
    @sender = sender
    @values = values
  end

  def match(tuple)
    return true if @values.length == 0
    return false unless @values.length == tuple.values.length
    
    @values.each_with_index do |lhs, i|
      rhs = tuple.values[i]
      return false unless lhs == nil || lhs === rhs || rhs == nil
    end
    true
  end

  def inspect
    sprintf "#<Marinda::Template:%#x @reqnum=%p @flags=0b%b, @sender=%#x, " +
      "@values=[%s]>", object_id, @reqnum, @flags, @sender,
      @values.map { |v| v.inspect }.join(", ")
  end

  def to_yaml_properties
    %w{ @reqnum @flags @sender @values }
  end

  def to_s
    "Marinda::Template[" + @values.map { |v| v.inspect }.join(", ") + "]"
  end

end

end  # module Marinda
