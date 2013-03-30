#############################################################################
## Implements common channel privilege bit flags used in Marinda::Channel
## and Marinda::Client.
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

module ChannelFlagReaders

  include Flags

  read_flag :can_read, 0x01
  read_flag :can_write, 0x02
  read_flag :can_take, 0x04

end


class ChannelFlags

  include Flags

  flag :can_read, 0x01
  flag :can_write, 0x02
  flag :can_take, 0x04

  attr_accessor :flags

  def initialize(flags=0)
    @flags = flags
  end

  def allow_all_privileges!
    self.can_read = true
    self.can_write = true
    self.can_take = true
  end

end

end
