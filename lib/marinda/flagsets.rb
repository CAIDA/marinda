#############################################################################
## Implements common channel privilege bit flags used in Marinda::Channel
## and Marinda::Client.
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

require 'marinda/flags'

module Marinda

module ChannelFlagReaders

  include Flags

  read_flag :is_global, 0x01
  read_flag :is_commons, 0x02
  read_flag :can_read, 0x04
  read_flag :can_write, 0x08
  read_flag :can_write_sticky, 0x10
  read_flag :can_take, 0x20
  read_flag :can_forward, 0x40
  read_flag :can_pass_access, 0x80
  read_flag :can_open_any_port, 0x100

end


class ChannelFlags

  include Flags

  flag :is_global, 0x01
  flag :is_commons, 0x02
  flag :can_read, 0x04
  flag :can_write, 0x08
  flag :can_write_sticky, 0x10
  flag :can_take, 0x20
  flag :can_forward, 0x40
  flag :can_pass_access, 0x80
  flag :can_open_any_port, 0x100

  attr_accessor :flags

  def initialize(flags=0)
    @flags = flags
  end

  def privileges
    flags = ChannelFlags.new @flags
    flags.is_global = flags.is_commons = false
    flags
  end

  def allow_all_privileges!
    self.can_read = true
    self.can_write = true
    self.can_write_sticky = true
    self.can_take = true
    self.can_forward = true
    self.can_pass_access = true
    self.can_open_any_port = true
  end

  def allow_commons_privileges!
    self.can_read = true
    self.can_write = true
    self.can_write_sticky = false
    self.can_take = true
    self.can_forward = false
    self.can_pass_access = true
    self.can_open_any_port = false

    # XXX enable privileges during development only
    self.can_write_sticky = true
    self.can_forward = true
    self.can_open_any_port = true
  end

end

end
