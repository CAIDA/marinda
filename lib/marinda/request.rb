#############################################################################
## A RegionRequest associates an outstanding Marinda operation (e.g., take)
## with internal state in the tuple space.
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

module Marinda

class RegionRequest

  attr_reader :worker, :port, :operation, :template, :channel

  # In GlobalSpaceMux, the initialization arguments are
  #
  #     worker => LocalSpace
  #     channel => Channel
  #
  # In GlobalSpace, the initialization arguments are
  #
  #     worker => GlobalSpace
  #     channel => Context
  #
  # Basically, {worker} should be the main event-dispatching thread.  When
  # a request is fulfilled, the {worker} is notified (from TemplateBag),
  # and it is then the {worker}'s job to notify the {channel}.
  #
  # In all situations, {operation} should be a symbol like :read, :take, etc.
  def initialize(worker, port, operation, template, channel)
    @worker = worker
    @port = port
    @operation = operation
    @template = template
    @channel = channel
  end

  public # ================================================================

  def inspect
    sprintf "\#<RegionRequest:%#x @worker=%#x @port=%#x @operation=%p, " +
      "@template=%p, @channel=%#x>", object_id, @worker.object_id, @port,
      @operation, @template, @channel.object_id
  end

end

end  # module Marinda
