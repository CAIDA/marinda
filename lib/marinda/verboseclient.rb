#############################################################################
## A wrapper around Marinda::Client that will print out the arguments and
## the result of each Marinda::Client method call.
##
## --------------------------------------------------------------------------
## Copyright (C) 2009 The Regents of the University of California.
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

class VerboseClient

  def initialize(ts, label)
    @ts = ts
    @label = label
  end

  def method_missing(name, *args, &block)
    printf "%s:%s(%s)\n", @label, name.to_s,
      args.map { |v| v.inspect }.join(", ")
    retval = @ts.__send__ name, *args, &block
    printf "=> %p\n", retval
    retval
  end

end

end  # module Marinda
