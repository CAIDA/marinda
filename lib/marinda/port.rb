#############################################################################
## Represents a region port identifier, which is a long integer with internal
## structure.
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

module Marinda

##---------------------------------------------------------------------------
## A port value is nominally an unsigned 64-bit value when externalized.
## Port values have the following structure, where subscripts are used to
## indicate the number of bits in a field:
##
##   <reserved:15, port-num:48, scope:1>
##
##    * port-num: special value 0 reserved for invalid/unknown
##    * scope: 0 means local; 1 means global
##---------------------------------------------------------------------------

class Port

  UNKNOWN = 0  # something that can never be confused with a valid port value

  LOCAL = 0
  GLOBAL = 1

  COMMONS = 1
  MIN_PORT_NUMBER = 1
  MAX_PORT_NUMBER = 2**48 - 1

  def Port.local?(port)
    port[1] == LOCAL
  end

  def Port.global?(port)
    port[1] == GLOBAL
  end

  def Port.make(scope, port_number)
    validate scope, port_number
    (port_number << 1) | scope
  end

  # Returns {port} if {port} is valid; otherwise returns nil.
  def Port.check_port(port)
    scope = port & 0x1
    port_number = port >> 1

    begin
      validate scope, port_number
    rescue ArgumentError
      return nil
    end

    return port
  end

  def Port.validate(scope, port_number)
    if port_number < Port::MIN_PORT_NUMBER ||
        port_number > Port::MAX_PORT_NUMBER
      raise ArgumentError, "port_number value out of range"
    end

    unless scope == LOCAL || scope == GLOBAL
      raise ArgumentError, "invalid scope"
    end
  end

end

end  # module Marinda
