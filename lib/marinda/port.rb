#############################################################################
## Represents a region port identifier, which is a long integer with internal
## structure.
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
## $Id: port.rb,v 1.13 2009/03/17 01:05:32 youngh Exp $
#############################################################################

require 'ostruct'

module Marinda

##---------------------------------------------------------------------------
## A port value is nominally an unsigned 64-bit value when externalized.
## Port values have the following structure, where subscripts are used to
## indicate the number of bits in a field:
##
##   public: <reserved_14, port-num_48, scope_1, 0_1>
##
##    * port-num -- special value 1 reserved for commons
##    * scope -- 0 means local; 1 means global
##    * 0_1 -- indicates public port
##
##   private: <port-num_48, scope_15, 1_1>
##
##    * port-num -- value 1 not reserved (no such thing as private commons)
##    * scope -- node ID; 0 means global node
##    * 1_1 -- indicates private port
##
## NOTE: Port-num 0 is reserved for both public and private ports.
##       This allows us to reserve the port value 0 to indicate an unknown
##       or invalid port.
##
## NOTE: Putting the port number in the leftmost field within each port value
##       has the following two advantages:
##
##         1) the port value will typically be a Fixnum, allowing efficient
##            storage and matching, and
##
##         2) the port value can grow without bounds, mitigating potential
##            denial-of-service attacks (since uniqueness of port numbers
##            over time and space is essential for tuple space security).
##---------------------------------------------------------------------------

class Port

  UNKNOWN = 0  # something that can never be confused with a valid port value

  COMMONS_PORTNUM = 1
  PUBLIC_PORTNUM_RESERVED = 1024
  PRIVATE_PORTNUM_RESERVED = 1024

  GLOBAL_SPACE_NODE_ID = 0
  MAX_NODE_ID = 2**15 - 1

  def Port.decode(port)
    retval = OpenStruct.new
    if public? port
      retval.private = false
      retval.public = true

      scope = port[1]
      retval.scope = scope
      retval.local = (scope == 0)
      retval.global = (scope == 1)

      port_number = port >> 2
      retval.port_number = port_number
    else
      retval.private = true
      retval.public = false

      scope = (port >> 1) & 0x7FFF
      retval.scope = scope

      port_number = port >> 16
      retval.port_number = port_number
    end
    retval
  end

  def Port.public?(port)
    port[0] == 0
  end

  def Port.private?(port)
    port[0] == 1
  end

  def Port.scope_of(port)
    private?(port) ? ((port >> 1) & 0x7FFF) : port[1]
  end

  def Port.same_scope?(port1, port2)
    if (private? port1 and private? port2) or
	(public? port1 and public? port2)
      scope_of(port1) == scope_of(port2)
    else
      false
    end
  end

  def Port.same_local_scope?(port, scope)
    private?(port) ? scope_of(port) == scope : port[1] == 0
  end

  def Port.global?(port)
    (public?(port) && scope_of(port) == 1) ||
      (private?(port) && scope_of(port) == GLOBAL_SPACE_NODE_ID)
  end

  def Port.commons?(port)
    public?(port) ? (port >> 2 == COMMONS_PORTNUM) : false
  end

  def Port.global_commons?(port)
    global?(port) && commons?(port)
  end

  def Port.make_public_local(port_number=COMMONS_PORTNUM)
    make_public 0, port_number
  end

  def Port.make_public_global(port_number=COMMONS_PORTNUM)
    make_public 1, port_number
  end

  def Port.make_private_global(port_number)
    make_private GLOBAL_SPACE_NODE_ID, port_number
  end

  # [ reserved{14}, port_number{48}, scope=<0=local|1=global>{1}, 0{1} ]
  def Port.make_public(scope, port_number=COMMONS_PORTNUM)
    scope >= 0 && scope <= 1 or
      fail "scope #{scope} is out of range"
    p = port_number << 2
    s = (scope == 0 ? 0 : 2)
    p | s
  end

  # [ port_number{48}, scope{15}, 1{1} ]
  def Port.make_private(scope, port_number)
    scope >= 0 && scope <= MAX_NODE_ID or
      fail "scope #{scope} is out of range"
    p = port_number << 16
    s = scope << 1
    p | s | 1
  end

end

end  # module Marinda
