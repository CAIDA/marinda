#############################################################################
## A generic utility for implementing named bit flags in classes.
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
## $Id: flags.rb,v 1.4 2009/03/17 00:56:35 youngh Exp $
#############################################################################

module Marinda

module Flags

  def self.included(mod)
    metaclass = class << mod; self; end
    metaclass.send(:define_method, :flag) do |name, value|
      name = name.to_s
      class_eval <<-END
      #{name.upcase}_FLAG = #{value}

      def #{name}?
	@flags ||= 0
	(@flags & #{name.upcase}_FLAG) != 0
      end

      def #{name}=(value)
	@flags ||= 0
        if value then @flags |= #{name.upcase}_FLAG
                 else @flags &= ~#{name.upcase}_FLAG end
      end
      END
    end

    metaclass.send(:define_method, :read_flag) do |name, value|
      name = name.to_s
      class_eval <<-END
      #{name.upcase}_FLAG = #{value}

      def #{name}?
	@flags ||= 0
	(@flags & #{name.upcase}_FLAG) != 0
      end
      END
    end
    mod
  end

end

end
