/*
** A set of includes to pull in basic system-dependent types, especially
** the C99 intN_t family of types.
**
** --------------------------------------------------------------------------
** Author: Young Hyun
** Copyright (C) 2009 The Regents of the University of California.
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU General Public License as published by
** the Free Software Foundation; either version 2 of the License, or
** (at your option) any later version.
** 
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
** 
** You should have received a copy of the GNU General Public License
** along with this program; if not, write to the Free Software
** Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
**/

#if defined(__APPLE__)
#define _BSD_SOCKLEN_T_
#endif

#include <sys/types.h>

#if defined(HAVE_UNISTD_H)
#include <unistd.h>
#endif

#if defined(HAVE_STDINT_H)
#include <stdint.h>
#endif
