/*
** Defines for the client/server messages sent/received on the control socket.
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
*/

#ifndef __MIO_MSG_H__
#define __MIO_MSG_H__

/*
** Note: Be careful when performing base64 encoding/decoding with
**       mio_base64.c.  First, base64_encode() will add a NUL terminator,
**       so the encode buffer must be MIO_MSG_MAX_ENCODED_VALUE_SIZE + 1
**       bytes long.  Second, base64_decode() does not add a NUL
**       terminator, but we manually add a NUL terminator when decoding
**       strings (vs. blobs), and so the decode buffer must be
**       MIO_MSG_MAX_RAW_VALUE_SIZE + 1 bytes long.
**
**       MIO_MSG_MAX_ENCODED_VALUE_SIZE must be < MIO_MSG_MAX_MESSAGE_SIZE.
*/
#define MIO_MSG_MAX_WORDS 1024
#define MIO_MSG_MAX_RAW_VALUE_SIZE 24575 /* max input for base64 encoding */
#define MIO_MSG_MAX_ENCODED_VALUE_SIZE 32768 /* decodes to max 24575 bytes */
#define MIO_MSG_MAX_MESSAGE_SIZE 65535  /* 2^16 - 1 */
#define MIO_MSG_MAX_NESTING 255

#endif /* __MIO_MSG_H__ */
