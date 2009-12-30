/*
** Ruby bindings to the Marinda messaging routines.
**
** --------------------------------------------------------------------------
** Author: Young Hyun
** Copyright (C) 2008, 2009 The Regents of the University of California.
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

#include "systypes.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "ruby.h"
#include "compat.h"

#include "mio_keywords.h"
#include "mio_msg.h"
#include "mio_base64.h"
#include "mio.h"

/* XXX can use a smaller sprintf_buf to save space */
typedef struct {
  /* message_buf is also used to return error messages to the user */
  char message_buf[MIO_MSG_MAX_MESSAGE_SIZE + 1];  /* + NUL-term */
  char sprintf_buf[MIO_MSG_MAX_MESSAGE_SIZE + 1];  /* + NUL-term */
} mio_data_t;

static VALUE mMarinda;
static VALUE cMIO;

/*=========================================================================*/

static void
mio_free(void *data)
{
  mio_data_t *mio_data = (mio_data_t *)data;

  if (mio_data) {
    /* nothing to do */
  }
}

static VALUE
mio_alloc(VALUE klass)
{
  return Data_Wrap_Struct(klass, 0, mio_free, 0);
}


static VALUE
mio_init(VALUE self)
{
  mio_data_t *data = NULL;

  data = ALLOC(mio_data_t);
  memset(data, 0, sizeof(mio_data_t));
  DATA_PTR(self) = data;
  return self;
}


/*
** {tuple} must be a T_ARRAY (the caller should have checked this),
** and it must not be empty (the caller should have dealt with the case
** of an empty array).
**
** {level} is the nesting level, where 0 indicates the top-level array,
** 1 indicates an array element in the top level array, etc.
**
** {index} should be the starting index into {data->message_buf} where
** {tuple} should be marshalled.
**
** Returns the next starting index past the marshalled {tuple}.  The return
** value is also equivalent to the length of the complete marshalled
** message up to this point.
**
** In case of error (e.g., exceeding the max message size), this raises a
** Ruby runtime exception.
*/
static size_t
mio_encode_array(mio_data_t *data, size_t level, size_t index, VALUE tuple)
{
  long tlen = RARRAY_LEN(tuple);
  long i;
  VALUE v;
  int l;

  for (i = 0; i < tlen; i++) {
    /* We should always be able to add ',' here because of the extra space
       allocated for NUL; that is, in the worst case, we'll overwrite the
       space allocated for the trailing NUL (because {index} has to be less
       than MIO_MSG_MAX_MESSAGE_SIZE at this point).  This may cause the
       bounds check to fail later when we try to add the next element, so
       be careful. */
    if (i > 0) { data->message_buf[index++] = ','; }

    l = 0;  /* bytes to copy from sprintf_buf to message_buf */
    v = RARRAY_PTR(tuple)[i];
    switch (TYPE(v)) {
    case T_STRING:
      if (RSTRING_LEN(v) == 0) {  /* special case: empty string */
	strcpy(data->sprintf_buf, "$$");
	l = 2;
      }
      else if (RSTRING_LEN(v) > MIO_MSG_MAX_RAW_VALUE_SIZE) {
	sprintf(data->message_buf, "string value too long; length %ld > max "
		"length %ld", RSTRING_LEN(v), (long)MIO_MSG_MAX_RAW_VALUE_SIZE);
	rb_raise(rb_eRuntimeError, data->message_buf);
      }
      else {
	*data->sprintf_buf = '$'; /* note: add 1 to l to adjust for this '$' */
	l = (int)base64_encode((unsigned char *)RSTRING_PTR(v),
			       RSTRING_LEN(v), data->sprintf_buf + 1) + 1;
      }
      break;

    case T_FIXNUM:
      l = sprintf(data->sprintf_buf, "%ld", FIX2LONG(v));
      break;

    case T_FLOAT:
      *data->sprintf_buf = '%'; /* note: add 1 to l to adjust for this '%' */
      /* XXX handle +/- infinity and NaN */
      l = sprintf(data->sprintf_buf + 1, "%f", RFLOAT(v)->value) + 1;
      break;

    case T_NIL:
      data->sprintf_buf[0] = '_';
      data->sprintf_buf[1] = '\0';
      l = 1;
      break;

    case T_ARRAY:
      if (level >= MIO_MSG_MAX_NESTING) {
	sprintf(data->message_buf, "array nesting level exceeds max %d",
		MIO_MSG_MAX_NESTING);
	rb_raise(rb_eRuntimeError, data->message_buf);
      }
      else if (index + 2 >= MIO_MSG_MAX_MESSAGE_SIZE) { /* accomodate "()" */
	sprintf(data->message_buf, "message length exceeds max %d",
		MIO_MSG_MAX_MESSAGE_SIZE);
	rb_raise(rb_eRuntimeError, data->message_buf);
      }
      else {
	data->message_buf[index++] = '(';
	index = mio_encode_array(data, level + 1, index, v);
	data->sprintf_buf[0] = ')';
	data->sprintf_buf[1] = '\0';
	l = 1;
      }
      break;

    /* support T_TRUE and T_FALSE? */
    default:
      rb_raise(rb_eRuntimeError, "unsupported element type; must be nil, "
	       "string, fixnum, float, or array");
    }

    if (index + l >= MIO_MSG_MAX_MESSAGE_SIZE) {
      sprintf(data->message_buf, "message length exceeds max %d",
	      MIO_MSG_MAX_MESSAGE_SIZE);
      rb_raise(rb_eRuntimeError, data->message_buf);
    }
    else {
      memcpy(&data->message_buf[index], data->sprintf_buf, l);
      index += l;
      data->message_buf[index] = '\0';
    }
  }

  return index;
}


/*
** Returns the encoded string form of the array {tuple}, which may have
** nested arrays up to the max nesting level.  {tuple} may not be nil,
** though it can be empty.  {tuple} may only contain nils, fixnums,
** strings, floats, or arrays.  Note that bignums aren't allowed currently.
**
** In case of error (e.g., exceeding the max message size), this raises a
** Ruby runtime exception.
*/
static VALUE
mio_encode_tuple(VALUE self, VALUE tuple)
{
  mio_data_t *data = NULL;

  Data_Get_Struct(self, mio_data_t, data);

  if (NIL_P(tuple)) {
    rb_raise(rb_eArgError, "tuple argument is nil");
  }
  else if (TYPE(tuple) == T_ARRAY) {
    if (RARRAY_LEN(tuple) == 0) {
      strcpy(data->message_buf, "E"); /* special case: empty top-level */
    }
    else {
      mio_encode_array(data, 0, 0, tuple);
    }
    return rb_str_new2(data->message_buf);
  }
  else {
    rb_raise(rb_eArgError, "tuple argument must be an array");
  }

  return Qnil;  /*NOTREACHED*/
}


/***************************************************************************/
/***************************************************************************/

#define DEF_LIMIT(name) \
  rb_define_const(cMIO, #name, ULONG2NUM(MIO_MSG_##name));

void
Init_mio(void)
{
  ID private_class_method_ID, private_ID;
  ID dup_ID, clone_ID;

  mMarinda = rb_define_module("Marinda");
  cMIO = rb_define_class_under(mMarinda, "MIO", rb_cObject);

  DEF_LIMIT(MAX_RAW_VALUE_SIZE);
  DEF_LIMIT(MAX_ENCODED_VALUE_SIZE);
  DEF_LIMIT(MAX_MESSAGE_SIZE);
  DEF_LIMIT(MAX_NESTING);

  rb_define_alloc_func(cMIO, mio_alloc);

  rb_define_method(cMIO, "initialize", mio_init, 0);
  rb_define_method(cMIO, "encode_tuple", mio_encode_tuple, 1);

  private_class_method_ID = rb_intern("private_class_method");
  private_ID = rb_intern("private");
  dup_ID = rb_intern("dup");
  clone_ID = rb_intern("clone");

  rb_funcall(cMIO, private_ID, 1, ID2SYM(dup_ID));
  rb_funcall(cMIO, private_ID, 1, ID2SYM(clone_ID));
}
