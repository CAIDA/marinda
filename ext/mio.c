/*
** Ruby bindings to the Marinda messaging routines.
**
** --------------------------------------------------------------------------
** Author: Young Hyun
** Copyright (C) 2009 Young Hyun
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

/* XXX can use a smaller value_buf to save space */
typedef struct {
  /* message_buf is also used to return error messages to the user */
  char message_buf[MIO_MSG_MAX_MESSAGE_SIZE + 1];  /* + NUL-term */
  char value_buf[MIO_MSG_MAX_MESSAGE_SIZE + 1];  /* + NUL-term */
} mio_data_t;

static const char base64_encode_tbl[64] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static VALUE mMarinda;
static VALUE cMIO;

/*=========================================================================*/

static void
fail_message_length(mio_data_t *data)
{
  sprintf(data->message_buf, "message length exceeds max %d",
	  MIO_MSG_MAX_MESSAGE_SIZE);
  rb_raise(rb_eRuntimeError, data->message_buf);
}


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
** This represents a fixnum with a base-64 number (NOT base-64 encoding)
** instead of with a decimal number.  Like all textual representations,
** the most-significant digit comes first, and a leading minus sign is
** used to represent a negative number.
**
** Examples:
**
**   >> m.encode_tuple [1] => "B"
**   >> m.encode_tuple [-1] => "-B"
**
**   On 64-bit platforms:
**
**   >> m.encode_tuple [2**32] => "EAAAAA"
**   >> m.encode_tuple [-2**32] => "-EAAAAA"          # 'A' == all bits clear
**   >> m.encode_tuple [2**62 - 1] => "D//////////"   # '/' == all bits set
**   >> m.encode_tuple [-2**62] => "-EAAAAAAAAAA"
**
**      -2**32 == -4294967296 (10 decimal digits vs. 6 base64 digits)
**      -2**62 == -4611686018427387904 (19 decimal digits vs. 11 b64 digits)
*/
static size_t
mio_encode_fixnum(mio_data_t *data, size_t index, VALUE value)
{
  long n = FIX2LONG(value);  /* never overflows */
  char *s = data->value_buf;
  char *d = &data->message_buf[index];
  int sign;

  if (n >= 0) {
    sign = 1;
  }
  else {
    sign = -1;
    n = -n;  /* always safe with fixnums since they don't use all bits */
  }

  for (;;) {
    *s++ = base64_encode_tbl[n & 0x3F];
    n >>= 6;
    if (n == 0) {
      if (sign < 0) { *s++ = '-'; }
      break;
    }
  }

  if (s - data->value_buf + index <= MIO_MSG_MAX_MESSAGE_SIZE) {
    while (s > data->value_buf) {
      *d++ = *--s;
    }
    *d = '\0';
    return d - data->message_buf;
  }
  else {
    fail_message_length(data);
  }
}


#ifdef HAVE_LONG_LONG
/*
** See mio_encode_fixnum() for a description of this representation.
**
** Examples:
**
**   >> m.encode_tuple [2**62] => "EAAAAAAAAAA"
**   >> m.encode_tuple [2**63 - 1] => "H//////////"  (9223372036854775807)
**   >> m.encode_tuple [-2**63] => "--"             (-9223372036854775808)
**
** Even on 64-bit platforms, 2**62 is the first value not representable in
** (64-bit) fixnum, and 2**63 is not representable in 'long long' period.
*/
static size_t
mio_encode_bignum(mio_data_t *data, size_t index, VALUE value)
{
  long long n = NUM2LL(value);  /* raises RangeError on overflow */
  char *s = data->value_buf;
  char *d = &data->message_buf[index];
  int sign;

  if (n >= 0) {
    sign = 1;
  }
  else {
    sign = -1;
    if (n == LLONG_MIN) { /* special case: can't take absolute value of MIN */
      if (index + 2 <= MIO_MSG_MAX_MESSAGE_SIZE) {
	strcpy(&data->message_buf[index], "--");
	return index + 2;
      }
      else {
	fail_message_length(data);
      }
    }
    else {
      n = -n;
    }
  }

  for (;;) {
    *s++ = base64_encode_tbl[n & 0x3F];
    n >>= 6;
    if (n == 0) {
      if (sign < 0) { *s++ = '-'; }
      break;
    }
  }

  if (s - data->value_buf + index <= MIO_MSG_MAX_MESSAGE_SIZE) {
    while (s > data->value_buf) {
      *d++ = *--s;
    }
    *d = '\0';
    return d - data->message_buf;
  }
  else {
    fail_message_length(data);
  }
}
#endif


/*
** This base64 encodes the raw bytes of a float (double) for both shorter
** representation and for greater fidelity (infinities, NaNs, etc.).
*/
static size_t
mio_encode_float(mio_data_t *data, size_t index, VALUE value)
{
#ifdef MIO_BIG_ENDIAN
  unsigned char *d = (unsigned char *)&RFLOAT(value)->value;
#elif defined(MIO_LITTLE_ENDIAN)
  unsigned char *dle = (unsigned char *)&RFLOAT(value)->value;
  unsigned char *dlep = dle + 7;
  unsigned char d[8], *dp = d;

  while (dlep >= dle) {
    *dp++ = *dlep--;
  }
#else
#error "MIO_BIG_ENDIAN or MIO_LITTLE_ENDIAN must be defined."
#endif

  /* Space optimization:
  **   The base64 encoding of 8 bytes produces 12 bytes, with the last
  **   byte always being '='.  We chop off this trailing byte to save
  **   space.
  **
  **   NOTE: Although we shorten the representation by dropping the
  **         trailing '=', we lengthen it by adding a leading '.', so
  **         we're back to using 12 bytes.
  **
  **         Also, base64_encode() always NUL-terminates the output, so we
  **         actually still need 13 bytes (11 bytes + '=' + NUL) available
  **         in data->message_buf during the encoding process, even though
  **         only 12 bytes are used ultimately.  Because of this, the user
  **         can't utilize the full message size in this one special case,
  **         which seems like a good tradeoff for the efficiency gained by
  **         directly encoding into message_buf rather than first into
  **         value_buf and then copying the result to message_buf.
  */
  if (index + 13 <= MIO_MSG_MAX_MESSAGE_SIZE) {/* see NOTE above for 13 != 12 */
    data->message_buf[index] = '.';
    base64_encode(d, 8, &data->message_buf[index + 1]);
    data->message_buf[index + 12] = '\0';  /* remove trailing '=' */
    return index + 12;
  }
  else {
    fail_message_length(data);
  }
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
**
** Design notes:
**
**  * The `E, `T, and `F syntax is inspired by OCaml's polymorphic variants.
**    You can think of them as an extensible set of keywords in the encoding
**    syntax.
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

    l = 0;  /* bytes to copy from value_buf to message_buf */
    v = RARRAY_PTR(tuple)[i];
    switch (TYPE(v)) {
    case T_STRING:
      if (RSTRING_LEN(v) == 0) {  /* special case: empty string */
	data->value_buf[0] = '$';
	data->value_buf[1] = '$';
	l = 2;
      }
      else if (RSTRING_LEN(v) > MIO_MSG_MAX_RAW_VALUE_SIZE) {
	sprintf(data->message_buf, "string value too long; length %ld > max "
		"length %ld", RSTRING_LEN(v), (long)MIO_MSG_MAX_RAW_VALUE_SIZE);
	rb_raise(rb_eRuntimeError, data->message_buf);
      }
      else {
	*data->value_buf = '$'; /* note: add 1 to l to adjust for this '$' */
	l = (int)base64_encode((unsigned char *)RSTRING_PTR(v),
			       RSTRING_LEN(v), data->value_buf + 1) + 1;
      }
      break;

    case T_FIXNUM:
      index = mio_encode_fixnum(data, index, v);
      break;

    case T_FLOAT:
      index = mio_encode_float(data, index, v);
      break;

    case T_NIL:
      data->value_buf[0] = '_';
      l = 1;
      break;

    case T_ARRAY:
      if (level >= MIO_MSG_MAX_NESTING) {
	sprintf(data->message_buf, "array nesting level exceeds max %d",
		MIO_MSG_MAX_NESTING);
	rb_raise(rb_eRuntimeError, data->message_buf);
      }
      else if (index + 2 <= MIO_MSG_MAX_MESSAGE_SIZE) { /* "()" at minimum */
	data->message_buf[index++] = '(';
	index = mio_encode_array(data, level + 1, index, v);
	data->value_buf[0] = ')';
	l = 1;
      }
      else {
	fail_message_length(data);
      }
      break;

    case T_TRUE:
      data->value_buf[0] = '`';
      data->value_buf[1] = 'T';
      l = 2;
      break;

    case T_FALSE:
      data->value_buf[0] = '`';
      data->value_buf[1] = 'F';
      l = 2;
      break;

#ifdef HAVE_LONG_LONG
    case T_BIGNUM:
      index = mio_encode_bignum(data, index, v);
      break;
#endif

    /* support symbol? */
    default:
      rb_raise(rb_eRuntimeError, "unsupported element type; must be nil, "
#ifdef HAVE_LONG_LONG
	       "boolean, string, fixnum, bignum (64-bits), float, or array");
#else
	       "boolean, string, fixnum, float, or array");
#endif
    }

    if (l > 0) {
      if (index + l <= MIO_MSG_MAX_MESSAGE_SIZE) {
	if (l > 2) {
	  memcpy(&data->message_buf[index], data->value_buf, l);
	  index += l;
	}
	else if (l == 1) {
	  data->message_buf[index++] = *data->value_buf;
	}
	else if (l == 2) {
	  data->message_buf[index++] = data->value_buf[0];
	  data->message_buf[index++] = data->value_buf[1];
	}

	data->message_buf[index] = '\0';
      }
      else {
	fail_message_length(data);
      }
    }
  }

  return index;
}


/*
** Returns the encoded string form of the array {tuple}, which may have
** nested arrays up to the max nesting level.
**
** {tuple} may not be nil, though it can be empty.  {tuple} may only
** contain nils, fixnums, strings, floats, or arrays.  Bignums are
** supported only if the host has the 'long long' type, and only for
** bignums that can fit within 'long long'.
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
      strcpy(data->message_buf, "`E"); /* special case: empty top-level */
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


/*
** Returns an empty string of the specified length for benchmarking purposes.
** This can be used to determine the Ruby overhead in wrapping a C function.
*/
static VALUE
mio_encode_tuple_noop(VALUE self, VALUE vlength)
{
  mio_data_t *data = NULL;
  long length = NUM2ULONG(vlength);

  Data_Get_Struct(self, mio_data_t, data);

  if (length > MIO_MSG_MAX_MESSAGE_SIZE) {
    length = MIO_MSG_MAX_MESSAGE_SIZE;
  }

  return rb_str_new(data->message_buf, length);
}


/*
** Converts an integer value to a base-64 number (NOT base-64 encoding)
** for benchmarking purposes.
*/
static VALUE
mio_benchmark_b64_encoding(VALUE self, VALUE vn, VALUE viterations)
{
  mio_data_t *data = NULL;
  long iterations = NUM2LONG(viterations);

  Data_Get_Struct(self, mio_data_t, data);

  if (NIL_P(vn)) {
    rb_raise(rb_eArgError, "numeric value argument is nil");
  }
  else if (TYPE(vn) == T_FIXNUM) {
    while (iterations > 0) {
      mio_encode_fixnum(data, 0, vn);
      --iterations;
    }
  }
#ifdef HAVE_LONG_LONG
  else if (TYPE(vn) == T_BIGNUM) {
    while (iterations > 0) {
      mio_encode_bignum(data, 0, vn);
      --iterations;
    }
  }
  else {
    rb_raise(rb_eArgError,
	     "numeric value argument must be either a fixnum or a bignum");
  }
#else
  else {
    rb_raise(rb_eArgError, "numeric value argument must be a fixnum");
  }
#endif

  return Qnil;
}


/*
** Converts an integer value to a decimal number using sprintf() for
** benchmarking purposes.
*/
static VALUE
mio_benchmark_decimal_int_encoding(VALUE self, VALUE vn, VALUE viterations)
{
  mio_data_t *data = NULL;
  long iterations = NUM2LONG(viterations);

  Data_Get_Struct(self, mio_data_t, data);

  if (NIL_P(vn)) {
    rb_raise(rb_eArgError, "numeric value argument is nil");
  }
  else if (TYPE(vn) == T_FIXNUM) {
    while (iterations > 0) {
      sprintf(data->value_buf, "%ld", FIX2LONG(vn));
      --iterations;
    }
  }
#ifdef HAVE_LONG_LONG
  else if (TYPE(vn) == T_BIGNUM) {
    while (iterations > 0) {
      sprintf(data->value_buf, "%lld", NUM2LL(vn));
      --iterations;
    }
  }
  else {
    rb_raise(rb_eArgError,
	     "numeric value argument must be either a fixnum or a bignum");
  }
#else
  else {
    rb_raise(rb_eArgError, "numeric value argument must be a fixnum");
  }
#endif

  return Qnil;
}


/*
** Converts a double value to a decimal number using sprintf() for
** benchmarking purposes.
*/
static VALUE
mio_benchmark_base64_double_encoding(VALUE self, VALUE vn, VALUE viterations)
{
  mio_data_t *data = NULL;
  long iterations = NUM2LONG(viterations);

  Data_Get_Struct(self, mio_data_t, data);

  if (NIL_P(vn)) {
    rb_raise(rb_eArgError, "numeric value argument is nil");
  }
  else if (TYPE(vn) == T_FLOAT) {
    while (iterations > 0) {
      mio_encode_float(data, 0, vn);
      --iterations;
    }
  }
  else {
    rb_raise(rb_eArgError, "numeric value argument must be a float");
  }

  return Qnil;
}


/*
** Converts a double value to a decimal number using sprintf() for
** benchmarking purposes.
*/
static VALUE
mio_benchmark_decimal_double_encoding(VALUE self, VALUE vn, VALUE viterations)
{
  mio_data_t *data = NULL;
  long iterations = NUM2LONG(viterations);

  Data_Get_Struct(self, mio_data_t, data);

  if (NIL_P(vn)) {
    rb_raise(rb_eArgError, "numeric value argument is nil");
  }
  else if (TYPE(vn) == T_FLOAT) {
    while (iterations > 0) {
      sprintf(data->value_buf, "%f", RFLOAT(vn)->value);
      --iterations;
    }
  }
  else {
    rb_raise(rb_eArgError, "numeric value argument must be a float");
  }

  return Qnil;
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
  rb_define_method(cMIO, "encode_tuple_noop", mio_encode_tuple_noop, 1);
  rb_define_method(cMIO, "benchmark_b64_encoding",
		   mio_benchmark_b64_encoding, 2);
  rb_define_method(cMIO, "benchmark_decimal_int_encoding",
		   mio_benchmark_decimal_int_encoding, 2);
  rb_define_method(cMIO, "benchmark_base64_double_encoding",
		   mio_benchmark_base64_double_encoding, 2);
  rb_define_method(cMIO, "benchmark_decimal_double_encoding",
		   mio_benchmark_decimal_double_encoding, 2);

  private_class_method_ID = rb_intern("private_class_method");
  private_ID = rb_intern("private");
  dup_ID = rb_intern("dup");
  clone_ID = rb_intern("clone");

  rb_funcall(cMIO, private_ID, 1, ID2SYM(dup_ID));
  rb_funcall(cMIO, private_ID, 1, ID2SYM(clone_ID));
}
