/*
** Ruby bindings to the Marinda messaging routines.
**
** Lexical syntax:
**
**   <empty> ::= "`E"  # empty top-level tuple
**   <nil> ::= "_"     # nil value
**   <true> ::= "`T"
**   <false> ::= "`F"
**
**   <base64-char> ::= [A-Za-z0-9+/=]
**
**   <b64-int> ::= <base64-char>+
**               | "-" <base64-char>+
**               | "--"   # long long min: -2**63
**
**   <float> ::= "." <base64-char>{11}  # no trailing "="
**      
**   <string> ::= "$" <base64-char>+
**              | "$$"  # empty string
**
** Syntax:
**
**   <top-level> ::= <empty> | <tuple>
**
**   <tuple> ::= <tuple-element> ("," <tuple-element>)*
**
**   <tuple-element> ::= <nil> | <true> | <false> | <b64-int> | <float>
**                     | <string> | "()" | "(" <tuple> ")"
**
** --------------------------------------------------------------------------
** Author: Young Hyun
** Copyright (C) 2009, 2010 Young Hyun
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
#include <ctype.h>
#include <assert.h>

#include "ruby.h"
#include "compat.h"

#include "mio_keywords.h"
#include "mio_msg.h"
#include "mio_base64.h"
#include "mio.h"

VALUE rb_float_new(double d);  /* defined in numeric.c */

/*
** The maximum number of b64 digits that can be represented in a fixnum.
**
** For 32-bit fixnums, the maximum fixnum is 2**30 - 1, or 5 b64 digits
** (at 6 bits each).  For 64-bit fixnums, the maximum fixnum is 2**62 - 1,
** or 10 b64 digits.
*/
#define MIO_MAX_FIXNUM_B64_LEN 5

#define MIO_MAX_64BIT_FIXNUM_B64_LEN 10
#define MIO_MAX_64BIT_B64_LEN 11 /* max b64 digits that _may_ fit in 64 bits */

/* XXX can use a smaller value_buf to save space */
typedef struct {
  char message_buf[MIO_MSG_MAX_MESSAGE_SIZE + 1];  /* + NUL-term */
  char value_buf[MIO_MSG_MAX_MESSAGE_SIZE + 1];  /* + NUL-term */
  const char *decode_source;  /* user-provided string to decode */
  const char *element_start;  /* start of current element being decoded */
} mio_data_t;

static mio_data_t g_data;  /* used by class encode/decode methods */

static const char base64_encode_tbl[64] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/*
** A base64-decoding table for mapping an encoded 'digit' into the 6-bit
** value it represents.  A non-base64 character maps to 64 (this works for
** the full unsigned 8-bit range, including NUL).
**
** NOTE: A '=' is not a valid base64 digit.
*/
static const unsigned char base64_decode_tbl[256] =
{
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
  52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
  64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
  15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
  64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
  41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
};

static VALUE cMIO;
static VALUE eEncodeError, eEncodeLimitExceeded;
static VALUE eDecodeError, eDecodeLimitExceeded, eParseError;

static const char *
decode_array(mio_data_t *data, int level, const char *s, VALUE array);

NORETURN(static void fail_message_length());

/*=========================================================================*/

static void
fail_message_length()
{
  rb_raise(eEncodeLimitExceeded, "message length exceeds max %d",
	   MIO_MSG_MAX_MESSAGE_SIZE);
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


static VALUE
mio_s_noop(VALUE self, VALUE vlength)
{
  return self;
}


static VALUE
mio_noop(VALUE self, VALUE vlength)
{
  return self;
}


/*=========================================================================*/
/* ENCODING                                                                */
/*=========================================================================*/

/*
** This represents a fixnum with a base-64 number (NOT base-64 encoding)
** instead of with a decimal number.  Like all textual representations,
** the most-significant digit comes first, and a leading minus sign is
** used to represent a negative number.
**
** Examples:
**
**   >> m.encode [1] => "B"
**   >> m.encode [-1] => "-B"
**
**   On 64-bit platforms:
**
**   >> m.encode [2**32] => "EAAAAA"
**   >> m.encode [-2**32] => "-EAAAAA"          # 'A' == all bits clear
**   >> m.encode [2**62 - 1] => "D//////////"   # '/' == all bits set
**   >> m.encode [-2**62] => "-EAAAAAAAAAA"
**
**      -2**32 == -4294967296 (10 decimal digits vs. 6 base64 digits)
**      -2**62 == -4611686018427387904 (19 decimal digits vs. 11 b64 digits)
*/
static size_t
encode_fixnum(mio_data_t *data, size_t index, VALUE value)
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
    fail_message_length();
  }
}


#ifdef HAVE_LONG_LONG
/*
** See encode_fixnum() for a description of this representation.
**
** Examples:
**
**   >> m.encode [2**62] => "EAAAAAAAAAA"
**   >> m.encode [2**63 - 1] => "H//////////"  (9223372036854775807)
**   >> m.encode [-2**63] => "--"             (-9223372036854775808)
**
** Even on 64-bit platforms, 2**62 is the first value not representable in
** (64-bit) fixnum, and 2**63 is not representable in 'long long' period.
*/
static size_t
encode_bignum(mio_data_t *data, size_t index, VALUE value)
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
	fail_message_length();
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
    fail_message_length();
  }
}
#endif


/*
** This base64 encodes the raw bytes of a float (double) for both shorter
** representation and for greater fidelity (infinities, NaNs, etc.).
*/
static size_t
encode_float(mio_data_t *data, size_t index, VALUE value)
{
#ifdef MIO_BIG_ENDIAN
  const unsigned char *d = (unsigned char *)&(RFLOAT_VALUE(value));
#elif defined(MIO_LITTLE_ENDIAN)
  const unsigned char *dle = (unsigned char *)&(RFLOAT_VALUE(value));
  const unsigned char *dlep = dle + 7;
  unsigned char d[8], *dp = d;

  *dp++ = *dlep--; *dp++ = *dlep--; *dp++ = *dlep--; *dp++ = *dlep--;
  *dp++ = *dlep--; *dp++ = *dlep--; *dp++ = *dlep--; *dp++ = *dlep--;
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
    fail_message_length();
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
encode_array(mio_data_t *data, int level, size_t index, VALUE tuple)
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
	rb_raise(eEncodeLimitExceeded, "string value too long; length %ld "
		 "> max length %ld", RSTRING_LEN(v),
		 (long)MIO_MSG_MAX_RAW_VALUE_SIZE);
      }
      else {
	*data->value_buf = '$'; /* note: add 1 to l to adjust for this '$' */
	l = (int)base64_encode((unsigned char *)RSTRING_PTR(v),
			       RSTRING_LEN(v), data->value_buf + 1) + 1;
      }
      break;

    case T_FIXNUM:
      index = encode_fixnum(data, index, v);
      break;

    case T_FLOAT:
      index = encode_float(data, index, v);
      break;

    case T_NIL:
      data->value_buf[0] = '_';
      l = 1;
      break;

    case T_ARRAY:
      if (level < MIO_MSG_MAX_NESTING) {
	if (index + 2 <= MIO_MSG_MAX_MESSAGE_SIZE) { /* "()" at minimum */
	  data->message_buf[index++] = '(';
	  index = encode_array(data, level + 1, index, v);
	  data->value_buf[0] = ')';
	  l = 1;
	}
	else {
	  fail_message_length();
	}
      }
      else {
	rb_raise(eEncodeLimitExceeded, "array nesting level exceeds max %d",
		 MIO_MSG_MAX_NESTING);
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
      index = encode_bignum(data, index, v);
      break;
#endif

    /* support symbol? */
    default:
      rb_raise(rb_eTypeError, "unsupported element type; must be nil, "
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
	fail_message_length();
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
mio_encode(VALUE self, VALUE tuple)
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
      encode_array(data, 0, 0, tuple);
    }
    return rb_str_new2(data->message_buf);
  }
  else {
    rb_raise(rb_eTypeError, "tuple argument must be an array");
  }

  return Qnil;  /*NOTREACHED*/
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
mio_s_encode(VALUE self, VALUE tuple)
{
  if (NIL_P(tuple)) {
    rb_raise(rb_eArgError, "tuple argument is nil");
  }
  else if (TYPE(tuple) == T_ARRAY) {
    if (RARRAY_LEN(tuple) == 0) {
      strcpy(g_data.message_buf, "`E"); /* special case: empty top-level */
    }
    else {
      encode_array(&g_data, 0, 0, tuple);
    }
    return rb_str_new2(g_data.message_buf);
  }
  else {
    rb_raise(rb_eTypeError, "tuple argument must be an array");
  }

  return Qnil;  /*NOTREACHED*/
}


/*
** Returns an empty string of the specified length for benchmarking purposes.
** This can be used to determine the Ruby overhead in wrapping a C function.
*/
static VALUE
mio_encode_noop(VALUE self, VALUE vlength)
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
** Returns an empty string of the specified length for benchmarking purposes.
** This can be used to determine the Ruby overhead in wrapping a C function.
*/
static VALUE
mio_s_encode_noop(VALUE self, VALUE vlength)
{
  long length = NUM2ULONG(vlength);

  if (length > MIO_MSG_MAX_MESSAGE_SIZE) {
    length = MIO_MSG_MAX_MESSAGE_SIZE;
  }

  return rb_str_new(g_data.message_buf, length);
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
      encode_fixnum(data, 0, vn);
      --iterations;
    }
  }
#ifdef HAVE_LONG_LONG
  else if (TYPE(vn) == T_BIGNUM) {
    while (iterations > 0) {
      encode_bignum(data, 0, vn);
      --iterations;
    }
  }
  else {
    rb_raise(rb_eTypeError,
	     "numeric value argument must be either a fixnum or a bignum");
  }
#else
  else {
    rb_raise(rb_eTypeError, "numeric value argument must be a fixnum");
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
    rb_raise(rb_eTypeError,
	     "numeric value argument must be either a fixnum or a bignum");
  }
#else
  else {
    rb_raise(rb_eTypeError, "numeric value argument must be a fixnum");
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
      encode_float(data, 0, vn);
      --iterations;
    }
  }
  else {
    rb_raise(rb_eTypeError, "numeric value argument must be a float");
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
      sprintf(data->value_buf, "%f", RFLOAT_VALUE(vn));
      --iterations;
    }
  }
  else {
    rb_raise(rb_eTypeError, "numeric value argument must be a float");
  }

  return Qnil;
}


/*=========================================================================*/
/* DECODING                                                                */
/*=========================================================================*/

/* Generate cases for matching the first <base64-char> (no '='):

require 'enumerator'
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".unpack("C*").each_slice(7) do |x| printf " "; x.each do |c| printf " case '%s':", c.chr end; puts; end

 */


#if 0
      else if (b64_len < MIO_MAX_64BIT_B64_LEN) {
	/* {n} now has 60 bits of data (== 10 b64 digits), and thus {n} is
	   <= 2**60 - 1.  If we shift in 6 more bits, {n} will be <= 2**66 - 1,
	   which exceeds the maximum 64-bit fixnum 2**62 - 1 (or 62 bits of
           data).  Hence, the upper 4 bits of {n} must be zero if {n} is to
	   fit; that is, {n} must be <= 2**(60 - 4) - 1 < 2**56 == 1LL << 56.
	   However, */
	if (n < (1LL << 56) || (sign == -1 && n == (1LL << 56))) {
	  ++b64_len;
	  n <<= 6;
	  n |= d;
	  ++s;
	}
	else {
	  rb_raise(eDecodeLimitExceeded, "<b64-int> at pos %d is too large "
	   "for 64 bits", (int)(data->element_start - data->decode_source));
	}
      }
#endif


/*
** Decodes a b64 integer that is too large to fit in a fixnum.
**
** This should be called as needed from decode_b64_int().  {n} should be
** the value decoded so far, and {b64_len} should be the number of b64
** digits decoded so far.
*/
static const char *
decode_b64_bignum(mio_data_t *data, const char *s, VALUE array, int sign,
		 long long n, int b64_len)
{
  int d;            /* numeric value of the current b64 digit */

  while (*s && *s != ',' && *s != ')') {
    d = base64_decode_tbl[(int)*s];
    if (d < 64) {
      if (b64_len < MIO_MAX_64BIT_FIXNUM_B64_LEN) {
	++b64_len;
	n <<= 6;
	n |= d;
	++s;
      }
      else if (b64_len < MIO_MAX_64BIT_B64_LEN) {
	/* {n} now has 60 bits of data (== 10 b64 digits), and thus {n} is
	   <= 2**60 - 1.  If we shift in 6 more bits, {n} will be <= 2**66 - 1,
	   which exceeds the maximum 64-bit integer 2**63 - 1 (or 63 bits of
           data).  Hence, the upper 3 bits of {n} must be zero if {n} is to
	   fit; that is, {n} must be <= 2**(60 - 3) - 1 < 2**57 == 1LL << 57.
	   Note, however, that negative numbers can be -2**63. */
	if (n < (1LL << 57) || (sign == -1 && n == (1LL << 57))) {
	  ++b64_len;
	  n <<= 6;
	  n |= d;
	  ++s;
	}
	else {
	  rb_raise(eDecodeLimitExceeded, "<b64-int> at pos %d is too large "
	   "for 64 bits", (int)(data->element_start - data->decode_source));
	}
      }
      else {
	/* Although it's theoretically possible for a long <b64-int> to fit
	   within 64 bits (imagine a bunch of zeros), we don't allow that
	   possibility here, since the <b64-int> encoder never uses zero
	   padding.  */
	rb_raise(eDecodeLimitExceeded, "<b64-int> at pos %d is too large "
	   "for 64 bits", (int)(data->element_start - data->decode_source));
      }
    }
    else {
      if (b64_len > 0) {
	rb_raise(eParseError, "illegal character in <b64-int> at pos %d",
		 (int)(s - data->decode_source));
      }
      else {
	rb_raise(eParseError, "illegal character at the start of <tuple-"
		 "element> at pos %d", (int)(s - data->decode_source));

      }
    }
  }

  if (b64_len > 0) {
    if (sign == -1) { n = -n; }
    rb_ary_push(array, LL2NUM(n));
    return s;
  }
  else {
    /* By this point, all other error cases have been checked in
       decode_element() before this function was called; namely,
       truncated <b64-int> and missing <tuple-element>. */
    rb_bug("MIO INTERNAL ERROR: decode_b64_bignum: b64_len == 0 and *s == %d",
	   (int)*s);
  }
}


/*
** Decodes a b64 integer, switching automatically to decoding a bignum
** if necessary.
**
** For efficiency, we call this function optimistically in decode_element()
** if the lookahead doesn't match any other lexical element.  Hence, {s}
** may not be a well-formed <b64-int>, so we should give helpful error
** messages.
**
** Note:
**   1. {s} should not be the special case "--".
**   2. {s} should not start with a minus sign; the caller should have already
**      decoded the minus sign and indicated this by passing -1 for {sign}.
*/
static const char *
decode_b64_int(mio_data_t *data, const char *s, VALUE array, int sign)
{
  long n = 0;       /* value of the <b64-int> decoded so far */
  int b64_len = 0;  /* number of b64 digits decoded so far */
  int d;            /* numeric value of the current b64 digit */

  while (*s && *s != ',' && *s != ')') {
    d = base64_decode_tbl[(int)*s];
    if (d < 64) {
      if (b64_len < MIO_MAX_FIXNUM_B64_LEN) {
	++b64_len;
	n <<= 6;
	n |= d;
	++s;
      }
      else {
	return decode_b64_bignum(data, s, array, sign, n, b64_len);
      }
    }
    else {
      if (b64_len > 0) {
	rb_raise(eParseError, "illegal character in <b64-int> at pos %d",
		 (int)(s - data->decode_source));
      }
      else {
	rb_raise(eParseError, "illegal character at the start of <tuple-"
		 "element> at pos %d", (int)(s - data->decode_source));

      }
    }
  }

  if (b64_len > 0) {
    if (sign == -1) { n = -n; }
    rb_ary_push(array, LONG2FIX(n));
    return s;
  }
  else {
    /* By this point, all other error cases have been checked in
       decode_element() before this function was called; namely,
       truncated <b64-int> and missing <tuple-element>. */
    rb_bug("MIO INTERNAL ERROR: decode_b64_int: b64_len == 0 and *s == %d",
	   (int)*s);
  }
}


/*
** See encode_float() for a description of the space optimization.
*/
static const char *
decode_float(mio_data_t *data, const char *s, VALUE array)
{
  const char *s0 = s;
  char *d = data->message_buf;
  char *dend = &data->message_buf[11];  /* not 12 -- see encode_float() */
  size_t decode_len;

  /* assert(*s != '\0' && *s != ',' && *s != ')'); */
  while (*s && *s != ',' && *s != ')') {
    if (d < dend) {
      *d++ = *s++;
    }
    else {
      rb_raise(eDecodeLimitExceeded, "<float> at pos %d is too long",
	       (int)(s0 - data->decode_source));
    }
  }

  if (d == dend) {  /* input must be exactly 11 base64 characters */
    *d++ = '=';  /* restore '=' dropped in encode_float() */
    *d = '\0';
    decode_len = base64_decode(data->message_buf,
			       (unsigned char *)data->value_buf);
    if (decode_len > 0) {
      double *d = (double *)data->value_buf;
#ifdef MIO_LITTLE_ENDIAN
      unsigned char *dp = (unsigned char *)d;
      unsigned char *dep = dp + 7;
      unsigned char t;

      t = *dp; *dp++ = *dep; *dep-- = t; t = *dp; *dp++ = *dep; *dep-- = t;
      t = *dp; *dp++ = *dep; *dep-- = t; t = *dp; *dp++ = *dep; *dep-- = t;
#elif !defined(MIO_BIG_ENDIAN)
#error "MIO_BIG_ENDIAN or MIO_LITTLE_ENDIAN must be defined."
#endif
      rb_ary_push(array, rb_float_new(*d));
      return s;
    }
    else {
      rb_raise(eParseError, "malformed <float> at pos %d",
	       (int)(s0 - data->decode_source));
    }
  }
  else {
    rb_raise(eDecodeLimitExceeded, "<float> at pos %d is too short",
	     (int)(s0 - data->decode_source));
  }
}


/*
** First copy the base64 string to {message_buf}, NUL-terminate it, and
** then decode to {value_buf}.  These buffers should be exactly the right
** size for each role.
**
** If it weren't for the need to NUL-terminate the base64 string, we could
** skip the copying to {message_buf}.
*/
static const char *
#if 0
decode_string(mio_data_t *data, const char *s, VALUE array)
{
  const char *s0 = s;
  char *d = data->message_buf;
  char *dend = &data->message_buf[MIO_MSG_MAX_ENCODED_VALUE_SIZE];
  size_t decode_len;

 /* assert(*s != '\0' && *s != ',' && *s != ')'); */
  while (*s && *s != ',' && *s != ')') {
    if (d < dend) {
      *d++ = *s++;
    }
    else {
      rb_raise(eDecodeLimitExceeded, "<string> at pos %d is too long "
	       "for decoding", (int)(s0 - data->decode_source));
    }
  }
  *d = '\0';

  decode_len = base64_decode(data->message_buf,
			     (unsigned char *)data->value_buf);
  if (decode_len > 0) {
    rb_ary_push(array, rb_str_new(data->value_buf, (long)decode_len));
    return s;
  }
  else {
    rb_raise(eParseError, "malformed <string> at pos %d",
	     (int)(s0 - data->decode_source));
  }
}
#elif 0
decode_string(mio_data_t *data, const char *s0, VALUE array)
{
  char *s = (char *)s0;
  char *send = s + MIO_MSG_MAX_ENCODED_VALUE_SIZE;
  size_t decode_len;
  char c;

  /* assert(*s != '\0' && *s != ',' && *s != ')'); */
  while (*s && *s != ',' && *s != ')') {
    if (s < send) {
      ++s;
    }
    else {
      rb_raise(eDecodeLimitExceeded, "<string> at pos %d is too long "
	       "for decoding", (int)(s0 - data->decode_source));
    }
  }
  c = *s;
  *s = '\0';

  decode_len = base64_decode(s0, (unsigned char *)data->value_buf);
  *s = c;
  if (decode_len > 0) {
    rb_ary_push(array, rb_str_new(data->value_buf, (long)decode_len));
    return s;
  }
  else {
    rb_raise(eParseError, "malformed <string> at pos %d",
	     (int)(s0 - data->decode_source));
  }
}
#elif 0
decode_string(mio_data_t *data, const char *s0, VALUE array)
{
  char *s = (char *)s0 + 26668;
  size_t decode_len;
  char c;

  c = *s;
  *s = '\0';

  decode_len = base64_decode(s0, (unsigned char *)data->value_buf);
  *s = c;
  if (decode_len > 0) {
    rb_ary_push(array, rb_str_new(data->value_buf, (long)decode_len));
    return s;
  }
  else {
    rb_raise(eParseError, "malformed <string> at pos %d",
	     (int)(s0 - data->decode_source));
  }
}
#elif 1
decode_string(mio_data_t *data, const char *s0, VALUE array)
{
  size_t decode_len;
  const char *s;

  decode_len = base64_decode2(s0, (unsigned char *)data->value_buf, &s);
  if (decode_len > 0) {
    rb_ary_push(array, rb_str_new(data->value_buf, (long)decode_len));
    return s;
  }
  else {
    rb_raise(eParseError, "malformed <string> at pos %d",
	     (int)(s0 - data->decode_source));
  }
}
#endif


static const char *
decode_element(mio_data_t *data, int level, const char *s, VALUE array)
{
  data->element_start = s;
  switch (*s) {
  case '\0':
    rb_raise(eParseError, "missing <tuple-element> at pos %d",
	     (int)(s - data->decode_source));

  case '_':
    rb_ary_push(array, Qnil);
    ++s;
    break;

  case '`':  /* `T  `F; but `E should not occur here  */
    ++s;
    if (*s == 'T') {
      rb_ary_push(array, Qtrue);
    }
    else if (*s == 'F') {
      rb_ary_push(array, Qfalse);
    }
    else { /* if (*s == '\0' || *s == ',' || *s == ')' || any other char) */
      rb_raise(eParseError, "truncated <keyword-element> at pos %d",
	       (int)(data->element_start - data->decode_source));
    }
    ++s;
    break;

  case '-':
    ++s;
    if (*s == '\0' || *s == ',' || *s == ')') {
      rb_raise(eParseError, "truncated <b64-int> at pos %d",
	       (int)(data->element_start - data->decode_source));
    }
    else if (*s == '-') {
      rb_ary_push(array, LL2NUM(LLONG_MIN));
      ++s;
    }
    else {
      s = decode_b64_int(data, s, array, -1);
    }
    break;

  case '.':
    ++s;
    if (*s == '\0' || *s == ',' || *s == ')') {
      rb_raise(eParseError, "truncated <float> at pos %d",
	       (int)(data->element_start - data->decode_source));
    }
    else {
      s = decode_float(data, s, array);
    }
    break;

  case '$':
    ++s;
    if (*s == '\0' || *s == ',' || *s == ')') {
      rb_raise(eParseError, "truncated <string> at pos %d",
	       (int)(data->element_start - data->decode_source));
    }
    else if (*s == '$') {
      rb_ary_push(array, rb_str_new2(""));
      ++s;
    }
    else {
      s = decode_string(data, s, array);
    }
    break;

  case '(':
    if (level < MIO_MSG_MAX_NESTING) {
      VALUE subarray = rb_ary_new();
      s = decode_array(data, level + 1, s + 1, subarray);
      if (*s == ')') {
	++s;
	rb_ary_push(array, subarray);
      }
      else {
	rb_raise(eParseError, "syntax error at pos %d; expected ')'",
		 (int)(s - data->decode_source));
      }
    }
    else {
      rb_raise(eDecodeLimitExceeded, "array nesting level exceeds max %d",
	       MIO_MSG_MAX_NESTING);
    }
    break;

  case ',': case ')':
    rb_bug("MIO INTERNAL ERROR: decode_element: unexpected ',' or ')'");

  default:
    /* For efficiency, try decoding as <b64-int> without checking first. */
    s = decode_b64_int(data, s, array, 1);
    break;
  }

  return s;
}


/*
** {level} is the nesting level, where 0 indicates the top-level array,
** 1 indicates an array element in the top level array, etc.
**
** {array} should be the new Array object to populate with the elements
** at this new level of nesting; that is, the *caller* should allocate the
** Array object to use and pass it in.
**
** Returns a pointer to the closing ')' (or '\0' for the top-level) in the
** normal case.
*/
static const char *
decode_array(mio_data_t *data, int level, const char *s, VALUE array)
{
  if (*s == ')') { /* empty subarray--that is, "()"; nothing to do */
    if (level > 0) {
      return s;  /* don't consume ')'; let caller deal with it */
    }
    else {
      rb_raise(eParseError, "syntax error at pos %d; extra ')'",
	       (int)(s - data->decode_source));
    }
  }

  s = decode_element(data, level, s, array);
  for (;;) {
    if (*s) {
      if (*s == ',') {
	s = decode_element(data, level, s + 1, array);
      }
      else if (*s == ')') {
	if (level > 0) {
	  return s;  /* don't consume ')'; let caller deal with it */
	}
	else {
	  rb_raise(eParseError, "syntax error at pos %d; extra ')'",
		   (int)(s - data->decode_source));
	}
      }
      else {
	rb_raise(eParseError, "syntax error at pos %d; expected ',', ')', "
		 "or end of string", (int)(s - data->decode_source));
      }
    }
    else {
      return s;
    }
  }
}


static VALUE
mio_decode(VALUE self, VALUE v)
{
  mio_data_t *data = NULL;
  const char *s;

  Data_Get_Struct(self, mio_data_t, data);

  StringValue(v);
  data->decode_source = s = RSTRING_PTR(v);

  if (*s == '`' && strcmp(s, "`E") == 0) {
    return rb_ary_new();
  }
  else {
    VALUE retval = rb_ary_new();
    decode_array(data, 0, s, retval);
    return retval;
  }
}


static VALUE
mio_s_decode(VALUE self, VALUE v)
{
  const char *s;

  StringValue(v);
  g_data.decode_source = s = RSTRING_PTR(v);

  if (*s == '`' && strcmp(s, "`E") == 0) {
    return rb_ary_new();
  }
  else {
    VALUE retval = rb_ary_new();
    decode_array(&g_data, 0, s, retval);
    return retval;
  }
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

  /* Class method invocation is 18% slower with MIO nested inside Marinda. */
#if 0
  mMarinda = rb_define_module("Marinda");
  cMIO = rb_define_class_under(mMarinda, "MIO", rb_cObject);
#else
  cMIO = rb_define_class("MIO", rb_cObject);
#endif

  eEncodeError = rb_define_class_under(cMIO, "EncodeError", rb_eStandardError);
  eEncodeLimitExceeded = rb_define_class_under(cMIO, "EncodeLimitExceeded",
					       eEncodeError);

  eDecodeError = rb_define_class_under(cMIO, "DecodeError", rb_eStandardError);
  eDecodeLimitExceeded = rb_define_class_under(cMIO, "DecodeLimitExceeded",
					       eDecodeError);
  eParseError = rb_define_class_under(cMIO, "ParseError", eDecodeError);

  DEF_LIMIT(MAX_RAW_VALUE_SIZE);
  DEF_LIMIT(MAX_ENCODED_VALUE_SIZE);
  DEF_LIMIT(MAX_MESSAGE_SIZE);
  DEF_LIMIT(MAX_NESTING);

  rb_define_alloc_func(cMIO, mio_alloc);
  rb_define_singleton_method(cMIO, "encode", mio_s_encode, 1);
  rb_define_singleton_method(cMIO, "encode_noop", mio_s_encode_noop, 1);
  rb_define_singleton_method(cMIO, "decode", mio_s_decode, 1);
  rb_define_singleton_method(cMIO, "noop", mio_s_noop, 1);

  rb_define_method(cMIO, "initialize", mio_init, 0);
  rb_define_method(cMIO, "encode", mio_encode, 1);
  rb_define_method(cMIO, "encode_noop", mio_encode_noop, 1);
  rb_define_method(cMIO, "benchmark_b64_encoding",
		   mio_benchmark_b64_encoding, 2);
  rb_define_method(cMIO, "benchmark_decimal_int_encoding",
		   mio_benchmark_decimal_int_encoding, 2);
  rb_define_method(cMIO, "benchmark_base64_double_encoding",
		   mio_benchmark_base64_double_encoding, 2);
  rb_define_method(cMIO, "benchmark_decimal_double_encoding",
		   mio_benchmark_decimal_double_encoding, 2);
  rb_define_method(cMIO, "decode", mio_decode, 1);
  rb_define_method(cMIO, "noop", mio_noop, 1);

  private_class_method_ID = rb_intern("private_class_method");
  private_ID = rb_intern("private");
  dup_ID = rb_intern("dup");
  clone_ID = rb_intern("clone");

  rb_funcall(cMIO, private_ID, 1, ID2SYM(dup_ID));
  rb_funcall(cMIO, private_ID, 1, ID2SYM(clone_ID));
}
