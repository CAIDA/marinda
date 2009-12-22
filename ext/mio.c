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
#include "mio_msg_reader.h"
#include "mio_msg_writer.h"

#include "mio.h"

typedef struct {
  void *buffer;
} mio_data_t;

static VALUE cMIO;

static VALUE create_error_message(const char *msg_start, const char *msg_end);

/*=========================================================================*/

static void
mio_free(void *data)
{
  mio_data_t *mio_data = (mio_data_t *)data;

  if (mio_data) {

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
mio_encode_tuple(VALUE self, VALUE tuple)
{
  mio_data_t *data = NULL;

  Data_Get_Struct(self, mio_data_t, data);

  return Qnil;
}


static VALUE
create_error_message(const char *msg_start, const char *msg_end)
{
  size_t len = strlen(msg_start) + 2 + strlen(msg_end);
  char *buf;
  VALUE retval;

  buf = ALLOC_N(char, len + 1);
  strcpy(buf, msg_start);
  strcat(buf, ": ");
  strcat(buf, msg_end);

  retval = rb_str_new2(buf);
  free(buf);

  return retval;
}


/***************************************************************************/
/***************************************************************************/

#define IV_INTERN(name) iv_##name = rb_intern("@" #name)
#define METH_INTERN(name) meth_##name = rb_intern(#name)

void
Init_mio(void)
{
  ID private_class_method_ID, private_ID;
  ID /*new_ID,*/ dup_ID, clone_ID;

  /* XXX make MIO a singleton */
  /* XXX fix message creation/parsing routines to not use static buffers */

  cMIO = rb_define_class("MIO", rb_cObject);

  rb_define_alloc_func(cMIO, mio_alloc);

  rb_define_method(cMIO, "initialize", mio_init, 0);
  rb_define_method(cMIO, "encode_tuple", mio_encode_tuple, 1);

  private_class_method_ID = rb_intern("private_class_method");
  private_ID = rb_intern("private");
  /* new_ID = rb_intern("new"); */
  dup_ID = rb_intern("dup");
  clone_ID = rb_intern("clone");

  /* rb_funcall(cMIO, private_class_method_ID, 1, ID2SYM(new_ID)); */
  rb_funcall(cMIO, private_ID, 1, ID2SYM(dup_ID));
  rb_funcall(cMIO, private_ID, 1, ID2SYM(clone_ID));
}
