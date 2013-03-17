/*
 * Based on the SWIG output of Lyle Johnson's RJudy.
 */

#include <assert.h>
#include <math.h>

#include "ruby.h"
#include "compat.h"
#include "Judy.h"

static VALUE mJudy, cJudyL;

/* The size of a full Judy structure; that is, 2 ** (8 * sizeof(Word_t)). */
static VALUE max_count;

typedef struct judyl {
  Pvoid_t PJLArray;
} JudyL;


/*=========================================================================*/

static void
JudyL_mark(void *p)
{
  JudyL *data = (JudyL *)p;
  PWord_t PValue;
  Word_t Index = 0;

  JLF(PValue, data->PJLArray, Index);
  while (PValue) {
    rb_gc_mark((VALUE)*PValue);
    JLN(PValue, data->PJLArray, Index);
  }
}


static void
JudyL_free(void *p)
{
  JudyL *data = (JudyL *)p;
  if (data) {
    Word_t Rc_word;
    JLFA(Rc_word, data->PJLArray);
    xfree((void *)data);
  }
}


static VALUE
JudyL_allocate(VALUE klass)
{
  JudyL *data = ALLOC(JudyL);
  data->PJLArray = NULL;
  return Data_Wrap_Struct(klass, JudyL_mark, JudyL_free, data);
}


static VALUE
JudyL_init(VALUE self)
{
  return self;
}


/*=========================================================================*/

static VALUE
JudyL_setitem(VALUE self, VALUE index, VALUE value)
{
  JudyL *data;
  Word_t i;
  PWord_t PValue;

  Data_Get_Struct(self, JudyL, data);
  i = (Word_t)NUM2ULONG(index);
  JLI(PValue, data->PJLArray, i);
  *PValue = (Word_t)value;
  return value;
}


static VALUE
JudyL_delete_at(VALUE self, VALUE index)
{
  JudyL *data;
  Word_t i;
  PWord_t PValue;
  int Rc_int;
  VALUE v = Qnil;

  Data_Get_Struct(self, JudyL, data);
  i = (Word_t)NUM2ULONG(index);
  JLG(PValue, data->PJLArray, i);
  if (PValue) {
    v = (VALUE)*PValue;
    JLD(Rc_int, data->PJLArray, i);
  }
  return v;
}


static VALUE
JudyL_getitem(VALUE self, VALUE index)
{
  JudyL *data;
  Word_t i;
  PWord_t PValue;
  int Rc_int;
  VALUE v = Qnil;

  Data_Get_Struct(self, JudyL, data);
  i = (Word_t)NUM2ULONG(index);
  JLG(PValue, data->PJLArray, i);
  return (PValue ? (VALUE)*PValue : Qnil);
}


static VALUE
JudyL_count(int argc, VALUE *argv, VALUE self)
{
  JudyL *data;
  VALUE index1, index2;
  Word_t i1, i2, Rc_word;
  JError_t JError;

  rb_scan_args(argc, argv, "02", &index1, &index2);
  i1 = (NIL_P(index1) ? 0 : (Word_t)NUM2ULONG(index1));
  i2 = (NIL_P(index2) ? -1 : (Word_t)NUM2ULONG(index2));

  Data_Get_Struct(self, JudyL, data);
  Rc_word = JudyLCount(data->PJLArray, i1, i2, &JError);
  if (Rc_word == 0) {
    if (JU_ERRNO(&JError) == JU_ERRNO_NONE)
      return INT2FIX(0);
    else if (JU_ERRNO(&JError) == JU_ERRNO_FULL)
      return max_count;
    else assert(0);
  }
  return UINT2NUM(Rc_word);
}


#if 0
static VALUE
JudyL_count(VALUE self, VALUE index1, VALUE index2)
{
  JudyL *data;
  Word_t i1, i2;
  Word_t Rc_word;
  JError_t JError;

  Data_Get_Struct(self, JudyL, data);
  i1 = (Word_t)NUM2ULONG(index1);
  i2 = (Word_t)NUM2ULONG(index2);
  Rc_word = JudyLCount(data->PJLArray, index1, index2, &JError);
  if (Rc_word == 0) {
    if (JU_ERRNO(&JError) == JU_ERRNO_NONE)
      return INT2FIX(0);
    else if (JU_ERRNO(&JError) == JU_ERRNO_FULL)
      return max_count;
    else assert(0);
  }
  return UINT2NUM(Rc_word);
}
#endif


static VALUE
JudyL_nth_value(VALUE self, VALUE Nth)
{
  JudyL *data;
  Word_t n, i;
  PWord_t PValue;

  Data_Get_Struct(self, JudyL, data);
  n = (Word_t)NUM2ULONG(Nth);
  JLBC(PValue, data->PJLArray, n, i);
  return (PValue ? (VALUE)*PValue : Qnil);
}


static VALUE
JudyL_nth_index(VALUE self, VALUE Nth)
{
  JudyL *data;
  Word_t n, i;
  PWord_t PValue;

  Data_Get_Struct(self, JudyL, data);
  n = (Word_t)NUM2ULONG(Nth);
  JLBC(PValue, data->PJLArray, n, i);
  return (PValue ? ULONG2NUM(i) : Qnil);
}


static VALUE
JudyL_free_array(VALUE self)
{
  JudyL *data;
  Word_t Rc_word;

  Data_Get_Struct(self, JudyL, data);
  JLFA(Rc_word, data->PJLArray);
  return UINT2NUM(Rc_word);
}


static VALUE
JudyL_mem_used(VALUE self)
{
  JudyL *data;
  Word_t Rc_word;

  Data_Get_Struct(self, JudyL, data);
  JLMU(Rc_word, data->PJLArray);
  return ULONG2NUM(Rc_word);
}


static VALUE
JudyL_first_index(int argc, VALUE *argv, VALUE self)
{
  JudyL *data;
  VALUE index;
  Word_t i;
  PWord_t PValue;

  rb_scan_args(argc, argv, "01", &index);
  i = (NIL_P(index) ? 0 : (Word_t)NUM2ULONG(index));

  Data_Get_Struct(self, JudyL, data);
  JLF(PValue, data->PJLArray, i);
  return (PValue ? ULONG2NUM(i) : Qnil);
}


static VALUE
JudyL_next_index(VALUE self, VALUE index)
{
  JudyL *data;
  Word_t i;
  PWord_t PValue;

  Data_Get_Struct(self, JudyL, data);
  i = (Word_t)NUM2ULONG(index);
  JLN(PValue, data->PJLArray, i);
  return (PValue ? ULONG2NUM(i) : Qnil);
}


static VALUE
JudyL_last_index(int argc, VALUE *argv, VALUE self)
{
  JudyL *data;
  VALUE index;
  Word_t i;
  PWord_t PValue;

  rb_scan_args(argc, argv, "01", &index);
  i = (NIL_P(index) ? -1 : (Word_t)NUM2ULONG(index));

  Data_Get_Struct(self, JudyL, data);
  JLL(PValue, data->PJLArray, i);
  return (PValue ? ULONG2NUM(i) : Qnil);
}


static VALUE
JudyL_prev_index(VALUE self, VALUE index)
{
  JudyL *data;
  Word_t i;
  PWord_t PValue;

  Data_Get_Struct(self, JudyL, data);
  i = (Word_t)NUM2ULONG(index);
  JLP(PValue, data->PJLArray, i);
  return (PValue ? ULONG2NUM(i) : Qnil);
}


static VALUE
JudyL_first_empty_index(int argc, VALUE *argv, VALUE self)
{
  JudyL *data;
  VALUE index;
  Word_t i;
  int Rc_int;

  rb_scan_args(argc, argv, "01", &index);
  i = (NIL_P(index) ? 0 : (Word_t)NUM2ULONG(index));

  Data_Get_Struct(self, JudyL, data);
  JLFE(Rc_int, data->PJLArray, i);
  return (Rc_int == 1 ? ULONG2NUM(i) : Qnil);
}


static VALUE
JudyL_next_empty_index(VALUE self, VALUE index)
{
  JudyL *data;
  Word_t i;
  int Rc_int;

  Data_Get_Struct(self, JudyL, data);
  i = (Word_t)NUM2ULONG(index);
  JLNE(Rc_int, data->PJLArray, i);
  return (Rc_int == 1 ? ULONG2NUM(i) : Qnil);
}


static VALUE
JudyL_last_empty_index(int argc, VALUE *argv, VALUE self)
{
  JudyL *data;
  VALUE index;
  Word_t i;
  int Rc_int;

  rb_scan_args(argc, argv, "01", &index);
  i = (NIL_P(index) ? -1 : (Word_t)NUM2ULONG(index));

  Data_Get_Struct(self, JudyL, data);
  JLLE(Rc_int, data->PJLArray, i);
  return (Rc_int == 1 ? ULONG2NUM(i) : Qnil);
}


static VALUE
JudyL_prev_empty_index(VALUE self, VALUE index)
{
  JudyL *data;
  Word_t i;
  int Rc_int;

  Data_Get_Struct(self, JudyL, data);
  i = (Word_t)NUM2ULONG(index);
  JLPE(Rc_int, data->PJLArray, i);
  return (Rc_int == 1 ? ULONG2NUM(i) : Qnil);
}


static VALUE
JudyL_first(VALUE self)
{
  JudyL *data;
  Word_t i = 0;
  PWord_t PValue;

  Data_Get_Struct(self, JudyL, data);
  JLF(PValue, data->PJLArray, i);
  return (PValue ? (VALUE)*PValue : Qnil);
}


static VALUE
JudyL_last(VALUE self)
{
  JudyL *data;
  Word_t i = -1;
  PWord_t PValue;

  Data_Get_Struct(self, JudyL, data);
  JLL(PValue, data->PJLArray, i);
  return (PValue ? (VALUE)*PValue : Qnil);
}


static VALUE
JudyL_each(VALUE self)
{
  JudyL *data;
  Word_t i = 0;
  PWord_t PValue;

  Data_Get_Struct(self, JudyL, data);
  JLF(PValue, data->PJLArray, i);
  while (PValue) {
    rb_yield((VALUE)*PValue);
    JLN(PValue, data->PJLArray, i);
  }
  return self;
}


static VALUE
JudyL_each_index(VALUE self)
{
  JudyL *data;
  Word_t i = 0;
  PWord_t PValue;

  Data_Get_Struct(self, JudyL, data);
  JLF(PValue, data->PJLArray, i);
  while (PValue) {
    rb_yield(ULONG2NUM(i));
    JLN(PValue, data->PJLArray, i);
  }
  return self;
}


static VALUE
JudyL_each_empty_index(VALUE self)
{
  JudyL *data;
  Word_t i = 0;
  int Rc_int;

  Data_Get_Struct(self, JudyL, data);
  JLFE(Rc_int, data->PJLArray, i);
  while (Rc_int) {
    rb_yield(ULONG2NUM(i));
    JLNE(Rc_int, data->PJLArray, i);
  }
  return self;
}


static VALUE
JudyL_clear(VALUE self)
{
  JudyL *data;
  Word_t Rc_word;

  Data_Get_Struct(self, JudyL, data);
  JLFA(Rc_word, data->PJLArray);
  return self;
}


static VALUE
JudyL_emptyp(VALUE self)
{
  JudyL *data;

  Data_Get_Struct(self, JudyL, data);
  return (data->PJLArray == NULL ? Qtrue : Qfalse);
}


static VALUE
JudyL_includep(VALUE self, VALUE o)
{
  JudyL *data;
  PWord_t PValue;
  VALUE value;
  Word_t i = 0;

  Data_Get_Struct(self, JudyL, data);
  JLF(PValue, data->PJLArray, i);
  while (PValue) {
    value = (VALUE)*PValue;
    if (rb_equal(value, o) == Qtrue)
      return Qtrue;
    JLN(PValue, data->PJLArray, i);
  }
  return Qfalse;
}


static VALUE
JudyL_fullp(VALUE self)
{
  JudyL *data;
  JError_t JError;

  Data_Get_Struct(self, JudyL, data);
  return ((0 == JudyLCount(data->PJLArray, 0, -1, &JError)) &&
	  (JU_ERRNO(&JError) == JU_ERRNO_FULL) ? Qtrue : Qfalse);
}


static VALUE
JudyL_to_a(VALUE self)
{
  JudyL *data;
  VALUE ary;
  PWord_t PValue;
  Word_t i, last_i = -1;

  Data_Get_Struct(self, JudyL, data);

  ary = rb_ary_new();
  JLL(PValue, data->PJLArray, last_i);
  if (PValue) {
    for (i = 0; i <= last_i; i++) {
      JLG(PValue, data->PJLArray, i);
      rb_ary_push(ary, (PValue ? (VALUE)*PValue : Qnil));
    }
  }
  return ary;
}


static VALUE
JudyL_to_s(VALUE self)
{
  VALUE ary = JudyL_to_a(self);
  return rb_funcall(ary, rb_intern("to_s"), 0);
}


/*=========================================================================*/
/*=========================================================================*/

void
Init_judy(void)
{
  ID private_class_method_ID, private_ID;
  ID dup_ID, clone_ID, pow;
  int i;
    
  /* XXX make this a compile time error */
  assert(sizeof(Word_t) >= sizeof(VALUE));

  pow = rb_intern("**");
  max_count = rb_funcall(INT2FIX(2), pow, 1, INT2FIX(8 * sizeof(Word_t)));

  mJudy = rb_define_module("Judy");

  rb_define_const(mJudy, "WORD_SIZE", INT2FIX(8 * sizeof(Word_t)));
  rb_define_const(mJudy, "MAX_COUNT", max_count);
    
  cJudyL = rb_define_class_under(mJudy, "JudyL", rb_cObject);
  rb_include_module(cJudyL, rb_mEnumerable);
  rb_define_alloc_func(cJudyL, JudyL_allocate);

  rb_define_method(cJudyL, "initialize", JudyL_init, 0);
  rb_define_method(cJudyL, "[]=", JudyL_setitem, 2);
  rb_define_method(cJudyL, "delete_at", JudyL_delete_at, 1);
  rb_define_method(cJudyL, "[]", JudyL_getitem, 1);
  rb_define_method(cJudyL, "count", JudyL_count, -1);
  rb_define_alias(cJudyL, "size", "count");
  rb_define_alias(cJudyL, "length", "count");
  rb_define_method(cJudyL, "nth_value", JudyL_nth_value, 1);
  rb_define_method(cJudyL, "nth_index", JudyL_nth_index, 1);
  rb_define_method(cJudyL, "free_array", JudyL_free_array, 0);
  rb_define_method(cJudyL, "mem_used", JudyL_mem_used, 0);
  rb_define_method(cJudyL, "first_index", JudyL_first_index, -1);
  rb_define_method(cJudyL, "next_index", JudyL_next_index, 1);
  rb_define_method(cJudyL, "last_index", JudyL_last_index, -1);
  rb_define_method(cJudyL, "prev_index", JudyL_prev_index, 1);
  rb_define_method(cJudyL, "first_empty_index", JudyL_first_empty_index, -1);
  rb_define_method(cJudyL, "next_empty_index", JudyL_next_empty_index, 1);
  rb_define_method(cJudyL, "last_empty_index", JudyL_last_empty_index, -1);
  rb_define_method(cJudyL, "prev_empty_index", JudyL_prev_empty_index, 1);
  rb_define_method(cJudyL, "first", JudyL_first, 0);
  rb_define_method(cJudyL, "last", JudyL_last, 0);
  rb_define_method(cJudyL, "each", JudyL_each, 0);
  rb_define_method(cJudyL, "each_index", JudyL_each_index, 0);
  rb_define_method(cJudyL, "each_empty_index", JudyL_each_empty_index, 0);
  rb_define_method(cJudyL, "clear", JudyL_clear, 0);
  rb_define_method(cJudyL, "empty?", JudyL_emptyp, 0);
  rb_define_method(cJudyL, "include?", JudyL_includep, 1);
  rb_define_method(cJudyL, "full?", JudyL_fullp, 0);
  rb_define_method(cJudyL, "to_a", JudyL_to_a, 0);
  rb_define_method(cJudyL, "to_s", JudyL_to_s, 0);

  private_class_method_ID = rb_intern("private_class_method");
  private_ID = rb_intern("private");
  dup_ID = rb_intern("dup");
  clone_ID = rb_intern("clone");

  rb_funcall(cJudyL, private_ID, 1, ID2SYM(dup_ID));
  rb_funcall(cJudyL, private_ID, 1, ID2SYM(clone_ID));
}
