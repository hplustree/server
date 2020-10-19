//
// Created by kadam on 16/10/20.
//

#ifndef MYSQL_UTL_DATA_H
#define MYSQL_UTL_DATA_H


#include <iostream>
#include <dict0mem.h>
#include <data0data.h>
#include <mem0mem.h>
#include <que0que.h>
#include <row0upd.h>
#include <my_config.h>

void write_int_at(unsigned char* mem_loc, ulint integer, bool is_signed=true){
  mach_write_to_4(mem_loc, integer);

  // set the MSB in case of signed integer
  if (is_signed) {
    *mem_loc ^= 128;
  }
}

void dtuple_add_sys_cols(mem_heap_t* heap, dtuple_t* ret_data_tuple){
  byte* mem_ptr;
  dfield_t* dfield;

  /* allocate buffer to hold the needed system created hidden columns. */
  uint len = DATA_ROW_ID_LEN + DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN;
  mem_ptr = static_cast<byte*>(mem_heap_zalloc(heap, len));

  /* 1. Populate row id */
  dfield = dtuple_get_nth_field(ret_data_tuple, DATA_ROW_ID);
  dfield_set_data(dfield, mem_ptr, DATA_ROW_ID_LEN);

  row_id_t row_id = dict_sys_get_new_row_id();
  dict_sys_write_row_id(mem_ptr, row_id);

  mem_ptr += DATA_ROW_ID_LEN;

  /* 2. Populate trx id */
  dfield = dtuple_get_nth_field(ret_data_tuple, DATA_TRX_ID);
  dfield_set_data(dfield, mem_ptr, DATA_TRX_ID_LEN);

  // TODO: transaction might be needed outside in order to commit
  trx_t* trx = trx_allocate_for_background();
  trx_write_trx_id(mem_ptr, trx->id);

  mem_ptr += DATA_TRX_ID_LEN;

  /* 3. Populate roll ptr */
  dfield = dtuple_get_nth_field(ret_data_tuple, DATA_ROLL_PTR);
  dfield_set_data(dfield, mem_ptr, DATA_ROLL_PTR_LEN);

  roll_ptr_t roll_ptr = 0;
  trx_write_roll_ptr(mem_ptr, roll_ptr);

}


void dtuple_add_usr_cols(mem_heap_t* heap, unsigned int n_fields, dtuple_t* ret_data_tuple){
  // TODO: only supports integer datatype as of now

  // allocate memory from heap for user defined columns
  uint len = (n_fields - DATA_N_SYS_COLS) * SIZEOF_INT;
  byte* mem_ptr = static_cast<byte*>(mem_heap_zalloc(heap, len));

  // TODO: uses fix integer value which 10 i.e. 0a in hex
  dfield_t* dfield;
  for (unsigned n=DATA_N_SYS_COLS; n<n_fields; n++){
    dfield = dtuple_get_nth_field(ret_data_tuple, n);
    dfield_set_data(dfield, mem_ptr, SIZEOF_INT);
    write_int_at(mem_ptr, 10, !(dfield->type.prtype & DATA_UNSIGNED));
    mem_ptr += SIZEOF_INT;
  }
}

// TODO: remove the use of pointer to pointer as we are already passing heap
void create_data_tuple(dict_index_t* index, mem_heap_t* heap, dtuple_t** ret_data_tuple){
  // TODO: only supports DATA_INT as of now

  *ret_data_tuple = dtuple_create(heap, index->n_fields);;

  (*ret_data_tuple)->n_fields_cmp = index->n_uniq;

  (*ret_data_tuple)->tuple_list.prev = nullptr;
  (*ret_data_tuple)->tuple_list.next = nullptr;

  // for reference:

  // row id:
  //  data bytes
  //  ext 0
  //  len 6
  //  type
  //    prtype 256
  //    mtype 8
  //    len 6
  //    mbminlen 0
  //    mbmaxlen 0

  // trx id:
  //  data bytes
  //  ext 0
  //  len 6
  //  type
  //    prtype 257
  //    mtype 8
  //    len 6
  //    mbminlen 0
  //    mbmaxlen 0

  // roll ptr:
  //  data bytes
  //  ext 0
  //  len 7
  //  type
  //    prtype 258
  //    mtype 8
  //    len 7
  //    mbminlen 0
  //    mbmaxlen 0

  // col 1:
  //  data bytes
  //  ext 0
  //  len 4
  //  type
  //    prtype 0
  //    mtype 6
  //    len 4
  //    mbminlen 0
  //    mbmaxlen 0


  dict_index_copy_types(*ret_data_tuple, index, index->n_fields);

  dtuple_add_sys_cols(heap, *ret_data_tuple);
  dtuple_add_usr_cols(heap, index->n_fields, *ret_data_tuple);
}

#endif // MYSQL_UTL_DATA_H
