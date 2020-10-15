//
// Created by kadam on 07/10/20.
//

#ifndef MYSQL_UTL_TABLE_H
#define MYSQL_UTL_TABLE_H

#include <srv0srv.h>
#include <dict0boot.h>
#include <fil0fil.h>
#include <fsp0fsp.h>

bool create_user_tablespace(dict_table_t* table){
  // We create a new single-table tablespace for the table.
  // We initially let it be 4 pages:
  // - page 0 is the fsp header and an extent descriptor page,
  // - page 1 is an ibuf bitmap page,
  // - page 2 is the first inode page,
  // - page 3 will contain the root of the clustered index of the table we create here.

  // char* path = table->data_dir_path ? table->data_dir_path
  //                             : table->dir_path_of_temp_table;
  char* path = nullptr;

  dberr_t code = fil_create_new_single_table_tablespace(
      table->space, table->name, path,
      dict_tf_to_fsp_flags(table->flags), table->flags2,
      FIL_IBD_FILE_INITIAL_SIZE,
      FIL_ENCRYPTION_OFF, ENCRYPTION_KEY_SYSTEM_DATA);

  if(code != dberr_t::DB_SUCCESS){
    return false;
  }

  // initialize the file space header
  mtr_t mtr;
  mtr_start(&mtr);
  fsp_header_init(table->space, FIL_IBD_FILE_INITIAL_SIZE, &mtr);
  mtr_commit(&mtr);

  return true;
}

void delete_tablespace_ibd_file(char* table_name){
  char file_name[100];
  snprintf(file_name, sizeof(file_name), "%s%s.ibd", srv_data_home, table_name);
  remove(file_name);
}


bool create_table(char* table_name, dict_table_t* ret_table, ulint n_cols = 2){
  ulint flags = DICT_TF_COMPACT;
  ulint flags2 = DICT_TF2_USE_TABLESPACE | DICT_TF2_FTS_AUX_HEX_NAME;


  // Get a new space id.
  ulint space_id;
  dict_hdr_get_new_id(nullptr, nullptr, &space_id);

  // create table object
  *ret_table = *dict_mem_table_create(table_name, space_id, n_cols, flags, flags2);

  char col_name[10];
  /** refer data0type.h */
  ulint prtype = 0; // no restrictions on column
  ulint type = DATA_INT; // set column datatype as integer
  ulint data_len = 4; // 4 bytes integer

  mem_heap_t *heap = mem_heap_create(450);

  for (unsigned int i=0; i<n_cols; ++i){
    snprintf(col_name, sizeof(col_name), "col_%u", i);
    dict_mem_table_add_col(ret_table, heap, col_name, type, prtype, data_len);
  }

  dict_table_add_system_columns(ret_table, heap);

  mem_heap_free(heap);

  // Get a new table id.
  dict_hdr_get_new_id(&ret_table->id, NULL, NULL);

  UT_LIST_INIT(ret_table->indexes);

  return create_user_tablespace(ret_table);
}

#endif // MYSQL_UTL_TABLE_H
