//
// Created by kadam on 07/10/20.
//

#ifndef MYSQL_TABLESPACE_H
#define MYSQL_TABLESPACE_H

#include <srv0srv.h>
#include <dict0boot.h>
#include <fil0fil.h>
#include <fsp0fsp.h>

void fill_dict_table(char* name, dict_table_t* ret_table){
  ret_table->name = (char *) name;

  // Get a new table id.
  dict_hdr_get_new_id(&(ret_table->id), nullptr, nullptr);

  // refer dict_tf_set()
  ret_table->flags = DICT_TF_COMPACT;
  // refer innobase_table_flags()
  ret_table->flags2 = DICT_TF2_USE_TABLESPACE | DICT_TF2_FTS_AUX_HEX_NAME;
}

bool create_user_tablespace(dict_table_t* table){
  ulint space_id;

  // Get a new space id.
  dict_hdr_get_new_id(nullptr, nullptr, &space_id);

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
      space_id, table->name, path,
      dict_tf_to_fsp_flags(table->flags), table->flags2,
      FIL_IBD_FILE_INITIAL_SIZE,
      FIL_ENCRYPTION_OFF, ENCRYPTION_KEY_SYSTEM_DATA);

  if(code != dberr_t::DB_SUCCESS){
    return false;
  }

  table->space = (unsigned int) space_id;

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

#endif // MYSQL_TABLESPACE_H
