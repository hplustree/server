//
// Created by kadam on 05/10/20.
//

#include "init_n_des.h"
#include <dict0boot.h>
//#include "mysys/charset.c"

void test_create_index()
{
  ulint type= 1;
  ulint space= 6;
  ulint zip_size= 0;
  index_id_t index_id= (index_id_t) 22;
  ulint n_fields= 5;
  const char *table_name= "testing/test";
  const char *index_name= "GEN_CLUST_INDEX";
  dict_table_t table;
  mtr_t mtr;
  ulint root_page_no;
//  que_thr_t *thr;
//  tab_node_t *node;

  table.name= (char *) table_name;
  table.space= space;
  table.flags= 0;
  table.flags2= 80;

  dict_hdr_get_new_id(&table.id, &index_id, &space);

  dberr_t code= fil_create_new_single_table_tablespace(
      space, table.name, NULL, table.flags, table.flags2,
      FIL_IBD_FILE_INITIAL_SIZE, FIL_ENCRYPTION_OFF, 1);

  ok(code == dberr_t::DB_SUCCESS, "Created Table");

  dict_index_t *index=
      dict_mem_index_create(table_name, index_name, space, type, n_fields);

  index->table = &table;
  mtr_start(&mtr);

  fsp_header_init(table.space, FIL_IBD_FILE_INITIAL_SIZE, &mtr);

  //    index->id = 22;
  //    index->page = FIL_NULL;
  //    index->name = "GEN_CLUST_INDEX";
  //    index->table_name = "testing/test";
  //    index->space = 6;
  //    index->type = 1;
  //    index->trx_id_offset = 6;
  //    index->n_user_defined_cols = 0;
  //    index->n_uniq =  1;
  //    index->n_def =  5;
  //    index->n_fields = 5;
  //    index->n_nullable = 2;
  //    index->cached = 1;
  //    index->to_be_dropped = 0;
  //    index->online_status = 0;
  root_page_no= btr_create(type, space, zip_size, index_id, index, &mtr);
  printf("%lul", root_page_no);

}

//void test_insert() {
//  btr_cur_optimistic_insert()
//}



int main(int argc __attribute__((unused)),char *argv[])
{

  MY_INIT(argv[0]);

  // count is the number of tests to run in this file
  plan(1);

  // setup
  setup();
//  init_available_charsets();

  // test1: create tablespace
  test_create_index();

  destroy();

  my_end(0);
  return exit_status();

}
