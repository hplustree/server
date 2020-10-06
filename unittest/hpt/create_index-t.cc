//
// Created by kadam on 05/10/20.
//

#include "init_n_des.h"


void test_create_index() {
  ulint type = 1;
  ulint space = 6;
  ulint zip_size = 0;
  index_id_t index_id = (index_id_t)22;
  ulint n_fields = 1;
  const char* table_name = "testing/test";
  const char* index_name = "GEN_CLUST_INDEX";

  dberr_t code = fil_create_new_single_table_tablespace(space, table_name, NULL, 0, 80, 4, FIL_ENCRYPTION_OFF, 1);

  ok(code == dberr_t::DB_SUCCESS, "Created Table");

  dict_index_t*	index = dict_mem_index_create(table_name, index_name, space, type, n_fields);
  mtr_t mtr;
  ulint root_page_no;
  mtr_start(&mtr);
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
  root_page_no = btr_create(type, space, zip_size, index_id, index, &mtr);
  printf("%lul", root_page_no);
}



int main(int argc __attribute__((unused)),char *argv[])
{

  MY_INIT(argv[0]);

  // count is the number of tests to run in this file
  plan(1);

  // setup
  setup();

  // test1: create tablespace
  test_create_index();

  destroy();

  my_end(0);
  return exit_status();

}
