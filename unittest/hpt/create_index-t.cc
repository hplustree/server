//
// Created by kadam on 05/10/20.
//

#include "init_n_des.h"
#include <dict0boot.h>
#include "tablespace.h"
#include "tree_index.h"
//#include "mysys/charset.c"


void test_create_index(char* table_name)
{
  dict_table_t table;
  fill_dict_table(table_name, &table);

  bool success = create_user_tablespace(&table);
  ok(success, "Tablespace creation");


  // must be greater than 3
  ulint num_fields = 5;

  dict_index_t* index = get_dict_index(table, num_fields);

  success = create_gen_clustered_index(index);
  ok(success, "Index creation");

//  dict_create_index_tree_step
//  root_page_no = btr_create(type, table.space, zip_size, index_id, index, &mtr);

//  printf("%ul", table.space);

}

//void test_insert() {
//  btr_cur_optimistic_insert()
//}



int main(int argc __attribute__((unused)),char *argv[])
{

  MY_INIT(argv[0]);

  // count is the number of tests to run in this file
  plan(2);

  // setup
  setup();
  const char *table_name = "test";

  // test1: create tablespace
  test_create_index((char* )table_name);

  destroy();

  // delete the created tablespace file
  // always run it after destruction
  delete_tablespace_ibd_file((char* )table_name);

  my_end(0);
  return exit_status();

}
