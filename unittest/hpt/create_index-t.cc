//
// Created by kadam on 05/10/20.
//

#include "init_n_des.h"
#include <dict0boot.h>
#include "utl_table.h"
#include "utl_index.h"
//#include "mysys/charset.c"


void test_create_index(char* table_name)
{
  dict_table_t *table = nullptr;
  dict_index_t *index= nullptr;

  bool success = create_table(table_name, &table);
  ok(success, "Tablespace creation");

  success = create_clustered_index_without_primary(table, &index);
  ok(success, "Index creation");
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
