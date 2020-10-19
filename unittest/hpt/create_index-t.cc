//
// Created by kadam on 05/10/20.
//

#include "init_n_des.h"
#include <dict0boot.h>
#include "utl_table.h"
#include "utl_index.h"
#include "utl_data.h"
//#include "mysys/charset.c"


void test_hpt(char* table_name)
{
  dict_table_t *table = nullptr;
  dict_index_t *index= nullptr;
  mem_heap_t* data_heap = nullptr;
  dtuple_t* data_tuple = nullptr;


  bool success = create_table(table_name, &table);
  ok(success, "Tablespace creation");

  success = create_clustered_index_without_primary(table, &index);
  ok(success, "Index creation");

  // as the data tuple is a dynamic length variable, we use a heap to store it
  data_heap = mem_heap_create(sizeof(dtuple_t)
                                          + 2 * (sizeof(dfield_t)
                                                 + sizeof(que_fork_t)
                                                 + sizeof(upd_node_t)
                                                 + sizeof(upd_t) + 12));
  create_data_tuple(index, data_heap, &data_tuple);

  // TODO: index insertion needs thread as well as it's transaction
  //  hence, find out the relation between thi thread's transaction
  //  and the transaction used in fields
  //  modify code accordingly

  ulint err;
  btr_cur_t cursor;
  mtr_t mtr;
  ulint *offsets = NULL;
  mem_heap_t *heap = NULL;
  rec_t *rec;
  que_thr_t *que_thr = NULL;
  big_rec_t *big_rec = NULL;

  mtr_start(&mtr);
  err = btr_cur_search_to_nth_level(
      index, 0, data_tuple, PAGE_CUR_LE,
      BTR_MODIFY_LEAF, &cursor, 0,
      __FILE__, __LINE__, &mtr);

  if (err != DB_SUCCESS) {
    index->table->file_unreadable = true;
    mtr_commit(&mtr);
    ok(0, "search failed");
    exit(1);
  }

  err = btr_cur_optimistic_insert(0, &cursor, &offsets, &heap,
                                  data_tuple, &rec,
                                  &big_rec, 0, que_thr, &mtr);

  if (err != DB_SUCCESS) {
    index->table->file_unreadable = true;
    mtr_commit(&mtr);
    ok(0, "search failed");
    exit(1);
  }

  // TODO: free the data_heap as and when needed
  //  delete it at last
  //  refer: mem_heap_free and mem_heap_empty
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
  test_hpt((char* )table_name);

  destroy();

  // delete the created tablespace file
  // always run it after destruction
  delete_tablespace_ibd_file((char* )table_name);

  my_end(0);
  return exit_status();

}
