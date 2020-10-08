//
// Created by aayushi on 07/10/20.
//

#include "init_n_des.h"
#include <dict0boot.h>

void test_create_index(dict_index_t *index)
{

  // create table test (id int, name int);

  ulint type= 1;
  ulint space= 100;
  ulint zip_size= 0;
  index_id_t index_id= (index_id_t) 22;
  ulint n_fields= 5;
  const char *table_name= "test";
  const char *index_name= "GEN_CLUST_INDEX";
  dict_table_t table;
  mtr_t mtr;
  ulint root_page_no;

  table.name= (char *) table_name;
  table.space= space;
  table.flags= 1;
  table.flags2= 80;

  dict_hdr_get_new_id(&table.id, NULL, NULL);

  dberr_t code= fil_create_new_single_table_tablespace(
      space, table.name, NULL, dict_tf_to_fsp_flags(table.flags), table.flags2,
      FIL_IBD_FILE_INITIAL_SIZE, FIL_ENCRYPTION_OFF, 1);

  ok(code == dberr_t::DB_SUCCESS, "Table creation");

  index= dict_mem_index_create(table_name, index_name, space, type, n_fields);

  index->table= &table;
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
  index->page= root_page_no;
  printf("\nroot page number : %lul \n", root_page_no);

  page_t *root= btr_root_get(index, &mtr);
  ok(mach_read_from_4(root + FIL_PAGE_OFFSET) == root_page_no,
     "Created index");
}

void test_insert(dict_index_t *index, ulint length)
{
  btr_cur_t cursor;
  ulint *offsets= NULL;
  dtuple_t *entry= NULL;
  rec_t *rec;
  que_thr_t *que_thr= NULL;
  mtr_t mtr;
  big_rec_t *big_rec= NULL;
  mem_heap_t *heap= NULL;

  mtr_start(&mtr);

  cursor.thr= que_thr;

  for (ulint i= 0; i < length; i++)
  {
    dberr_t err= btr_cur_search_to_nth_level(index, 0, entry, PAGE_CUR_LE,
                                             BTR_MODIFY_LEAF, &cursor, 2,
                                             __FILE__, __LINE__, &mtr);
    if (err != DB_SUCCESS)
    {
      index->table->file_unreadable= true;
      mtr_commit(&mtr);
      ok(0, "search failed");
      exit(1);
    }

    err= btr_cur_optimistic_insert(0, &cursor, &offsets, &heap, entry, &rec,
                                   &big_rec, 0, que_thr, &mtr);

    if (err == DB_FAIL)
    {
      err= btr_cur_pessimistic_insert(0, &cursor, &offsets, &heap, entry, &rec,
                                      &big_rec, 0, que_thr, &mtr);
    }

    if (err != DB_SUCCESS)
    {
      mtr_commit(&mtr);
      ok(0, "insert failed");
      exit(1);
    }
  }
}

int main(int argc __attribute__((unused)), char *argv[])
{

  MY_INIT(argv[0]);

  // count is the number of tests to run in this file
  plan(3);

  // setup
  setup();
  //  init_available_charsets();

  // test1: create tablespace
  dict_index_t index;
  test_create_index(&index);

  // test: insert operation
  ulint length= 1000;
  test_insert(&index, length);

  //  destroy();
  remove_local_files();

  my_end(0);
  return exit_status();
}
