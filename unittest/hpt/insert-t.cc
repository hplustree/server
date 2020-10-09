//
// Created by aayushi on 07/10/20.
//

#include "init_n_des.h"
#include "tablespace.h"
#include <dict0boot.h>
#include <que0que.h>
#include "../../storage/xtradb/row/row0mysql.cc"
#include "../../storage/xtradb/row/row0ins.cc"
#include "../../storage/xtradb/dict/dict0dict.cc"
#include <random>

void test_create_index(dict_index_t *index, dict_table_t *table,
                       char *table_name)
{

  // create table test (id int, name int);

  ulint type= 1;
  ulint space= 100;
  ulint zip_size= 0;
  index_id_t index_id= (index_id_t) 22;
  ulint n_fields= 5;
  ulint n_cols= 2;
  ulint flags= DICT_TF_COMPACT;
  ulint flags2= 80;
  const char *index_name= "GEN_CLUST_INDEX";
  mtr_t mtr;
  ulint root_page_no= FIL_NULL;

  mem_heap_t* heap = mem_heap_create(450);

  *table= *dict_mem_table_create(table_name, space, n_cols, flags, flags2);

  dict_mem_table_add_col(table, heap, "DB_ROW_ID", DATA_SYS,
                         DATA_ROW_ID | DATA_NOT_NULL,
                         DATA_ROW_ID_LEN);
  dict_mem_table_add_col(table, heap, "DB_TRX_ID", DATA_SYS,
                         DATA_TRX_ID | DATA_NOT_NULL,
                         DATA_TRX_ID_LEN);
  dict_mem_table_add_col(table, heap, "DB_ROLL_PTR", DATA_SYS,
                         DATA_ROLL_PTR | DATA_NOT_NULL,
                         DATA_ROLL_PTR_LEN);
  dict_mem_table_add_col(table, heap, "id", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "value", DATA_INT, 0, 4);

  dict_hdr_get_new_id(&table->id, NULL, NULL);
  UT_LIST_INIT(table->indexes);
  //  dict_table_add_to_cache(table, FALSE, heap);

  mtr_start(&mtr);

  dberr_t code= fil_create_new_single_table_tablespace(
      space, table->name, NULL, dict_tf_to_fsp_flags(table->flags),
      table->flags2, FIL_IBD_FILE_INITIAL_SIZE, FIL_ENCRYPTION_OFF, 1);

  fsp_header_init(table->space, FIL_IBD_FILE_INITIAL_SIZE, &mtr);

  ok(code == dberr_t::DB_SUCCESS, "Table creation");

  *index= *dict_mem_index_create(table_name, index_name, space, type, n_fields);
  *index = *dict_index_build_internal_clust(table, index);

//  index->id= index_id;
//  index->trx_id_offset= 6;
//  index->n_user_defined_cols= 0;
//  index->n_uniq= 1;
//  index->n_def= 5;
//  index->n_nullable= 2;
//  index->cached= 1;
//  index->to_be_dropped= 0;
//  index->online_status= 0;
//  index->table= table;

//  btr_search_index_init(index);

//  dberr_t err= dict_index_add_to_cache(table, index, root_page_no, FALSE);
//  ut_a(err == DB_SUCCESS);

  root_page_no= btr_create(type, space, zip_size, index_id, index, &mtr);
  index->page= root_page_no;
  printf("\nroot page number : %lul \n", root_page_no);

  page_t *root= btr_root_get(index, &mtr);
  UT_LIST_ADD_LAST(indexes, table->indexes, index);

  mem_heap_free(heap);
  mtr_commit(&mtr);

  ok(mach_read_from_4(root + FIL_PAGE_OFFSET) == root_page_no,
     "Created index");
}

void test_insert(dict_index_t *index, dict_table_t *table, ulint length)
{
  btr_cur_t cursor;
  ulint *offsets= NULL;
  rec_t *rec;
  ins_node_t *node= NULL;
  que_thr_t *que_thr= NULL;
  mtr_t mtr;
  big_rec_t *big_rec= NULL;
  mem_heap_t *heap= NULL;
  byte *mysql_rec= NULL;
  ulint mysql_row_len= 9;
  //  ulint value1, value2;

  // generate random numbers
  ulint seed= 42;
  std::vector<ulint> entries(length);

  for (ulint i= 1; i <= length; i++)
  {
    entries[i]= i;
  }

  std::shuffle(entries.begin(), entries.end(),
               std::default_random_engine(seed));

  mtr_start(&mtr);

  cursor.thr= que_thr;

  for (ulint i= 0; i < length; i++)
  {
    // convert values in bytes; this part will be changed
    //    value1 = entries[i];
    //    value2 = value1 * 10;
    mysql_rec= (unsigned char *) "00011010";

    row_prebuilt_t *pre_built= row_create_prebuilt(table, mysql_row_len);

    row_get_prebuilt_insert_row(pre_built);
    node= pre_built->ins_node;

    row_mysql_convert_row_to_innobase(pre_built->ins_node->row, pre_built,
                                      mysql_rec);

    que_thr= que_fork_get_first_thr(pre_built->ins_graph);

    que_thr->run_node= node;
    que_thr->prev_node= node;

    ut_ad(dtuple_check_typed(pre_built->ins_node->row));

    row_ins_index_entry_set_vals(node->index, node->entry, node->row);

    ut_ad(dtuple_check_typed(node->entry));

    dberr_t err= btr_cur_search_to_nth_level(
        index, 0, node->entry, PAGE_CUR_LE, BTR_MODIFY_LEAF, &cursor, 0,
        __FILE__, __LINE__, &mtr);
    if (err != DB_SUCCESS)
    {
      index->table->file_unreadable= true;
      mtr_commit(&mtr);
      ok(0, "search failed");
      exit(1);
    }

    err= btr_cur_optimistic_insert(0, &cursor, &offsets, &heap, node->entry,
                                   &rec, &big_rec, 0, que_thr, &mtr);

    if (err == DB_FAIL)
    {
      err= btr_cur_pessimistic_insert(0, &cursor, &offsets, &heap, node->entry,
                                      &rec, &big_rec, 0, que_thr, &mtr);
    }

    if (err != DB_SUCCESS)
    {
      mtr_commit(&mtr);
      ok(0, "insert failed");
      exit(1);
    }
  }

  mtr_commit(&mtr);
}

int main(int argc __attribute__((unused)), char *argv[])
{

  MY_INIT(argv[0]);

  // count is the number of tests to run in this file
  plan(3);

  // setup
  setup();

  // test1: create tablespace
  const char *table_name= "test";
  dict_index_t index;
  dict_table_t table;
  test_create_index(&index, &table, (char *) table_name);

  // test: insert operation
  ulint length= 10;
  test_insert(&index, &table, length);

  destroy();

  // delete the created tablespace file
  // always run it after destruction
  delete_tablespace_ibd_file((char *) table_name);

  my_end(0);
  return exit_status();
}
