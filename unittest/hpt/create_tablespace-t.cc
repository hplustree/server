//
// Created by kadam on 22/09/20.
//

/** Unit test case for the function create_tablespace(). */

#include <fil0fil.h>
#include <dict0dict.h>
#include <lock0lock.h>
#include <ctime>
#include <tap.h>

void get_strtime(char* ret_str, int size){
  time_t rawtime;
  struct tm* timeinfo;
  time (&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(ret_str,size,"%d-%m-%Y %H:%M:%S",timeinfo);
}

void setup(char* ret_tablename, int size){
  // TODO: see xtrabackup_prepare_func()
  // init variables (values and code are selected after closely inspecting them in debug menu)
//  extern "C" MYSQL_PLUGIN_IMPORT ulong server_id;
//  extern ulong concurrency;
//  extern time_t server_start_time, flush_status_time;
//  extern char *opt_mysql_tmpdir, mysql_charsets_dir[];
//  extern int mysql_unpacked_real_data_home_len;
//  extern MYSQL_PLUGIN_IMPORT MY_TMPDIR mysql_tmpdir_list;

  // test 1
  // mutexes
  UT_LIST_INIT(mutex_list);
  mutex_create(mutex_list_mutex_key, &mutex_list_mutex,
               SYNC_NO_ORDER_CHECK);

  UT_LIST_INIT(rw_lock_list);
  mutex_create(rw_lock_list_mutex_key, &rw_lock_list_mutex,
               SYNC_NO_ORDER_CHECK);

  // buffer variables
  srv_log_buffer_size = 1024;
  srv_log_block_size = 512;

  // memory
  mem_init(8388608);

  // as we are working on fil module
  fil_init(50000, 2000);

  // for logs
  log_init();

  // for granting write permissions
  srv_allow_writes_event = os_event_create();
  os_event_set(srv_allow_writes_event);

  get_strtime(ret_tablename, size);


  // test 2
//  rw_lock_create(dict_operation_lock_key,
//                 &dict_operation_lock, SYNC_DICT_OPERATION);
//
//  srv_buf_pool_size = 134217728;
//  srv_n_read_io_threads = 4;
//  srv_n_write_io_threads = 4;
//  btr_search_index_num = 1;
//  srv_lock_table_size = 5 * (srv_buf_pool_size / UNIV_PAGE_SIZE);
//  lock_sys_create(srv_lock_table_size);
//  buf_pool_init(srv_buf_pool_size, false, 1);
}

//void teardown(char* tablename){
//
//}

void test_create_tablespace(char* tablename){

  dberr_t code = fil_create_new_single_table_tablespace(100, tablename, NULL, 0, 80, 4, FIL_ENCRYPTION_OFF, 1);

  ok(code == dberr_t::DB_SUCCESS, "Create Tablespace");
}

void test_open_tablespace(char* tablename){
  dberr_t code = fil_open_single_table_tablespace(true, false, 100, 0, tablename, NULL);

  ok(code == dberr_t::DB_SUCCESS, "Open Tablespace");
}



int main(int argc __attribute__((unused)),char *argv[])
{

  MY_INIT(argv[0]);

  // count is the number of tests to run in this file
  plan(1);

  char tablename[80];

  // setup
  setup(tablename, sizeof(tablename));

  // test1: create tablespace
  test_create_tablespace(tablename);

  // test2: open tablespace
//  test_open_tablespace(tablename);

  // teardown
//  teardown(tablename);

  my_end(0);
  return exit_status();

}