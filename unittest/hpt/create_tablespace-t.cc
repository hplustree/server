//
// Created by kadam on 22/09/20.
//

/** Unit test case for the function create_tablespace(). */

#include <fil0fil.h>
#include <ctime>
#include <tap.h>

void setup(){
  // init variables (values and code are selected after closely inspecting them in debug menu)

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
}

void get_strtime(char* ret_str, int size){
  time_t rawtime;
  struct tm* timeinfo;
  time (&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(ret_str,size,"%d-%m-%Y %H:%M:%S",timeinfo);
}

void test_create_tablespace(){

  char tablename[80];
  get_strtime(tablename, sizeof(tablename));

  dberr_t code = fil_create_new_single_table_tablespace(100, tablename, NULL, 0, 80, 4, FIL_ENCRYPTION_OFF, 1);

  ok(code == dberr_t::DB_SUCCESS, "Tablespace creation");
}

int main(int argc __attribute__((unused)),char *argv[])
{

  MY_INIT(argv[0]);

  // count is the number of tests to run in this file
  plan(1);

  // setup
  setup();

  // test1: create tablespace
  test_create_tablespace();

  my_end(0);
  return exit_status();
  // TODO: the below code
  //check tablespace
//  ulint check = fil_open_single_table_tablespace(false, false, 1, 0, "TEMP/t1", "t1");
//  if(check == dberr_t::DB_SUCCESS) {
//    printf("tablespace exists");
//  }


}