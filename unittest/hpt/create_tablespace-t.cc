//
// Created by kadam on 22/09/20.
//

/** Unit test case for the function create_tablespace(). */

#include <fil0fil.h>


int main(int argc __attribute__((unused)),char *argv[])
{

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

  // tablespace creation
  dberr_t code = fil_create_new_single_table_tablespace(100, "t110", "data", 0, 80, 4, FIL_ENCRYPTION_OFF, 1);

  if(code == dberr_t::DB_SUCCESS) {
    printf("tablespace created");
  }

  // TODO: the below code
  //check tablespace
//  ulint check = fil_open_single_table_tablespace(false, false, 1, 0, "TEMP/t1", "t1");
//  if(check == dberr_t::DB_SUCCESS) {
//    printf("tablespace exists");
//  }


}