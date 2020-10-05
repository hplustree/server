//
// Created by anuj on 30/09/20.
//

#include <iostream>
#include <btr0btr.h>

int main() {
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
  dberr_t code = fil_create_new_single_table_tablespace(16, "t2", "data", 0, 80, 4, FIL_ENCRYPTION_OFF, 1);

  mtr_t mtr;
  //start mtr
  mtr_start(&mtr);
  dict_index_t* index = dict_mem_index_create("t2","GEN_CLUST_INDEX", 16, DICT_CLUSTERED, 0);

  //index tree creation
  btr_create(FIL_PAGE_INDEX, 16, 0, 32, index, &mtr);
  if(code == dberr_t::DB_SUCCESS) {
    printf("tablespace created");
  }

  buf_block_t* block = btr_root_block_get(index, RW_S_LATCH, mtr);
}