//
// Created by anuj on 17/09/20.
//

#include "../storage/xtradb/include/fil0fil.h"

int main() {
  //tablespace creation
  dberr_t code = fil_create_new_single_table_tablespace(1, "t1", "TEMP", 0, 0, 1, FIL_ENCRYPTION_OFF, 0);

  //check tablespace
  ulint check = fil_open_single_table_tablespace(false, false, 1, 0, "TEMP/t1", "t1");
  if(check == 10) {
    printf("tablespace exists");
  }
}