//
// Created by kadam on 22/09/20.
//

/** Unit test case for the function create_tablespace(). */

#include <fil0fil.h>

int main(int argc __attribute__((unused)),char *argv[])
{

  //tablespace creation
  dberr_t code = fil_create_new_single_table_tablespace(1, "t1", "TEMP", 0, 0, 1, FIL_ENCRYPTION_OFF, 0);

  if(code == dberr_t::DB_SUCCESS) {
    printf("tablespace exists");
  }

  //check tablespace
  ulint check = fil_open_single_table_tablespace(false, false, 1, 0, "TEMP/t1", "t1");
  if(check == dberr_t::DB_SUCCESS) {
    printf("tablespace exists");
  }


}