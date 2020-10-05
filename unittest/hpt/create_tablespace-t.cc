//
// Created by kadam on 05/10/20.
//

#include "init_n_des.h"
#include <ctime>
#include <tap.h>


void get_strtime(char* ret_str, int size){
  time_t rawtime;
  struct tm* timeinfo;
  time (&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(ret_str,size,"%d-%m-%Y %H:%M:%S",timeinfo);
}


void test_create_tablespace(char* tablename){

  dberr_t code = fil_create_new_single_table_tablespace(101, tablename, NULL, 0, 80, 4, FIL_ENCRYPTION_OFF, 1);

  ok(code == dberr_t::DB_SUCCESS, "Create Tablespace");
}

void test_open_tablespace(char* tablename){
  dberr_t code = fil_open_single_table_tablespace(true, false, 101, 0, tablename, NULL);

  ok(code == dberr_t::DB_SUCCESS, "Open Tablespace");
}


int main(int argc __attribute__((unused)),char *argv[])
{

  MY_INIT(argv[0]);

  // count is the number of tests to run in this file
  plan(2);


  // setup
  setup();

  char tablename[80];
  get_strtime(tablename, sizeof(tablename));

  // test1: create tablespace
  test_create_tablespace(tablename);

  // test2: open tablespace
  test_open_tablespace(tablename);

  // TODO: delete table space file (.ibd patch)
  char file_name[100];
  int dirnamelen = strlen(srv_data_home);
  memcpy(file_name, srv_data_home, dirnamelen);
  strcpy(file_name + dirnamelen, tablename);
  remove(file_name);

  destroy();

  my_end(0);
  return exit_status();

}