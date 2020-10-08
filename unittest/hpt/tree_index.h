//
// Created by kadam on 07/10/20.
//

#ifndef MYSQL_INDEX_H
#define MYSQL_INDEX_H

#include <btr0btr.h>
#include <dict0mem.h>
#include <btr0btr.h>
#include <dict0boot.h>


dict_index_t* get_dict_index(dict_table_t table, ulint num_fields){
  dict_index_t* ret_index = dict_mem_index_create(table.name,
                                    "GEN_CLUST_INDEX",
                                    table.space,
                                    DICT_CLUSTERED | DICT_UNIQUE,
                                    num_fields);


  // Get a new index id.
  dict_hdr_get_new_id(&(ret_index->id), NULL, NULL);

  ret_index->table = &table;

  return ret_index;
}

bool create_gen_clustered_index(dict_index_t* index){
  ulint root_page_no;

  mtr_t mtr;
  mtr_start(&mtr);
  root_page_no = btr_create(
      index->type, index->space, 0, index->id, index, &mtr);
  mtr_commit(&mtr);

  return root_page_no != FIL_NULL;
}


#endif // MYSQL_INDEX_H
