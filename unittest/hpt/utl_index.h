//
// Created by kadam on 07/10/20.
//

#ifndef MYSQL_INDEX_H
#define MYSQL_INDEX_H

#include <dict0mem.h>
#include <dict0boot.h>
#include <dict0dict.h>
#include <btr0btr.h>
#include <btr0sea.h>
#include <mach0data.h>
#include <data0data.h>
#include "utl_dict.h"

bool create_tree(dict_index_t* index, page_t** ret_root){

  mtr_t mtr;
  mtr_start(&mtr);
  index->page = btr_create(index->type, index->space, 0,
                           index->id, index, &mtr);

  if (index->page == FIL_NULL){
    return false;
  }
  *ret_root = btr_root_get(index, &mtr);
  mtr_commit(&mtr);

  return true;
}

bool create_clustered_index_without_primary(
    dict_table_t* table, dict_index_t** ret_index){
  const char innobase_index_reserve_name[] = "GEN_CLUST_INDEX";


  // create index object
  *ret_index = dict_mem_index_create(table->name,
                                     innobase_index_reserve_name,
                                     table->space,
                                     DICT_CLUSTERED,
                                     0);

  // Get a new index id.
  dict_hdr_get_new_id(NULL, &(*ret_index)->id, NULL);

  // TODO: the proper way to do this is to acquire mutex, add index to cache,
  //  and release the mutex
  *ret_index = dict_index_build_clust(table, *ret_index);

  (*ret_index)->n_fields = (*ret_index)->n_def;
  (*ret_index)->table = table;
  (*ret_index)->table_name = table->name;
  // TODO: status flags are not set in the newly created index
  rw_lock_create(index_tree_rw_lock_key, &(*ret_index)->lock, SYNC_INDEX_TREE);
  UT_LIST_ADD_LAST(indexes, table->indexes, *ret_index);
  (*ret_index)->search_info = btr_search_info_create((*ret_index)->heap);


  // create tree
  page_t* root;
  if (!create_tree(*ret_index, &root)){
    return false;
  }


  return (mach_read_from_4(root + FIL_PAGE_OFFSET) == (*ret_index)->page);
}


#endif // MYSQL_INDEX_H
