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
#include "../../../storage/xtradb/dict/dict0dict.cc"

bool create_tree(dict_index_t* index, page_t* ret_root){

  mtr_t mtr;
  index->page = btr_create(index->type, index->space, 0,
                           index->id, index, &mtr);

  if (index->page == FIL_NULL){
    return false;
  }
  *ret_root = *btr_root_get(index, &mtr);
  mtr_commit(&mtr);

  return true;
}

bool create_clustered_index_without_primary(
    dict_table_t* table, dict_index_t* ret_index){
  const char innobase_index_reserve_name[] = "GEN_CLUST_INDEX";


  // create index object
  *ret_index = *dict_mem_index_create(table->name,
                                     innobase_index_reserve_name,
                                     table->space,
                                     DICT_CLUSTERED,
                                     table->n_cols);

  // Get a new index id.
  dict_hdr_get_new_id(NULL, &ret_index->id, NULL);

  ret_index->n_def = ret_index->n_fields;
  ret_index->n_user_defined_cols = ret_index->n_fields;

  *ret_index = *dict_index_build_internal_clust(table, ret_index);

  ret_index->n_fields = ret_index->n_def;
  UT_LIST_ADD_LAST(indexes, table->indexes, ret_index);
  ret_index->table = table;
  ret_index->table_name = table->name;
  ret_index->search_info = btr_search_info_create(ret_index->heap);


  // create tree
  page_t root;
  if (!create_tree(ret_index, &root)){
    return false;
  }

  rw_lock_create(index_tree_rw_lock_key, &ret_index->lock,
                 dict_index_is_ibuf(ret_index)
                 ? SYNC_IBUF_INDEX_TREE : SYNC_INDEX_TREE);

  return (mach_read_from_4(&root + FIL_PAGE_OFFSET) == ret_index->page);
}


#endif // MYSQL_INDEX_H
