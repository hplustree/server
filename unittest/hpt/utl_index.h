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

/*******************************************************************//**
Copies fields contained in index2 to index1. */
static
void
dict_index_copy(
/*============*/
    dict_index_t*		index1,	/*!< in: index to copy to */
    dict_index_t*		index2,	/*!< in: index to copy from */
    const dict_table_t*	table,	/*!< in: table */
    ulint			start,	/*!< in: first position to copy */
    ulint			end)	/*!< in: last position to copy */
{
  dict_field_t*	field;
  ulint		i;

  /* Copy fields contained in index2 */

  for (i = start; i < end; i++) {

    field = dict_index_get_nth_field(index2, i);
    dict_index_add_col(index1, table, field->col,
                       field->prefix_len);
  }
}

dict_index_t *dict_index_build_clust(dict_table_t *table, dict_index_t *index) {
  dict_index_t *new_index;
  dict_field_t *field;
  ulint trx_id_pos;
  ulint i;
  ibool *indexed;

  ut_ad(table && index);
  ut_ad(dict_index_is_clust(index));
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  /* Create a new index object with certainly enough fields */
  new_index = dict_mem_index_create(table->name,
                                    index->name, table->space,
                                    index->type,
                                    index->n_fields + table->n_cols);

  /* Copy other relevant data from the old index struct to the new
  struct: it inherits the values */

  new_index->n_user_defined_cols = index->n_fields;

  new_index->id = index->id;
  btr_search_index_init(new_index);

  /* Copy the fields of index */
  dict_index_copy(new_index, index, table, 0, index->n_fields);

  if (dict_index_is_univ(index)) {
    /* No fixed number of fields determines an entry uniquely */

    new_index->n_uniq = REC_MAX_N_FIELDS;

  } else if (dict_index_is_unique(index)) {
    /* Only the fields defined so far are needed to identify
    the index entry uniquely */

    new_index->n_uniq = new_index->n_def;
  } else {
    /* Also the row id is needed to identify the entry */
    new_index->n_uniq = 1 + new_index->n_def;
  }

  new_index->trx_id_offset = 0;

  if (!dict_index_is_ibuf(index)) {
    /* Add system columns, trx id first */

    trx_id_pos = new_index->n_def;

#if DATA_ROW_ID != 0
# error "DATA_ROW_ID != 0"
#endif
#if DATA_TRX_ID != 1
# error "DATA_TRX_ID != 1"
#endif
#if DATA_ROLL_PTR != 2
# error "DATA_ROLL_PTR != 2"
#endif

    if (!dict_index_is_unique(index)) {
      dict_index_add_col(new_index, table,
                         dict_table_get_sys_col(
                             table, DATA_ROW_ID),
                         0);
      trx_id_pos++;
    }

    dict_index_add_col(new_index, table,
                       dict_table_get_sys_col(table, DATA_TRX_ID),
                       0);

    dict_index_add_col(new_index, table,
                       dict_table_get_sys_col(table,
                                              DATA_ROLL_PTR),
                       0);

    for (i = 0; i < trx_id_pos; i++) {

      ulint fixed_size = dict_col_get_fixed_size(
          dict_index_get_nth_col(new_index, i),
          dict_table_is_comp(table));

      if (fixed_size == 0) {
        new_index->trx_id_offset = 0;

        break;
      }

      if (dict_index_get_nth_field(new_index, i)->prefix_len
          > 0) {
        new_index->trx_id_offset = 0;

        break;
      }

      /* Add fixed_size to new_index->trx_id_offset.
      Because the latter is a bit-field, an overflow
      can theoretically occur. Check for it. */
      fixed_size += new_index->trx_id_offset;

      new_index->trx_id_offset = fixed_size;

      if (new_index->trx_id_offset != fixed_size) {
        /* Overflow. Pretend that this is a
        variable-length PRIMARY KEY. */
        ut_ad(0);
        new_index->trx_id_offset = 0;
        break;
      }
    }
  }

  /* Remember the table columns already contained in new_index */
  indexed = static_cast<ibool *>(
      mem_zalloc(table->n_cols * sizeof *indexed));

  /* Mark the table columns already contained in new_index */
  for (i = 0; i < new_index->n_def; i++) {

    field = dict_index_get_nth_field(new_index, i);

    /* If there is only a prefix of the column in the index
    field, do not mark the column as contained in the index */

    if (field->prefix_len == 0) {

      indexed[field->col->ind] = TRUE;
    }
  }

  /* Add to new_index non-system columns of table not yet included
  there */
  for (i = 0; i + DATA_N_SYS_COLS < (ulint) table->n_cols; i++) {

    dict_col_t *col = dict_table_get_nth_col(table, i);
    ut_ad(col->mtype != DATA_SYS);

    if (!indexed[col->ind]) {
      dict_index_add_col(new_index, table, col, 0);
    }
  }

  mem_free(indexed);
  dict_mem_index_free(index);

  ut_ad(dict_index_is_ibuf(index)
        || (UT_LIST_GET_LEN(table->indexes) == 0));

  new_index->cached = TRUE;

  return (new_index);
}

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

  *ret_index = dict_index_build_clust(table, *ret_index);

  (*ret_index)->n_fields = (*ret_index)->n_def;
  (*ret_index)->table = table;
  (*ret_index)->table_name = table->name;
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
