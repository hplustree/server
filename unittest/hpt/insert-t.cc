//
// Created by aayushi on 07/10/20.
//

#include "init_n_des.h"

#include <dict0boot.h>
#include <que0que.h>
#include <trx0trx.h>
#include "../../storage/xtradb/row/row0mysql.cc"
#include "../../storage/xtradb/row/row0ins.cc"
#include "../../storage/xtradb/dict/dict0dict.cc"
#include <random>
#include <ctime>
#include <iostream>


// insert into t values (10);
// data: fd 0a 00 00, len: 5 (1+4)

// insert into test values (10, "kadam"); varchar(20)
// data: fc 0a 00 00   00 05 6b 61   64 61 6d 00   00 00 00 00   00 00 00 00   00 00 00 00   00 00, len: 26 (1+4+21)

// insert into tii values (10, 31);
// data: f9 0a 00 00   00 1f 00 00   00, len: 9 (1+4+4)


// to calculate size of record in bytes rec_get_converted_size
// to convert data rec_convert_dtuple_to_rec


//table->record[0][0]= share->default_values[0];
//
///* Fix undefined null_bits. */
//if (share->null_bytes > 1 && share->last_null_bit_pos)
//{
//table->record[0][share->null_bytes - 1]=
//share->default_values[share->null_bytes - 1];
//}

// fill_record()

// see this function to populate data tuple of innodb - row_mysql_store_col_in_innobase_format()

dict_index_t *build_clust_index_def(dict_table_t *table, dict_index_t *index);

void dict_add_sys_cols(dict_table_t *table, mem_heap_t *heap);

void create_table_and_index(dict_index_t *index, dict_table_t *table,
                            char *table_name) {

    // create table test (id int, name int);

    ulint type = DICT_CLUSTERED;
    ulint space = 100;
    ulint zip_size = 0;
    ulint n_fields = 0;
    ulint n_cols = 2;
    ulint flags = DICT_TF_COMPACT;
    ulint flags2 = 80;
    const char *index_name = "GEN_CLUST_INDEX";
    mtr_t mtr;
    ulint root_page_no = FIL_NULL;

    mem_heap_t *heap = mem_heap_create(450);

    // create table object
    *table = *dict_mem_table_create(table_name, space, n_cols, flags, flags2);

    dict_mem_table_add_col(table, heap, "id", DATA_INT, 0, 4);
    dict_mem_table_add_col(table, heap, "value", DATA_INT, 0, 4);
    dict_add_sys_cols(table, heap);

    dict_hdr_get_new_id(&table->id, NULL, NULL);
    UT_LIST_INIT(table->indexes);

    mtr_start(&mtr);

    // create tablespace
    dberr_t code = fil_create_new_single_table_tablespace(
            space, table->name, NULL, dict_tf_to_fsp_flags(table->flags),
            table->flags2, FIL_IBD_FILE_INITIAL_SIZE, FIL_ENCRYPTION_OFF, 1);

    fsp_header_init(table->space, FIL_IBD_FILE_INITIAL_SIZE, &mtr);

    ok(code == dberr_t::DB_SUCCESS, "Table creation");

    // create index object
    *index = *dict_mem_index_create(table_name, index_name, space, type, n_fields);

    *index = *build_clust_index_def(table, index);

    index->table = table;
    index->table_name = table->name;
    index->search_info = btr_search_info_create(index->heap);
    dict_hdr_get_new_id(NULL, &index->id, NULL);
    rw_lock_create(index_tree_rw_lock_key, &index->lock,
                   dict_index_is_ibuf(index)
                   ? SYNC_IBUF_INDEX_TREE : SYNC_INDEX_TREE);

    // create tree
    root_page_no = btr_create(type, space, zip_size, index->id, index, &mtr);
    index->page = root_page_no;

    page_t *root = btr_root_get(index, &mtr);
    UT_LIST_ADD_LAST(indexes, table->indexes, index);

    mem_heap_free(heap);
    mtr_commit(&mtr);

    ok(mach_read_from_4(root + FIL_PAGE_OFFSET) == root_page_no,
       "Created index");
}

void create_table_and_index_with_primary_key(dict_index_t *index, dict_table_t *table,
                            char *table_name) {

    // create table test (id int primary key, name int);

    ulint type = DICT_CLUSTERED | DICT_UNIQUE;
    ulint space = 100;
    ulint zip_size = 0;
    ulint n_fields = 1;
    ulint n_cols = 2;
    ulint flags = DICT_TF_COMPACT;
    ulint flags2 = 80;
    const char *index_name = "PRIMARY";
    mtr_t mtr;
    ulint root_page_no = FIL_NULL;

    mem_heap_t *heap = mem_heap_create(450);

    // create table object
    *table = *dict_mem_table_create(table_name, space, n_cols, flags, flags2);

    dict_mem_table_add_col(table, heap, "id", DATA_INT, 0, 4);
    dict_mem_table_add_col(table, heap, "value", DATA_INT, 0, 4);
    dict_add_sys_cols(table, heap);

    dict_hdr_get_new_id(&table->id, NULL, NULL);
    UT_LIST_INIT(table->indexes);

    mtr_start(&mtr);

    // create tablespace
    dberr_t code = fil_create_new_single_table_tablespace(
            space, table->name, NULL, dict_tf_to_fsp_flags(table->flags),
            table->flags2, FIL_IBD_FILE_INITIAL_SIZE, FIL_ENCRYPTION_OFF, 1);

    fsp_header_init(table->space, FIL_IBD_FILE_INITIAL_SIZE, &mtr);

    ok(code == dberr_t::DB_SUCCESS, "Table creation");

    // create index object
    *index = *dict_mem_index_create(table_name, index_name, space, type, n_fields);

    // add primary key field to index
    dict_mem_index_add_field(index, "id", 0);
    index->fields->col = table->cols;
//    index->fields->col = static_cast<dict_col_t*>(mem_heap_alloc(heap, sizeof(dict_col_t)));
//    index->fields->col->mtype = DATA_INT;
//    index->fields->col->prtype = 0;
//    index->fields->col->len = 4;
//    index->fields->col->mbminlen = 0;
//    index->fields->col->mbmaxlen = 0;
//    index->fields->col->ind = 0;
//    index->fields->col->ord_part = 0;
//    index->fields->col->max_prefix = 0;
//    index->fields->fixed_len = 0;

    *index = *build_clust_index_def(table, index);

    index->table = table;
    index->table_name = table->name;
    index->search_info = btr_search_info_create(index->heap);
    index->n_fields = index->n_def;
    dict_hdr_get_new_id(NULL, &index->id, NULL);
    rw_lock_create(index_tree_rw_lock_key, &index->lock,
                   dict_index_is_ibuf(index)
                   ? SYNC_IBUF_INDEX_TREE : SYNC_INDEX_TREE);

    // create tree
    root_page_no = btr_create(type, space, zip_size, index->id, index, &mtr);
    index->page = root_page_no;

    page_t *root = btr_root_get(index, &mtr);
    UT_LIST_ADD_LAST(indexes, table->indexes, index);

    mem_heap_free(heap);
    mtr_commit(&mtr);

    ok(mach_read_from_4(root + FIL_PAGE_OFFSET) == root_page_no,
       "Created index");
}

dict_index_t *build_clust_index_def(dict_table_t *table, dict_index_t *index) {
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

    ut_ad(dict_index_is_ibuf(index)
          || (UT_LIST_GET_LEN(table->indexes) == 0));

    new_index->cached = TRUE;

    return (new_index);
}

void dict_add_sys_cols(dict_table_t *table, mem_heap_t *heap) {
    dict_mem_table_add_col(table, heap, "DB_ROW_ID", DATA_SYS,
                           DATA_ROW_ID | DATA_NOT_NULL,
                           DATA_ROW_ID_LEN);
    dict_mem_table_add_col(table, heap, "DB_TRX_ID", DATA_SYS,
                           DATA_TRX_ID | DATA_NOT_NULL,
                           DATA_TRX_ID_LEN);
    dict_mem_table_add_col(table, heap, "DB_ROLL_PTR", DATA_SYS,
                           DATA_ROLL_PTR | DATA_NOT_NULL,
                           DATA_ROLL_PTR_LEN);
}

row_prebuilt_t* test_insert(dict_index_t *index, dict_table_t *table,
                            std::vector<ulint> entries) {

    btr_cur_t cursor;
    ulint *offsets = NULL;
    rec_t *rec;
    que_thr_t *que_thr = NULL;
    mtr_t mtr;
    big_rec_t *big_rec = NULL;
    mem_heap_t *heap = NULL;
    byte *mysql_rec = NULL;
    dberr_t err;
    ulint mysql_row_len = 9;
    trx_t *user_trx = trx_allocate_for_background();

    row_prebuilt_t *pre_built = row_create_prebuilt(table, mysql_row_len);
    pre_built->default_rec = (unsigned char*)"\xff\x00\x00\x00\x00\x00\x00\x00\x00";
    pre_built->trx = user_trx;

    pre_built->n_template = 2;
    pre_built->mysql_template = (mysql_row_templ_t *)
            mem_alloc(5 * sizeof(mysql_row_templ_t));

    ulint col_offset[2] = {1, 5};
    for (ulint j = 0; j < pre_built->n_template; j++) {
        pre_built->mysql_template[j].mysql_null_bit_mask = 0;
        pre_built->mysql_template[j].mysql_col_len = 4;
        pre_built->mysql_template[j].mysql_col_offset = col_offset[j];
    }
    pre_built->mysql_template->mysql_null_bit_mask = 0;
    pre_built->mysql_template->mysql_col_len = 4;
    pre_built->mysql_template->mysql_col_offset = *col_offset;

    std::time_t start_time = time(nullptr);
    ulint data_len = entries.size();

    for (ulint i = 0; i < data_len; i++) {

        trx_start_if_not_started_xa(pre_built->trx);
        pre_built->trx->state = TRX_STATE_ACTIVE;

        mtr_start(&mtr);

        // convert values in bytes
        ulint data[] = {entries[i], entries[i] * 10};
        unsigned char arrayOfByte[9];
        int offs[2] = {0, 4};
        // header byte
        arrayOfByte[0] = '\xf9';

        for (ulint k = 0; k < 2; k++) {
            for (ulint l = 0; l < 4; l++)
                arrayOfByte[l + 1 + offs[k]] = (data[k] >> (l * 8));
        }
        mysql_rec = arrayOfByte;
//        mysql_rec =(unsigned char*) "\xf9\x01\x00\x00\x00\x02\x00\x00\x00";

        row_get_prebuilt_insert_row(pre_built);

        pre_built->ins_node->state = INS_NODE_ALLOC_ROW_ID;
//        pre_built->ins_node->state = INS_NODE_SET_IX_LOCK;

        row_ins_alloc_row_id_step(pre_built->ins_node);

        pre_built->ins_node->index = dict_table_get_first_index(
                pre_built->ins_node->table);

        pre_built->ins_node->entry = UT_LIST_GET_FIRST(
                pre_built->ins_node->entry_list);

        row_mysql_convert_row_to_innobase(pre_built->ins_node->row, pre_built,
                                          mysql_rec);

        que_thr = que_fork_get_first_thr(pre_built->ins_graph);

        que_thr->run_node = pre_built->ins_node;
        que_thr->prev_node = pre_built->ins_node;

        ut_ad(dtuple_check_typed(pre_built->ins_node->row));

        row_ins_index_entry_set_vals(pre_built->ins_node->index,
                                     pre_built->ins_node->entry,
                                     pre_built->ins_node->row);

        ut_ad(dtuple_check_typed(pre_built->ins_node->entry));

        cursor.thr = que_thr;

        err = lock_table(0, index->table, LOCK_IX, que_thr);
        if (err != DB_SUCCESS) {
            mtr_commit(&mtr);
            trx_commit_for_mysql(pre_built->trx);
            ok(0, "can not lock table");
            exit(1);
        }

        err = btr_cur_search_to_nth_level(
                index, 0, pre_built->ins_node->entry, PAGE_CUR_LE,
                BTR_MODIFY_LEAF, &cursor, 0,
                __FILE__, __LINE__, &mtr);

        if (err != DB_SUCCESS) {
            index->table->file_unreadable = true;
            mtr_commit(&mtr);
            trx_commit_for_mysql(pre_built->trx);
            ok(0, "search failed");
            exit(1);
        }

        err = btr_cur_optimistic_insert(0, &cursor, &offsets, &heap,
                                        pre_built->ins_node->entry, &rec,
                                        &big_rec, 0, que_thr, &mtr);

        if (err == DB_FAIL) {
            err = btr_cur_search_to_nth_level(
                    index, 0, pre_built->ins_node->entry, PAGE_CUR_LE,
                    BTR_MODIFY_TREE, &cursor, 0,
                    __FILE__, __LINE__, &mtr);

            if (err != DB_SUCCESS) {
                index->table->file_unreadable = true;
                mtr_commit(&mtr);
                trx_commit_for_mysql(pre_built->trx);
                ok(0, "second search failed");
                exit(1);
            }

            err = btr_cur_optimistic_insert(0, &cursor, &offsets, &heap,
                                            pre_built->ins_node->entry, &rec,
                                            &big_rec, 0, que_thr, &mtr);

            if (err == DB_FAIL) {
                err = btr_cur_pessimistic_insert(0, &cursor, &offsets, &heap,
                                                 pre_built->ins_node->entry, &rec,
                                                 &big_rec, 0, que_thr, &mtr);
            }
        }

        if (err != DB_SUCCESS) {
            mtr_commit(&mtr);
            trx_commit_for_mysql(pre_built->trx);
            ok(0, "insert failed");
            exit(1);
        }

        ut_memcpy(pre_built->row_id, pre_built->ins_node->row_id_buf, DATA_ROW_ID_LEN);
        mtr_commit(&mtr);
        trx_commit_for_mysql(pre_built->trx);
    }

    std::time_t end_time = time(nullptr);
    std::cout << "\ntime taken for insertion: " << (end_time - start_time) / 60 << " m "
              << (end_time - start_time) % 60 << " s\n";

    ok(err == dberr_t::DB_SUCCESS, "insert successful");
    return pre_built;
}

std::vector<ulint> prepare_data(ulint length) {
    // generate random numbers
//    ulint seed = 42;
    std::vector<ulint> entries(length);

    for (ulint i = 0; i < length; i++) {
        entries[i] = i+1;
    }

//    std::shuffle(entries.begin(), entries.end(), std::default_random_engine(seed));

    return entries;
}

void test_search(row_prebuilt_t* pre_built, std::vector<ulint> entries) {

    ulint key_len = 4;
    ulint mode = PAGE_CUR_GE;   //HA_READ_KEY_EXACT
    ulint match_mode = ROW_SEL_EXACT;
    ulint data_len = entries.size();
    uchar* buf = (unsigned char*)"\xff\x00\x00\x00\x00\x00\x00\x00\x00";
    dberr_t ret;
    trx_t *user_trx = trx_allocate_for_background();

    pre_built->trx = user_trx;

    dict_index_t* index = pre_built->index;

    if (UNIV_UNLIKELY(index == NULL) || dict_index_is_corrupted(index)) {
        pre_built->index_usable = FALSE;
        ok(0, "HA_ERR_CRASHED");
        exit(1);
    }

    if (UNIV_UNLIKELY(!pre_built->index_usable)) {
        ok(0, "HA_ERR_TABLE_DEF_CHANGED or HA_ERR_INDEX_CORRUPT");
        exit(1);
    }

    if (index->type & DICT_FTS) {
        ok(0, "HA_ERR_KEY_NOT_FOUND");
    }

    pre_built->need_to_access_clustered = 1;

    for(ulint j=0;j<pre_built->n_template;j++) {
        mysql_row_templ_t* templ = pre_built->mysql_template + j;
        if (pre_built->mysql_prefix_len < templ->mysql_col_offset
                                          + templ->mysql_col_len) {
            pre_built->mysql_prefix_len = templ->mysql_col_offset
                                          + templ->mysql_col_len;
        }
    }

    dtuple_set_n_fields(pre_built->search_tuple, pre_built->index->n_fields);

    dict_index_copy_types(pre_built->search_tuple, pre_built->index,
                          pre_built->index->n_fields);
    pre_built->search_tuple->fields->type.prtype = 1283;

    std::time_t start_time = time(nullptr);

    for (ulint i=0;i<data_len;i++) {

        trx_start_if_not_started_xa(pre_built->trx);

        unsigned char key_ptr[4];
        for (ulint l = 0; l < 4; l++)
            key_ptr[l] = (entries[i] >> (l * 8));

        row_sel_convert_mysql_key_to_innobase(
                pre_built->search_tuple,
                pre_built->srch_key_val1,
                pre_built->srch_key_val_len,
                index,
                (byte*) key_ptr,
                key_len,
                pre_built->trx);
        ut_ad(pre_built->search_tuple->n_fields > 0);
//      innobase_srv_conc_enter_innodb(pre_built->trx);

        ret = row_search_for_mysql((byte*) buf, mode, pre_built, match_mode, 0);
        ut_ad(ret==dberr_t::DB_SUCCESS);

//    innobase_srv_conc_exit_innodb(pre_built->trx);

    }
    std::time_t end_time = time(nullptr);
    std::cout << "\ntime taken for insertion: " << (end_time - start_time) / 60 << " m "
              << (end_time - start_time) % 60 << " s\n";

}

int main(int argc __attribute__((unused)), char *argv[]) {

    MY_INIT(argv[0]);

    // count is the number of tests to run in this file
    plan(3);

    // setup
    setup();

    // test1: create tablespace
    const char *table_name = "test";
    dict_index_t index;
    dict_table_t table;
//    create_table_and_index(&index, &table, (char *) table_name);
    create_table_and_index_with_primary_key(&index, &table, (char *) table_name);

    // test: insert operation
    ulint length = 10000;
    std::vector<ulint> entries = prepare_data(length);

    row_prebuilt_t* pre_built = test_insert(&index, &table, entries);
    pre_built->index = &index;
    pre_built->index_usable = TRUE;

    test_search(pre_built, entries);

    destroy();

    // delete the created tablespace file
    // always run it after destruction
// TODO: uncomment this ->   delete_tablespace_ibd_file((char *) table_name);

    my_end(0);
    return exit_status();
}
