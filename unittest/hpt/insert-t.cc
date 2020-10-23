//
// Created by aayushi on 07/10/20.
//

#include "init_n_des.h"
#include <dict0boot.h>
#include <que0que.h>
#include <trx0trx.h>
#include "utl_index.h"
#include "../../storage/xtradb/row/row0mysql.cc"
#include "../../storage/xtradb/row/row0ins.cc"
#include "../../storage/xtradb/row/row0sel.cc"
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

void test_create_table_index(dict_index_t *index, dict_table_t *table,
                            char *table_name) {

    // create table test (id int, name int);

    ulint type = DICT_CLUSTERED;
    ulint space = 150;
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

void test_create_table_index_with_primary_key(dict_index_t *index, dict_table_t *table,
                            char *table_name) {

    // create table test (id int primary key, name int);

    ulint type = DICT_CLUSTERED | DICT_UNIQUE;
    ulint space = 150;
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
    ulint seed = 42;
    std::vector<ulint> entries(length);

    for (ulint i = 0; i < length; i++) {
        entries[i] = i+1;
    }

    std::shuffle(entries.begin(), entries.end(), std::default_random_engine(seed));

    return entries;
}

dberr_t
search_index(byte* buf, ulint mode, row_prebuilt_t*	prebuilt,
            ulint match_mode, ulint direction)
{
    dict_index_t *index = prebuilt->index;
    ibool comp = dict_table_is_comp(index->table);
    const dtuple_t *search_tuple = prebuilt->search_tuple;
    btr_pcur_t *pcur = &prebuilt->pcur;
    trx_t *trx = prebuilt->trx;
    dict_index_t *clust_index;
    que_thr_t *thr;
    const rec_t *rec = NULL;
    const rec_t *result_rec;
    const rec_t *clust_rec;
    dberr_t err = DB_SUCCESS;
    ibool unique_search = FALSE;
    ibool mtr_has_extra_clust_latch = FALSE;
    ibool moves_up = FALSE;
    ibool set_also_gap_locks = TRUE;
    /* if the query is a plain locking SELECT, and the isolation level
    is <= TRX_ISO_READ_COMMITTED, then this is set to FALSE */
    ibool did_semi_consistent_read = FALSE;
    /* if the returned record was locked and we did a semi-consistent
    read (fetch the newest committed version), then this is set to
    TRUE */
#ifdef UNIV_SEARCH_DEBUG
    ulint		cnt				= 0;
#endif /* UNIV_SEARCH_DEBUG */
    ulint next_offs;
    ibool same_user_rec;
    mtr_t mtr;
    mem_heap_t *heap = NULL;
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    ulint *offsets = offsets_;
    ibool table_lock_waited = FALSE;

    rec_offs_init(offsets_);

    ut_ad(index && pcur && search_tuple);

    /* We don't support FTS queries from the HANDLER interfaces, because
    we implemented FTS as reversed inverted index with auxiliary tables.
    So anything related to traditional index query would not apply to
    it. */
    if (index->type & DICT_FTS) {
        return (DB_END_OF_INDEX);
    }

    ut_ad(!trx->has_search_latch);
#ifdef UNIV_SYNC_DEBUG
    ut_ad(!btr_search_own_any());
    ut_ad(!sync_thread_levels_nonempty_trx(trx->has_search_latch));
#endif /* UNIV_SYNC_DEBUG */

    if (dict_table_is_discarded(prebuilt->table)) {

        return (DB_TABLESPACE_DELETED);

    } else if (!prebuilt->table->is_readable()) {
        if (fil_space_get(prebuilt->table->space) == NULL) {
            return (DB_TABLESPACE_NOT_FOUND);
        } else {
            return (DB_DECRYPTION_FAILED);
        }
    } else if (!prebuilt->index_usable) {

        return (DB_MISSING_HISTORY);

    } else if (dict_index_is_corrupted(index)) {

        return (DB_CORRUPTION);

    } else if (prebuilt->magic_n != ROW_PREBUILT_ALLOCATED) {
        fprintf(stderr,
                "InnoDB: Error: trying to free a corrupt\n"
                "InnoDB: table handle. Magic n %lu, table name ",
                (ulong) prebuilt->magic_n);
        ut_print_name(stderr, trx, TRUE, prebuilt->table->name);
        putc('\n', stderr);

        mem_analyze_corruption(prebuilt);

        ut_error;
    }

#if 0
    /* August 19, 2005 by Heikki: temporarily disable this error
    print until the cursor lock count is done correctly.
    See bugs #12263 and #12456!*/

    if (trx->n_mysql_tables_in_use == 0
        && UNIV_UNLIKELY(prebuilt->select_lock_type == LOCK_NONE)) {
        /* Note that if MySQL uses an InnoDB temp table that it
        created inside LOCK TABLES, then n_mysql_tables_in_use can
        be zero; in that case select_lock_type is set to LOCK_X in
        ::start_stmt. */

        fputs("InnoDB: Error: MySQL is trying to perform a SELECT\n"
              "InnoDB: but it has not locked"
              " any tables in ::external_lock()!\n",
              stderr);
        trx_print(stderr, trx, 600);
        fputc('\n', stderr);
    }
#endif

#if 0
    fprintf(stderr, "Match mode %lu\n search tuple ",
        (ulong) match_mode);
    dtuple_print(search_tuple);
    fprintf(stderr, "N tables locked %lu\n",
        (ulong) trx->mysql_n_tables_locked);
#endif
    /* Reset the new record lock info if srv_locks_unsafe_for_binlog
    is set or session is using a READ COMMITED isolation level. Then
    we are able to remove the record locks set here on an individual
    row. */
    prebuilt->new_rec_locks = 0;

    /*-------------------------------------------------------------*/
    /* PHASE 1: Try to pop the row from the prefetch cache */

    if (UNIV_UNLIKELY(direction == 0)) {
        trx->op_info = "starting index read";

        prebuilt->n_rows_fetched = 0;
        prebuilt->n_fetch_cached = 0;
        prebuilt->fetch_cache_first = 0;

        if (prebuilt->sel_graph == NULL) {
            /* Build a dummy select query graph */
            row_prebuild_sel_graph(prebuilt);
        }
    }

    /* In a search where at most one record in the index may match, we
    can use a LOCK_REC_NOT_GAP type record lock when locking a
    non-delete-marked matching record.

    Note that in a unique secondary index there may be different
    delete-marked versions of a record where only the primary key
    values differ: thus in a secondary index we must use next-key
    locks when locking delete-marked records. */

    if (match_mode == ROW_SEL_EXACT
        && dict_index_is_unique(index)
        && dtuple_get_n_fields(search_tuple)
           == dict_index_get_n_unique(index)
        && (dict_index_is_clust(index)
            || !dtuple_contains_null(search_tuple))) {

        /* Note above that a UNIQUE secondary index can contain many
        rows with the same key value if one of the columns is the SQL
        null. A clustered index under MySQL can never contain null
        columns because we demand that all the columns in primary key
        are non-null. */

            unique_search = TRUE;
        }

    mtr_start(&mtr);

    /*-------------------------------------------------------------*/
    /* PHASE 2: Try fast adaptive hash index search if possible */

    /* Next test if this is the special case where we can use the fast
    adaptive hash index to try the search. Since we must release the
    search system latch when we retrieve an externally stored field, we
    cannot use the adaptive hash index in a search in the case the row
    may be long and there may be externally stored fields */

    if (UNIV_UNLIKELY(direction == 0)
        && unique_search
        && dict_index_is_clust(index)
        && !prebuilt->templ_contains_blob
        && !prebuilt->used_in_HANDLER
        && (prebuilt->mysql_row_len < UNIV_PAGE_SIZE / 8)
        && !prebuilt->innodb_api) {

        mode = PAGE_CUR_GE;
    }

    /*-------------------------------------------------------------*/
    /* PHASE 3: Open or restore index cursor position */

    ut_ad(!trx->has_search_latch);
#ifdef UNIV_SYNC_DEBUG
    ut_ad(!btr_search_own_any());
#endif

    /* The state of a running trx can only be changed by the
    thread that is currently serving the transaction. Because we
    are that thread, we can read trx->state without holding any
    mutex. */
    ut_ad(prebuilt->sql_stat_start || trx->state == TRX_STATE_ACTIVE);

    ut_ad(trx->state == TRX_STATE_NOT_STARTED
          || trx->state == TRX_STATE_ACTIVE);

    ut_ad(prebuilt->sql_stat_start
          || prebuilt->select_lock_type != LOCK_NONE
          || trx->read_view);

    trx_start_if_not_started(trx);

    if (trx->isolation_level <= TRX_ISO_READ_COMMITTED
        && prebuilt->select_lock_type != LOCK_NONE
        && trx->mysql_thd != NULL
        && thd_is_select(trx->mysql_thd)) {
        /* It is a plain locking SELECT and the isolation
        level is low: do not lock gaps */

        set_also_gap_locks = FALSE;
    }

    /* Note that if the search mode was GE or G, then the cursor
    naturally moves upward (in fetch next) in alphabetical order,
    otherwise downward */

    if (UNIV_UNLIKELY(direction == 0)) {
        if (mode == PAGE_CUR_GE || mode == PAGE_CUR_G) {
            moves_up = TRUE;
        }
    } else if (direction == ROW_SEL_NEXT) {
        moves_up = TRUE;
    }

    thr = que_fork_get_first_thr(prebuilt->sel_graph);

    que_thr_move_to_run_state_for_mysql(thr, trx);

    clust_index = dict_table_get_first_index(index->table);

    /* Do some start-of-statement preparations */

    if (!prebuilt->sql_stat_start) {
        /* No need to set an intention lock or assign a read view */

        if (UNIV_UNLIKELY
        (trx->read_view == NULL
         && prebuilt->select_lock_type == LOCK_NONE)) {

            fputs("InnoDB: Error: MySQL is trying to"
                  " perform a consistent read\n"
                  "InnoDB: but the read view is not assigned!\n",
                  stderr);
            trx_print(stderr, trx, 600);
            fputc('\n', stderr);
            ut_error;
        }
    } else if (prebuilt->select_lock_type == LOCK_NONE) {
        /* This is a consistent read */
        /* Assign a read view for the query */

        trx_assign_read_view(trx);
        prebuilt->sql_stat_start = FALSE;
    } else {
        wait_table_again:
        err = lock_table(0, index->table,
                         prebuilt->select_lock_type == LOCK_S
                         ? LOCK_IS : LOCK_IX, thr);

        if (err != DB_SUCCESS) {

            table_lock_waited = TRUE;
            goto lock_table_wait;
        }
        prebuilt->sql_stat_start = FALSE;
    }

    /* Open or restore index cursor position */

    if (UNIV_LIKELY(direction != 0)) {
        ibool need_to_process = sel_restore_position_for_mysql(
                &same_user_rec, BTR_SEARCH_LEAF,
                pcur, moves_up, &mtr);

        if (UNIV_UNLIKELY(need_to_process)) {
            if (UNIV_UNLIKELY(prebuilt->row_read_type
                              == ROW_READ_DID_SEMI_CONSISTENT)) {
                /* We did a semi-consistent read,
                but the record was removed in
                the meantime. */
                prebuilt->row_read_type
                        = ROW_READ_TRY_SEMI_CONSISTENT;
            }
        } else if (UNIV_LIKELY(prebuilt->row_read_type
                               != ROW_READ_DID_SEMI_CONSISTENT)) {

            /* The cursor was positioned on the record
            that we returned previously.  If we need
            to repeat a semi-consistent read as a
            pessimistic locking read, the record
            cannot be skipped. */

            goto next_rec;
        }

    } else if (dtuple_get_n_fields(search_tuple) > 0) {

        err = btr_pcur_open_with_no_init(index, search_tuple, mode,
                                         BTR_SEARCH_LEAF,
                                         pcur, 0, &mtr);

        if (err != DB_SUCCESS) {
            rec = NULL;
            goto lock_wait_or_error;
        }

        pcur->trx_if_known = trx;

        rec = btr_pcur_get_rec(pcur);

        if (!moves_up
            && !page_rec_is_supremum(rec)
            && set_also_gap_locks
            && !(srv_locks_unsafe_for_binlog
                 || trx->isolation_level <= TRX_ISO_READ_COMMITTED)
            && prebuilt->select_lock_type != LOCK_NONE) {

            /* Try to place a gap lock on the next index record
            to prevent phantoms in ORDER BY ... DESC queries */
            const rec_t *next_rec = page_rec_get_next_const(rec);

            offsets = rec_get_offsets(next_rec, index, offsets,
                                      ULINT_UNDEFINED, &heap);
            err = sel_set_rec_lock(btr_pcur_get_block(pcur),
                                   next_rec, index, offsets,
                                   prebuilt->select_lock_type,
                                   LOCK_GAP, thr);

            switch (err) {
                case DB_SUCCESS_LOCKED_REC:
                    err = DB_SUCCESS;
                    /* fall through */
                case DB_SUCCESS:
                    break;
                default:
                    goto lock_wait_or_error;
            }
        }
    } else if (mode == PAGE_CUR_G || mode == PAGE_CUR_L) {
        err = btr_pcur_open_at_index_side(
                mode == PAGE_CUR_G, index, BTR_SEARCH_LEAF,
                pcur, false, 0, &mtr);

        if (err != DB_SUCCESS) {
            if (err == DB_DECRYPTION_FAILED) {
                ib_push_warning(trx->mysql_thd,
                                DB_DECRYPTION_FAILED,
                                "Table %s is encrypted but encryption service or"
                                " used key_id is not available. "
                                " Can't continue reading table.",
                                prebuilt->table->name);
                index->table->file_unreadable = true;
            }
            rec = NULL;
            goto lock_wait_or_error;
        }
    }

    rec_loop:
    DEBUG_SYNC_C("row_search_rec_loop");
    if (trx_is_interrupted(trx)) {
        btr_pcur_store_position(pcur, &mtr);
        err = DB_INTERRUPTED;
        goto normal_return;
    }

    /*-------------------------------------------------------------*/
    /* PHASE 4: Look for matching records in a loop */

    rec = btr_pcur_get_rec(pcur);

    if (!index->table->is_readable()) {
        err = DB_DECRYPTION_FAILED;
        goto lock_wait_or_error;
    }

    SRV_CORRUPT_TABLE_CHECK(rec,
                            {
                                err = DB_CORRUPTION;
                                goto lock_wait_or_error;
                            });

    ut_ad(!!page_rec_is_comp(rec) == comp);
#ifdef UNIV_SEARCH_DEBUG
    /*
    fputs("Using ", stderr);
    dict_index_name_print(stderr, trx, index);
    fprintf(stderr, " cnt %lu ; Page no %lu\n", cnt,
    page_get_page_no(page_align(rec)));
    rec_print(stderr, rec, index);
    printf("delete-mark: %lu\n",
           rec_get_deleted_flag(rec, page_rec_is_comp(rec)));
    */
#endif /* UNIV_SEARCH_DEBUG */

    if (page_rec_is_infimum(rec)) {

        /* The infimum record on a page cannot be in the result set,
        and neither can a record lock be placed on it: we skip such
        a record. */

        goto next_rec;
    }

    if (page_rec_is_supremum(rec)) {
        if (set_also_gap_locks
            && !(srv_locks_unsafe_for_binlog
                 || trx->isolation_level <= TRX_ISO_READ_COMMITTED)
            && prebuilt->select_lock_type != LOCK_NONE) {

            /* Try to place a lock on the index record */

            /* If innodb_locks_unsafe_for_binlog option is used
            or this session is using a READ COMMITTED or lower isolation
            level we do not lock gaps. Supremum record is really
            a gap and therefore we do not set locks there. */

            offsets = rec_get_offsets(rec, index, offsets,
                                      ULINT_UNDEFINED, &heap);
            err = sel_set_rec_lock(btr_pcur_get_block(pcur),
                                   rec, index, offsets,
                                   prebuilt->select_lock_type,
                                   LOCK_ORDINARY, thr);

            switch (err) {
                case DB_SUCCESS_LOCKED_REC:
                    err = DB_SUCCESS;
                    /* fall through */
                case DB_SUCCESS:
                    break;
                default:
                    goto lock_wait_or_error;
            }
        }
        /* A page supremum record cannot be in the result set: skip
        it now that we have placed a possible lock on it */

        goto next_rec;
    }

    /*-------------------------------------------------------------*/
    /* Do sanity checks in case our cursor has bumped into page
    corruption */

    if (comp) {
        next_offs = rec_get_next_offs(rec, TRUE);
        if (UNIV_UNLIKELY(next_offs < PAGE_NEW_SUPREMUM)) {

            goto wrong_offs;
        }
    } else {
        next_offs = rec_get_next_offs(rec, FALSE);
        if (UNIV_UNLIKELY(next_offs < PAGE_OLD_SUPREMUM)) {

            goto wrong_offs;
        }
    }

    if (UNIV_UNLIKELY(next_offs >= UNIV_PAGE_SIZE - PAGE_DIR)) {

        wrong_offs:
        if (srv_pass_corrupt_table && index->table->space != 0 &&
            index->table->space < SRV_LOG_SPACE_FIRST_ID) {
            index->table->file_unreadable = TRUE;
            fil_space_set_corrupt(index->table->space);
        }

        if ((srv_force_recovery == 0 || moves_up == FALSE)
            && srv_pass_corrupt_table <= 1) {
            ut_print_timestamp(stderr);
            buf_page_print(page_align(rec), 0);
            fprintf(stderr,
                    "\nInnoDB: rec address %p,"
                    " buf block fix count %lu\n",
                    (void *) rec, (ulong)
                            btr_cur_get_block(btr_pcur_get_btr_cur(pcur))
                                    ->page.buf_fix_count);
            fprintf(stderr,
                    "InnoDB: Index corruption: rec offs %lu"
                    " next offs %lu, page no %lu,\n"
                    "InnoDB: ",
                    (ulong) page_offset(rec),
                    (ulong) next_offs,
                    (ulong) page_get_page_no(page_align(rec)));
            dict_index_name_print(stderr, trx, index);
            fputs(". Run CHECK TABLE. You may need to\n"
                  "InnoDB: restore from a backup, or"
                  " dump + drop + reimport the table.\n",
                  stderr);
            ut_ad(0);
            err = DB_CORRUPTION;

            goto lock_wait_or_error;
        } else {
            /* The user may be dumping a corrupt table. Jump
            over the corruption to recover as much as possible. */

            fprintf(stderr,
                    "InnoDB: Index corruption: rec offs %lu"
                    " next offs %lu, page no %lu,\n"
                    "InnoDB: ",
                    (ulong) page_offset(rec),
                    (ulong) next_offs,
                    (ulong) page_get_page_no(page_align(rec)));
            dict_index_name_print(stderr, trx, index);
            fputs(". We try to skip the rest of the page.\n",
                  stderr);

            btr_pcur_move_to_last_on_page(pcur, &mtr);

            goto next_rec;
        }
    }
    /*-------------------------------------------------------------*/

    /* Calculate the 'offsets' associated with 'rec' */

    ut_ad(fil_page_get_type(btr_pcur_get_page(pcur)) == FIL_PAGE_INDEX);
    ut_ad(btr_page_get_index_id(btr_pcur_get_page(pcur)) == index->id);

    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);

    if (UNIV_UNLIKELY(srv_force_recovery > 0
                      || (!index->table->is_readable() &&
                          srv_pass_corrupt_table == 2))) {
        if (!rec_validate(rec, offsets)
            || !btr_index_rec_validate(rec, index, FALSE)) {
            char buf[MAX_FULL_NAME_LEN];
            ut_format_name(index->table->name, FALSE, buf, sizeof(buf));

            ib_logf(IB_LOG_LEVEL_ERROR,
                    "Index %s corrupted: rec offs " ULINTPF
                    " next offs " ULINTPF
                    ", page no " ULINTPF " ."
                    " We try to skip the record.",
                    buf,
                    page_offset(rec),
                    next_offs,
                    page_get_page_no(page_align(rec)));

            goto next_rec;
        }
    }

    /* Note that we cannot trust the up_match value in the cursor at this
    place because we can arrive here after moving the cursor! Thus
    we have to recompare rec and search_tuple to determine if they
    match enough. */

    if (match_mode == ROW_SEL_EXACT) {
        /* Test if the index record matches completely to search_tuple
        in prebuilt: if not, then we return with DB_RECORD_NOT_FOUND */

        /* fputs("Comparing rec and search tuple\n", stderr); */

        if (0 != cmp_dtuple_rec(search_tuple, rec, offsets)) {

            if (set_also_gap_locks
                && !(srv_locks_unsafe_for_binlog
                     || trx->isolation_level
                        <= TRX_ISO_READ_COMMITTED)
                && prebuilt->select_lock_type != LOCK_NONE) {

                /* Try to place a gap lock on the index
                record only if innodb_locks_unsafe_for_binlog
                option is not set or this session is not
                using a READ COMMITTED or lower isolation level. */

                err = sel_set_rec_lock(
                        btr_pcur_get_block(pcur),
                        rec, index, offsets,
                        prebuilt->select_lock_type, LOCK_GAP,
                        thr);

                switch (err) {
                    case DB_SUCCESS_LOCKED_REC:
                    case DB_SUCCESS:
                        break;
                    default:
                        goto lock_wait_or_error;
                }
            }

            btr_pcur_store_position(pcur, &mtr);

            /* The found record was not a match, but may be used
            as NEXT record (index_next). Set the relative position
            to BTR_PCUR_BEFORE, to reflect that the position of
            the persistent cursor is before the found/stored row
            (pcur->old_rec). */
            ut_ad(pcur->rel_pos == BTR_PCUR_ON);
            pcur->rel_pos = BTR_PCUR_BEFORE;

            err = DB_RECORD_NOT_FOUND;
#if 0
            ut_print_name(stderr, trx, FALSE, index->name);
            fputs(" record not found 3\n", stderr);
#endif

            goto normal_return;
        }

    } else if (match_mode == ROW_SEL_EXACT_PREFIX) {

        if (!cmp_dtuple_is_prefix_of_rec(search_tuple, rec, offsets)) {

            if (set_also_gap_locks
                && !(srv_locks_unsafe_for_binlog
                     || trx->isolation_level
                        <= TRX_ISO_READ_COMMITTED)
                && prebuilt->select_lock_type != LOCK_NONE) {

                /* Try to place a gap lock on the index
                record only if innodb_locks_unsafe_for_binlog
                option is not set or this session is not
                using a READ COMMITTED or lower isolation level. */

                err = sel_set_rec_lock(
                        btr_pcur_get_block(pcur),
                        rec, index, offsets,
                        prebuilt->select_lock_type, LOCK_GAP,
                        thr);

                switch (err) {
                    case DB_SUCCESS_LOCKED_REC:
                    case DB_SUCCESS:
                        break;
                    default:
                        goto lock_wait_or_error;
                }
            }

            btr_pcur_store_position(pcur, &mtr);

            /* The found record was not a match, but may be used
            as NEXT record (index_next). Set the relative position
            to BTR_PCUR_BEFORE, to reflect that the position of
            the persistent cursor is before the found/stored row
            (pcur->old_rec). */
            ut_ad(pcur->rel_pos == BTR_PCUR_ON);
            pcur->rel_pos = BTR_PCUR_BEFORE;

            err = DB_RECORD_NOT_FOUND;
#if 0
            ut_print_name(stderr, trx, FALSE, index->name);
            fputs(" record not found 4\n", stderr);
#endif

            goto normal_return;
        }
    }

    /* We are ready to look at a possible new index entry in the result
    set: the cursor is now placed on a user record */

    if (prebuilt->select_lock_type != LOCK_NONE) {
        /* Try to place a lock on the index record; note that delete
        marked records are a special case in a unique search. If there
        is a non-delete marked record, then it is enough to lock its
        existence with LOCK_REC_NOT_GAP. */

        /* If innodb_locks_unsafe_for_binlog option is used
        or this session is using a READ COMMITED isolation
        level we lock only the record, i.e., next-key locking is
        not used. */

        ulint lock_type;

        if (srv_locks_unsafe_for_binlog
            || trx->isolation_level <= TRX_ISO_READ_COMMITTED) {
            /* At READ COMMITTED or READ UNCOMMITTED
            isolation levels, do not lock committed
            delete-marked records. */
            if (!rec_get_deleted_flag(rec, comp)) {
                goto no_gap_lock;
            }
            if (trx_id_t trx_id = index == clust_index
                                  ? row_get_rec_trx_id(rec, index, offsets)
                                  : row_vers_impl_x_locked(rec, index, offsets)) {
                if (trx_rw_is_active(trx_id, NULL)) {
                    /* The record belongs to an active
                    transaction. We must acquire a lock. */
                    goto no_gap_lock;
                }
            }
            goto locks_ok_del_marked;
        }

        if (!set_also_gap_locks
            || (unique_search && !rec_get_deleted_flag(rec, comp))) {

            goto no_gap_lock;
        } else {
            lock_type = LOCK_ORDINARY;
        }

        /* If we are doing a 'greater or equal than a primary key
        value' search from a clustered index, and we find a record
        that has that exact primary key value, then there is no need
        to lock the gap before the record, because no insert in the
        gap can be in our search range. That is, no phantom row can
        appear that way.

        An example: if col1 is the primary key, the search is WHERE
        col1 >= 100, and we find a record where col1 = 100, then no
        need to lock the gap before that record. */

        if (index == clust_index
            && mode == PAGE_CUR_GE
            && direction == 0
            && dtuple_get_n_fields_cmp(search_tuple)
               == dict_index_get_n_unique(index)
            && 0 == cmp_dtuple_rec(search_tuple, rec, offsets)) {
            no_gap_lock:
            lock_type = LOCK_REC_NOT_GAP;
        }

        err = sel_set_rec_lock(btr_pcur_get_block(pcur),
                               rec, index, offsets,
                               prebuilt->select_lock_type,
                               lock_type, thr);

        switch (err) {
            const rec_t *old_vers;
            case DB_SUCCESS_LOCKED_REC:
                if (srv_locks_unsafe_for_binlog
                    || trx->isolation_level
                       <= TRX_ISO_READ_COMMITTED) {
                    /* Note that a record of
                    prebuilt->index was locked. */
                    prebuilt->new_rec_locks = 1;
                }
                err = DB_SUCCESS;
                /* fall through */
            case DB_SUCCESS:
                break;
            case DB_LOCK_WAIT:
                /* Never unlock rows that were part of a conflict. */
                prebuilt->new_rec_locks = 0;

                if (UNIV_LIKELY(prebuilt->row_read_type
                                != ROW_READ_TRY_SEMI_CONSISTENT)
                    || unique_search
                    || index != clust_index) {

                    goto lock_wait_or_error;
                }

                /* The following call returns 'offsets'
                associated with 'old_vers' */
                row_sel_build_committed_vers_for_mysql(
                        clust_index, prebuilt, rec,
                        &offsets, &heap, &old_vers, &mtr);

                /* Check whether it was a deadlock or not, if not
                a deadlock and the transaction had to wait then
                release the lock it is waiting on. */

                lock_mutex_enter();
                trx_mutex_enter(trx);
                err = lock_trx_handle_wait(trx);
                lock_mutex_exit();
                trx_mutex_exit(trx);

                switch (err) {
                    case DB_SUCCESS:
                        /* The lock was granted while we were
                        searching for the last committed version.
                        Do a normal locking read. */

                        offsets = rec_get_offsets(
                                rec, index, offsets, ULINT_UNDEFINED,
                                &heap);
                        goto locks_ok;
                    case DB_DEADLOCK:
                        goto lock_wait_or_error;
                    case DB_LOCK_WAIT:
                        err = DB_SUCCESS;
                        break;
                    default:
                        ut_error;
                }

                if (old_vers == NULL) {
                    /* The row was not yet committed */

                    goto next_rec;
                }

                did_semi_consistent_read = TRUE;
                rec = old_vers;
                break;
            default:

                goto lock_wait_or_error;
        }
    } else {
        /* This is a non-locking consistent read: if necessary, fetch
        a previous version of the record */

        if (trx->isolation_level == TRX_ISO_READ_UNCOMMITTED) {

            /* Do nothing: we let a non-locking SELECT read the
            latest version of the record */

        } else if (index == clust_index) {

            /* Fetch a previous version of the row if the current
            one is not visible in the snapshot; if we have a very
            high force recovery level set, we try to avoid crashes
            by skipping this lookup */

            if (UNIV_LIKELY(srv_force_recovery < 5)
                && !lock_clust_rec_cons_read_sees(
                    rec, index, offsets, trx->read_view)) {

                rec_t *old_vers;
                /* The following call returns 'offsets'
                associated with 'old_vers' */
                err = row_sel_build_prev_vers_for_mysql(
                        trx->read_view, clust_index,
                        prebuilt, rec, &offsets, &heap,
                        &old_vers, &mtr);

                if (err != DB_SUCCESS) {

                    goto lock_wait_or_error;
                }

                if (old_vers == NULL) {
                    /* The row did not exist yet in
                    the read view */

                    goto next_rec;
                }

                rec = old_vers;
            }
        } else {
            /* We are looking into a non-clustered index,
            and to get the right version of the record we
            have to look also into the clustered index: this
            is necessary, because we can only get the undo
            information via the clustered index record. */

            ut_ad(!dict_index_is_clust(index));

            if (!lock_sec_rec_cons_read_sees(
                    rec, trx->read_view)) {
                /* We should look at the clustered index.
                However, as this is a non-locking read,
                we can skip the clustered index lookup if
                the condition does not match the secondary
                index entry. */
                switch (row_search_idx_cond_check(
                        buf, prebuilt, rec, offsets)) {
                    case ICP_NO_MATCH:
                        goto next_rec;
                    case ICP_OUT_OF_RANGE:
                        err = DB_RECORD_NOT_FOUND;
                        goto idx_cond_failed;
                    case ICP_ABORTED_BY_USER:
                        err = DB_SEARCH_ABORTED_BY_USER;
                        goto idx_cond_failed;
                    case ICP_ERROR:
                        err = DB_ERROR;
                        goto idx_cond_failed;
                    case ICP_MATCH:
                        goto requires_clust_rec;
                }

                ut_error;
            }
        }
    }

    locks_ok:
    /* NOTE that at this point rec can be an old version of a clustered
    index record built for a consistent read. We cannot assume after this
    point that rec is on a buffer pool page. Functions like
    page_rec_is_comp() cannot be used! */

    if (rec_get_deleted_flag(rec, comp)) {
        locks_ok_del_marked:
        /* The record is delete-marked: we can skip it */

        /* This is an optimization to skip setting the next key lock
        on the record that follows this delete-marked record. This
        optimization works because of the unique search criteria
        which precludes the presence of a range lock between this
        delete marked record and the record following it.

        For now this is applicable only to clustered indexes while
        doing a unique search except for HANDLER queries because
        HANDLER allows NEXT and PREV even in unique search on
        clustered index. There is scope for further optimization
        applicable to unique secondary indexes. Current behaviour is
        to widen the scope of a lock on an already delete marked record
        if the same record is deleted twice by the same transaction */
        if (index == clust_index && unique_search
            && !prebuilt->used_in_HANDLER) {

            err = DB_RECORD_NOT_FOUND;

            goto normal_return;
        }

        goto next_rec;
    }

    /* Check if the record matches the index condition. */
    switch (row_search_idx_cond_check(buf, prebuilt, rec, offsets)) {
        case ICP_NO_MATCH:
            if (did_semi_consistent_read) {
                row_unlock_for_mysql(prebuilt, TRUE);
            }
            goto next_rec;
        case ICP_ABORTED_BY_USER:
            err = DB_SEARCH_ABORTED_BY_USER;
            goto idx_cond_failed;
        case ICP_ERROR:
            err = DB_ERROR;
            goto idx_cond_failed;
        case ICP_OUT_OF_RANGE:
            err = DB_RECORD_NOT_FOUND;
            goto idx_cond_failed;
        case ICP_MATCH:
            break;
    }

    if (index != clust_index && prebuilt->need_to_access_clustered) {
        if (row_search_with_covering_prefix(prebuilt, rec, offsets)) {
            goto use_covering_index;
        }
        requires_clust_rec:
        ut_ad(index != clust_index);
        /* We use a 'goto' to the preceding label if a consistent
        read of a secondary index record requires us to look up old
        versions of the associated clustered index record. */

        ut_ad(rec_offs_validate(rec, index, offsets));

        /* It was a non-clustered index and we must fetch also the
        clustered index record */

        mtr_has_extra_clust_latch = TRUE;

        /* The following call returns 'offsets' associated with
        'clust_rec'. Note that 'clust_rec' can be an old version
        built for a consistent read. */

        err = row_sel_get_clust_rec_for_mysql(prebuilt, index, rec,
                                              thr, &clust_rec,
                                              &offsets, &heap, &mtr);
        switch (err) {
            case DB_SUCCESS:
                if (clust_rec == NULL) {
                    /* The record did not exist in the read view */
                    ut_ad(prebuilt->select_lock_type == LOCK_NONE);

                    goto next_rec;
                }
                break;
            case DB_SUCCESS_LOCKED_REC:
                ut_a(clust_rec != NULL);
                if (srv_locks_unsafe_for_binlog
                    || trx->isolation_level
                       <= TRX_ISO_READ_COMMITTED) {
                    /* Note that the clustered index record
                    was locked. */
                    prebuilt->new_rec_locks = 2;
                }
                err = DB_SUCCESS;
                break;
            default:
                goto lock_wait_or_error;
        }

        if (rec_get_deleted_flag(clust_rec, comp)) {

            /* The record is delete marked: we can skip it */

            if ((srv_locks_unsafe_for_binlog
                 || trx->isolation_level <= TRX_ISO_READ_COMMITTED)
                && prebuilt->select_lock_type != LOCK_NONE) {

                /* No need to keep a lock on a delete-marked
                record if we do not want to use next-key
                locking. */

                row_unlock_for_mysql(prebuilt, TRUE);
            }

            goto next_rec;
        }

        result_rec = clust_rec;
        ut_ad(rec_offs_validate(result_rec, clust_index, offsets));
    } else{
        use_covering_index:
            result_rec = rec;
    }
    /* We found a qualifying record 'result_rec'. At this point,
	'offsets' are associated with 'result_rec'. */

    ut_ad(rec_offs_validate(result_rec,
                            result_rec != rec ? clust_index : index,
                            offsets));
    ut_ad(!rec_get_deleted_flag(result_rec, comp));

    /* At this point, the clustered index record is protected
    by a page latch that was acquired when pcur was positioned.
    The latch will not be released until mtr_commit(&mtr). */
    /* From this point on, 'offsets' are invalid. */
    /* We have an optimization to save CPU time: if this is a consistent
    read on a unique condition on the clustered index, then we do not
    store the pcur position, because any fetch next or prev will anyway
    return 'end of file'. Exceptions are locking reads and the MySQL
    HANDLER command where the user can move the cursor with PREV or NEXT
    even after a unique search. */
    if (prebuilt->n_fetch_cached < MYSQL_FETCH_CACHE_SIZE) {
        goto next_rec;
    }

    err = DB_SUCCESS;

    idx_cond_failed:
    if (!unique_search
        || !dict_index_is_clust(index)
        || direction != 0
        || prebuilt->select_lock_type != LOCK_NONE
        || prebuilt->used_in_HANDLER
        || prebuilt->innodb_api) {

        /* Inside an update always store the cursor position */

        btr_pcur_store_position(pcur, &mtr);

        if (prebuilt->innodb_api) {
            prebuilt->innodb_api_rec = result_rec;
        }
    }

    goto normal_return;

    next_rec:
    /* Reset the old and new "did semi-consistent read" flags. */
    if (UNIV_UNLIKELY(prebuilt->row_read_type
                      == ROW_READ_DID_SEMI_CONSISTENT)) {
        prebuilt->row_read_type = ROW_READ_TRY_SEMI_CONSISTENT;
    }
    did_semi_consistent_read = FALSE;
    prebuilt->new_rec_locks = 0;

    /*-------------------------------------------------------------*/
    /* PHASE 5: Move the cursor to the next index record */

    /* NOTE: For moves_up==FALSE, the mini-transaction will be
    committed and restarted every time when switching b-tree
    pages. For moves_up==TRUE in index condition pushdown, we can
    scan an entire secondary index tree within a single
    mini-transaction. As long as the prebuilt->idx_cond does not
    match, we do not need to consult the clustered index or
    return records to MySQL, and thus we can avoid repositioning
    the cursor. What prevents us from buffer-fixing all leaf pages
    within the mini-transaction is the btr_leaf_page_release()
    call in btr_pcur_move_to_next_page(). Only the leaf page where
    the cursor is positioned will remain buffer-fixed. */

    if (UNIV_UNLIKELY(mtr_has_extra_clust_latch)) {
        /* We must commit mtr if we are moving to the next
        non-clustered index record, because we could break the
        latching order if we would access a different clustered
        index page right away without releasing the previous. */

        btr_pcur_store_position(pcur, &mtr);

        mtr_commit(&mtr);
        mtr_has_extra_clust_latch = FALSE;

        mtr_start(&mtr);
        if (sel_restore_position_for_mysql(&same_user_rec,
                                           BTR_SEARCH_LEAF,
                                           pcur, moves_up, &mtr)) {
#ifdef UNIV_SEARCH_DEBUG
            cnt++;
#endif /* UNIV_SEARCH_DEBUG */

            goto rec_loop;
        }
    }

    if (moves_up) {
        if (UNIV_UNLIKELY(!btr_pcur_move_to_next(pcur, &mtr))) {
            not_moved:
            btr_pcur_store_position(pcur, &mtr);

            if (match_mode != 0) {
                err = DB_RECORD_NOT_FOUND;
            } else {
                err = DB_END_OF_INDEX;
            }

            goto normal_return;
        }
    } else {
        if (UNIV_UNLIKELY(!btr_pcur_move_to_prev(pcur, &mtr))) {
            goto not_moved;
        }
    }

#ifdef UNIV_SEARCH_DEBUG
        cnt++;
#endif /* UNIV_SEARCH_DEBUG */

    goto rec_loop;

    lock_wait_or_error:
    /* Reset the old and new "did semi-consistent read" flags. */
    if (UNIV_UNLIKELY(prebuilt->row_read_type
                      == ROW_READ_DID_SEMI_CONSISTENT)) {
        prebuilt->row_read_type = ROW_READ_TRY_SEMI_CONSISTENT;
    }
    did_semi_consistent_read = FALSE;

    /*-------------------------------------------------------------*/

    if (rec) {
        btr_pcur_store_position(pcur, &mtr);
    }

    lock_table_wait:
    mtr_commit(&mtr);
    mtr_has_extra_clust_latch = FALSE;

    trx->error_state = err;

    /* The following is a patch for MySQL */

    que_thr_stop_for_mysql(thr);

    thr->lock_state = QUE_THR_LOCK_ROW;

    if (row_mysql_handle_errors(&err, trx, thr, NULL)) {
        /* It was a lock wait, and it ended */

        thr->lock_state = QUE_THR_LOCK_NOLOCK;
        mtr_start(&mtr);

        /* Table lock waited, go try to obtain table lock
        again */
        if (table_lock_waited) {
            table_lock_waited = FALSE;

            goto wait_table_again;
        }

        sel_restore_position_for_mysql(&same_user_rec,
                                       BTR_SEARCH_LEAF, pcur,
                                       moves_up, &mtr);

        if ((srv_locks_unsafe_for_binlog
             || trx->isolation_level <= TRX_ISO_READ_COMMITTED)
            && !same_user_rec) {

            /* Since we were not able to restore the cursor
            on the same user record, we cannot use
            row_unlock_for_mysql() to unlock any records, and
            we must thus reset the new rec lock info. Since
            in lock0lock.cc we have blocked the inheriting of gap
            X-locks, we actually do not have any new record locks
            set in this case.

            Note that if we were able to restore on the 'same'
            user record, it is still possible that we were actually
            waiting on a delete-marked record, and meanwhile
            it was removed by purge and inserted again by some
            other user. But that is no problem, because in
            rec_loop we will again try to set a lock, and
            new_rec_lock_info in trx will be right at the end. */

            prebuilt->new_rec_locks = 0;
        }

        mode = pcur->search_mode;

        goto rec_loop;
    }

    thr->lock_state = QUE_THR_LOCK_NOLOCK;

#ifdef UNIV_SEARCH_DEBUG
    /*	fputs("Using ", stderr);
dict_index_name_print(stderr, index);
fprintf(stderr, " cnt %lu ret value %lu err\n", cnt, err); */
#endif /* UNIV_SEARCH_DEBUG */
    goto func_exit;

    normal_return:
    /*-------------------------------------------------------------*/
    que_thr_stop_for_mysql_no_error(thr, trx);

    mtr_commit(&mtr);

    if (prebuilt->idx_cond != 0) {

        /* When ICP is active we don't write to the MySQL buffer
        directly, only to buffers that are enqueued in the pre-fetch
        queue. We need to dequeue the first buffer and copy the contents
        to the record buffer that was passed in by MySQL. */

        if (prebuilt->n_fetch_cached > 0) {
            row_sel_dequeue_cached_row_for_mysql(buf, prebuilt);
            err = DB_SUCCESS;
        }

    } else {

        /* We may or may not have enqueued some buffers to the
        pre-fetch queue, but we definitely wrote to the record
        buffer passed to use by MySQL. */

        DEBUG_SYNC_C("row_search_cached_row");
        err = DB_SUCCESS;
    }

#ifdef UNIV_SEARCH_DEBUG
    /*	fputs("Using ", stderr);
dict_index_name_print(stderr, index);
fprintf(stderr, " cnt %lu ret value %lu err\n", cnt, err); */
#endif /* UNIV_SEARCH_DEBUG */

    func_exit:
    trx->op_info = "";
    if (UNIV_LIKELY_NULL(heap)) {
        mem_heap_free(heap);
    }

    /* Set or reset the "did semi-consistent read" flag on return.
    The flag did_semi_consistent_read is set if and only if
    the record being returned was fetched with a semi-consistent read. */
    ut_ad(prebuilt->row_read_type != ROW_READ_WITH_LOCKS
          || !did_semi_consistent_read);

    if (UNIV_UNLIKELY(prebuilt->row_read_type != ROW_READ_WITH_LOCKS)) {
        if (UNIV_UNLIKELY(did_semi_consistent_read)) {
            prebuilt->row_read_type = ROW_READ_DID_SEMI_CONSISTENT;
        } else {
            prebuilt->row_read_type = ROW_READ_TRY_SEMI_CONSISTENT;
        }
    }

    ut_ad(!trx->has_search_latch);
#ifdef UNIV_SYNC_DEBUG
    ut_ad(!btr_search_own_any());
    ut_ad(!sync_thread_levels_nonempty_trx(trx->has_search_latch));
#endif /* UNIV_SYNC_DEBUG */

    DEBUG_SYNC_C("innodb_row_search_for_mysql_exit");

    // we will exclude trx_id and roll_ptr data from result record for final output
    for(ulint i=0; i<4;i++)
        buf[i] = (byte) result_rec[i];
    for(ulint i=4; i<8;i++)
        buf[i] = (byte)result_rec[13+i];

    // convert sign bit to normal
    buf[0] = (byte) (buf[0] ^ 128);
    buf[4] = (byte) (buf[4] ^ 128);

    trx_commit_for_mysql(prebuilt->trx);

    return(err);
}


void test_search(row_prebuilt_t* pre_built, std::vector<ulint> entries) {

    ulint count=0;
    const ulint key_len = 4;
    ulint mode = PAGE_CUR_GE;   //HA_READ_KEY_EXACT
    ulint match_mode = ROW_SEL_EXACT;
    ulint data_len = entries.size();
    uchar buf[8] = {'\x00','\x00','\x00','\x00','\x00','\x00','\x00','\x00'};
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

        pre_built->sql_stat_start = TRUE;

        unsigned char key_ptr[key_len];
        for (ulint l = 0; l < key_len; l++)
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

        ret = search_index((byte*) buf, mode, pre_built, match_mode, 0);
        ut_ad(ret==dberr_t::DB_SUCCESS);

        ulint value1=0, value2=0;
        for (ulint j=0;j<key_len;j++)
            value1 |= buf[j] << (8*(3-j));

        for (ulint j=0;j<key_len;j++)
            value2 |= buf[j+key_len] << (8*(3-j));

        ut_ad(value1 == entries[i] && value2 == entries[i]*10);
        count++;

    }

    std::time_t end_time = time(nullptr);
    std::cout << "\ntime taken for reading: " << (end_time - start_time) / 60 << " m "
              << (end_time - start_time) % 60 << " s\nentries:"<< count<<"\n";

    ok(ret == dberr_t::DB_SUCCESS, "read successful");

}

int main(int argc __attribute__((unused)), char *argv[]) {

    MY_INIT(argv[0]);

    // count is the number of tests to run in this file
    plan(4);

    // setup
    setup(); //test with default page_size 4KB, 100M bufferpool size

    // test1: create tablespace
    const char *table_name = "test";
    dict_index_t index;
    dict_table_t table;
//    test_create_table_index(&index, &table, (char *) table_name);
    test_create_table_index_with_primary_key(&index, &table, (char *) table_name);

    // test: insert operation
    ulint length = 100000;
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
