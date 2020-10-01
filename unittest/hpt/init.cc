//
// Created by kadam on 30/09/20.
//

#include <univ.i>
#include <ut0crc32.h>
#include <my_default.h>
#include <mysqld.h>
#include <srv0srv.h>
#include <srv0start.h>
#include <btr0types.h>
#include <crc_glue.c>


my_bool innodb_inited= 0;

/* === xtrabackup specific options === */
char xtrabackup_real_target_dir[FN_REFLEN] = "./xtrabackup_backupfiles/";
char *xtrabackup_target_dir= xtrabackup_real_target_dir;
my_bool xtrabackup_version = FALSE;
my_bool xtrabackup_backup = FALSE;
my_bool xtrabackup_prepare = FALSE;
my_bool xtrabackup_copy_back = FALSE;
my_bool xtrabackup_move_back = FALSE;
my_bool xtrabackup_decrypt_decompress = FALSE;
my_bool xtrabackup_print_param = FALSE;

my_bool xtrabackup_export = FALSE;
my_bool xtrabackup_apply_log_only = FALSE;

longlong xtrabackup_use_memory = 100*1024*1024L;
my_bool xtrabackup_create_ib_logfile = FALSE;

uint opt_protocol;
long xtrabackup_throttle = 0; /* 0:unlimited */
lint io_ticket;
os_event_t wait_throttle = NULL;
os_event_t log_copying_stop = NULL;

char *xtrabackup_incremental = NULL;
lsn_t incremental_lsn;
lsn_t incremental_to_lsn;
lsn_t incremental_last_lsn;
xb_page_bitmap *changed_page_bitmap = NULL;

char *xtrabackup_incremental_basedir = NULL; /* for --backup */
char *xtrabackup_extra_lsndir = NULL; /* for --backup with --extra-lsndir */
char *xtrabackup_incremental_dir = NULL; /* for --prepare */

char xtrabackup_real_incremental_basedir[FN_REFLEN];
char xtrabackup_real_extra_lsndir[FN_REFLEN];
char xtrabackup_real_incremental_dir[FN_REFLEN];

char *xtrabackup_tmpdir;

char *xtrabackup_tables = NULL;
char *xtrabackup_tables_file = NULL;
char *xtrabackup_tables_exclude = NULL;

typedef std::list<regex_t> regex_list_t;
static regex_list_t regex_include_list;
static regex_list_t regex_exclude_list;

static hash_table_t* tables_include_hash = NULL;
static hash_table_t* tables_exclude_hash = NULL;

char *xtrabackup_databases = NULL;
char *xtrabackup_databases_file = NULL;
char *xtrabackup_databases_exclude = NULL;
static hash_table_t* databases_include_hash = NULL;
static hash_table_t* databases_exclude_hash = NULL;

static hash_table_t* inc_dir_tables_hash;

struct xb_filter_entry_struct{
  char*		name;
  ibool		has_tables;
  hash_node_t	name_hash;
};
typedef struct xb_filter_entry_struct	xb_filter_entry_t;

static ulint		thread_nr[SRV_MAX_N_IO_THREADS + 6];
static os_thread_id_t	thread_ids[SRV_MAX_N_IO_THREADS + 6];

lsn_t checkpoint_lsn_start;
lsn_t checkpoint_no_start;
lsn_t log_copy_scanned_lsn;
ibool log_copying = TRUE;
ibool log_copying_running = FALSE;
ibool io_watching_thread_running = FALSE;

ibool xtrabackup_logfile_is_renamed = FALSE;

int xtrabackup_parallel;

char *xtrabackup_stream_str = NULL;
xb_stream_fmt_t xtrabackup_stream_fmt = XB_STREAM_FMT_NONE;
ibool xtrabackup_stream = FALSE;

const char *xtrabackup_compress_alg = NULL;
ibool xtrabackup_compress = FALSE;
uint xtrabackup_compress_threads;
ulonglong xtrabackup_compress_chunk_size = 0;

/* sleep interval beetween log copy iterations in log copying thread
in milliseconds (default is 1 second) */
ulint xtrabackup_log_copy_interval = 1000;
static ulong max_buf_pool_modified_pct;

/* Ignored option (--log) for MySQL option compatibility */
char*	log_ignored_opt				= NULL;


extern my_bool opt_use_ssl;
my_bool opt_ssl_verify_server_cert;
my_bool opt_extended_validation;
my_bool opt_backup_encrypted;

/* === metadata of backup === */
#define XTRABACKUP_METADATA_FILENAME "xtrabackup_checkpoints"
char metadata_type[30] = ""; /*[full-backuped|log-applied|
			     full-prepared|incremental]*/
lsn_t metadata_from_lsn = 0;
lsn_t metadata_to_lsn = 0;
lsn_t metadata_last_lsn = 0;

#define XB_LOG_FILENAME "xtrabackup_logfile"

ds_file_t	*dst_log_file = NULL;

static char mysql_data_home_buff[2];

const char *defaults_group = "mysqld";

/* === static parameters in ha_innodb.cc */

#define HA_INNOBASE_ROWS_IN_TABLE 10000 /* to get optimization right */
#define HA_INNOBASE_RANGE_COUNT	  100

ulong 	innobase_large_page_size = 0;

/* The default values for the following, type long or longlong, start-up
parameters are declared in mysqld.cc: */

long innobase_additional_mem_pool_size = 1*1024*1024L;
long innobase_buffer_pool_awe_mem_mb = 0;
long innobase_file_io_threads = 4;
long innobase_read_io_threads = 4;
long innobase_write_io_threads = 4;
long innobase_force_recovery = 0;
long innobase_log_buffer_size = 1024*1024L;
long innobase_log_files_in_group = 2;
long innobase_open_files = 300L;

longlong innobase_page_size = (1LL << 14); /* 16KB */
static ulong innobase_log_block_size = 512;
char*	innobase_doublewrite_file = NULL;
char*	innobase_buffer_pool_filename = NULL;

longlong innobase_log_file_size = 48*1024*1024L;

/* The default values for the following char* start-up parameters
are determined in innobase_init below: */

char*	innobase_ignored_opt			= NULL;
char*	innobase_data_home_dir			= NULL;
char*	innobase_data_file_path 		= NULL;
/* The following has a misleading name: starting from 4.0.5, this also
affects Windows: */
char*	innobase_unix_file_flush_method		= NULL;

/* Below we have boolean-valued start-up parameters, and their default
values */

ulong	innobase_fast_shutdown			= 1;
my_bool innobase_use_doublewrite    = TRUE;
my_bool innobase_use_checksums      = TRUE;
my_bool innobase_use_large_pages    = FALSE;
my_bool	innobase_file_per_table			= FALSE;
my_bool innobase_locks_unsafe_for_binlog        = FALSE;
my_bool innobase_rollback_on_timeout		= FALSE;
my_bool innobase_create_status_file		= FALSE;
my_bool innobase_adaptive_hash_index		= TRUE;

static char *internal_innobase_data_file_path	= NULL;

/* The following counter is used to convey information to InnoDB
about server activity: in selects it is not sensible to call
srv_active_wake_master_thread after each fetch or search, we only do
it every INNOBASE_WAKE_INTERVAL'th step. */

#define INNOBASE_WAKE_INTERVAL	32
ulong	innobase_active_counter	= 0;


static char *xtrabackup_debug_sync = NULL;

my_bool xtrabackup_incremental_force_scan = FALSE;

/* The flushed lsn which is read from data files */
lsn_t	flushed_lsn= 0;

ulong xb_open_files_limit= 0;
char *xb_plugin_dir;
char *xb_plugin_load;
my_bool xb_close_files= FALSE;

/* Datasinks */
ds_ctxt_t       *ds_data     = NULL;
ds_ctxt_t       *ds_meta     = NULL;
ds_ctxt_t       *ds_redo     = NULL;

static bool	innobackupex_mode = false;

static long	innobase_log_files_in_group_save;
static char	*srv_log_group_home_dir_save;
static longlong	innobase_log_file_size_save;

/* String buffer used by --print-param to accumulate server options as they are
parsed from the defaults file */
static std::ostringstream print_param_str;

/* Set of specified parameters */
std::set<std::string> param_set;

static ulonglong global_max_value;

extern "C" sig_handler handle_fatal_signal(int sig);

my_bool opt_galera_info = FALSE;
my_bool opt_slave_info = FALSE;
my_bool opt_no_lock = FALSE;
my_bool opt_safe_slave_backup = FALSE;
my_bool opt_rsync = FALSE;
my_bool opt_force_non_empty_dirs = FALSE;
my_bool opt_noversioncheck = FALSE;
my_bool opt_no_backup_locks = FALSE;
my_bool opt_decompress = FALSE;
my_bool opt_remove_original = FALSE;

static const char *binlog_info_values[] = {"off", "lockless", "on", "auto",
                                           NullS};
static TYPELIB binlog_info_typelib = {array_elements(binlog_info_values)-1, "",
                                      binlog_info_values, NULL};
ulong opt_binlog_info;

char *opt_incremental_history_name = NULL;
char *opt_incremental_history_uuid = NULL;

char *opt_user = NULL;
char *opt_password = NULL;
char *opt_host = NULL;
char *opt_defaults_group = NULL;
char *opt_socket = NULL;
uint opt_port = 0;
char *opt_login_path = NULL;
char *opt_log_bin = NULL;

const char *query_type_names[] = { "ALL", "UPDATE", "SELECT", NullS};

TYPELIB query_type_typelib= {array_elements(query_type_names) - 1, "",
                             query_type_names, NULL};

ulong opt_lock_wait_query_type;
ulong opt_kill_long_query_type;

uint opt_kill_long_queries_timeout = 0;
uint opt_lock_wait_timeout = 0;
uint opt_lock_wait_threshold = 0;
uint opt_debug_sleep_before_unlock = 0;
uint opt_safe_slave_backup_timeout = 0;

const char *opt_history = NULL;


static inline size_t
get_bit_shift(size_t value)
{
  size_t shift;

  if (value == 0)
    return 0;

  for (shift = 0; !(value & 1); shift++) {
    value >>= 1;
  }
  return (value >> 1) ? 0 : shift;
}


static void init_tempdir(void){
  memset((G_PTR) &mysql_tmpdir_list, 0, sizeof(mysql_tmpdir_list));
  if (init_tmpdir(&mysql_tmpdir_list, opt_mysql_tmpdir)){
    // TODO: change to ok
    exit(EXIT_FAILURE);
  }
}

static void init_page_size(void){
  // set page size and page size shift
  srv_page_size = 0;
  srv_page_size_shift = 0;

  if (innobase_page_size != (1LL << 14)) {
    int n_shift = (int)get_bit_shift((ulint) innobase_page_size);

    if (n_shift >= 12 && n_shift <= UNIV_PAGE_SIZE_SHIFT_MAX) {
      srv_page_size_shift = n_shift;
      srv_page_size = 1 << n_shift;
      printf("InnoDB: The universal page size of the "
          "database is set to %lu.\n", srv_page_size);
    } else {
      printf("InnoDB: Error: invalid value of "
          "innobase_page_size: %lld", innobase_page_size);
      exit(EXIT_FAILURE);
    }
  } else {
    // set to default 16KB
    srv_page_size_shift = 14;
    srv_page_size = (1 << srv_page_size_shift);
  }
}

static void init_log_block_size(void){
  srv_log_block_size = 0;
  if (innobase_log_block_size != 512) {
    uint n_shift = (uint)get_bit_shift(innobase_log_block_size);;

    if (n_shift > 0) {
      srv_log_block_size = (ulint)(1LL << n_shift);
      printf("InnoDB: The log block size is set to %lu.\n",
          srv_log_block_size);
    }
  } else {
    srv_log_block_size = 512;
  }
  if (!srv_log_block_size) {
    printf("InnoDB: Error: %lu is not valid value for "
        "innodb_log_block_size.\n", innobase_log_block_size);
    exit(EXIT_FAILURE);
  }
}

static void init_innodb_data_home_dir(void){
  static char current_dir[3];		/* Set if using current lib */
  char *default_path;

  /* First calculate the default path for innodb_data_home_dir etc.,
  in case the user has not given any value.

  Note that when using the embedded server, the datadirectory is not
  necessarily the current directory of this program. */

  /* It's better to use current lib, to keep paths short */
  current_dir[0] = FN_CURLIB;
  current_dir[1] = FN_LIBCHAR;
  current_dir[2] = 0;
  default_path = current_dir;

  srv_data_home = (innobase_data_home_dir ? innobase_data_home_dir : default_path);
  printf("InnoDB: innodb_data_home_dir = %s\n", srv_data_home);
}


// TODO: understand srv_parse_data_file_paths_and_sizes func
// TODO: It reads the data from ibdata1 but ibdata1 might not exist before
static void load_data_files(void){
  /* Set InnoDB initialization parameters according to the values
	read from MySQL .cnf file */

  /*--------------- Data files -------------------------*/

  /* The default dir for data files is the datadir of MySQL */

  init_innodb_data_home_dir();


  /* Set default InnoDB data file size to 10 MB and let it be
    auto-extending. Thus users can use InnoDB in >= 4.0 without having
  to specify any startup options. */

  if (!innobase_data_file_path) {
    innobase_data_file_path = (char*) "ibdata1:10M:autoextend";
  }
  printf("InnoDB: innodb_data_file_path = %s\n",
      innobase_data_file_path);

  /* Since InnoDB edits the argument in the next call, we make another
  copy of it: */

  internal_innobase_data_file_path = strdup(innobase_data_file_path);

  ret = (my_bool) srv_parse_data_file_paths_and_sizes(
      internal_innobase_data_file_path);

  if (!ret){
    printf("ERROR: syntax error in internal_innobase_data_file_path, ");
    free(internal_innobase_data_file_path);
    internal_innobase_data_file_path = NULL;
    exit(EXIT_FAILURE);
  }
}

static void load_log_files(void){
  /* -------------- Log files ---------------------------*/

  /* The default dir for log files is the datadir of MySQL */

  srv_log_group_home_dir = srv_data_home;

  msg("InnoDB: innodb_log_group_home_dir = %s\n",
      srv_log_group_home_dir);

  srv_normalize_path_for_win(srv_log_group_home_dir);

  if (strchr(srv_log_group_home_dir, ';')) {
    printf("ERROR: syntax error in innodb_log_group_home_dir, ");
    // TODO: repeated code
    free(internal_innobase_data_file_path);
    internal_innobase_data_file_path = NULL;
    exit(EXIT_FAILURE);
  }
}

static void init_srv_variables(void){
  srv_adaptive_flushing = FALSE;
  srv_use_sys_malloc = TRUE;
  srv_file_format = 1; /* Barracuda */
  srv_max_file_format_at_startup = UNIV_FORMAT_MIN; /* on */

  srv_file_flush_method_str = innobase_unix_file_flush_method;

  srv_n_log_files = (ulint) innobase_log_files_in_group;
  srv_log_file_size = (ulint) innobase_log_file_size;
  printf("InnoDB: innodb_log_files_in_group = %ld\n",
      srv_n_log_files);
  printf("InnoDB: innodb_log_file_size = %lld\n",
      (long long int) srv_log_file_size);

  srv_log_buffer_size = (ulint) innobase_log_buffer_size;

  /* We set srv_pool_size here in units of 1 kB. InnoDB internally
  changes the value so that it becomes the number of database pages. */
  srv_buf_pool_size = (ulint) xtrabackup_use_memory;

  srv_mem_pool_size = (ulint) innobase_additional_mem_pool_size;

  srv_n_file_io_threads = (ulint) innobase_file_io_threads;
  srv_n_read_io_threads = (ulint) innobase_read_io_threads;
  srv_n_write_io_threads = (ulint) innobase_write_io_threads;

  srv_force_recovery = (ulint) innobase_force_recovery;

  srv_use_doublewrite_buf = (ibool) innobase_use_doublewrite;

  if (!innobase_use_checksums) {

    srv_checksum_algorithm = SRV_CHECKSUM_ALGORITHM_NONE;
  }

  btr_search_enabled = (char) innobase_adaptive_hash_index;
  btr_search_index_num = 1;

  os_use_large_pages = (ibool) innobase_use_large_pages;
  os_large_page_size = (ulint) innobase_large_page_size;

  static char default_dir[3] = "./";
  srv_arch_dir = default_dir;

  row_rollback_on_timeout = (ibool) innobase_rollback_on_timeout;

  srv_file_per_table = (my_bool) innobase_file_per_table;

  srv_locks_unsafe_for_binlog = (ibool) innobase_locks_unsafe_for_binlog;

  srv_max_n_open_files = (ulint) innobase_open_files;
  srv_innodb_status = (ibool) innobase_create_status_file;

  srv_print_verbose_log = 1;

  // Set AIO mode

  /* On 5.5+ srv_use_native_aio is TRUE by default. It is later reset
  if it is not supported by the platform in
  innobase_start_or_create_for_mysql(). As we don't call it in here,
  we have to duplicate checks from that function here. */

#ifdef __WIN__
  switch (os_get_os_version()) {
	case OS_WIN95:
	case OS_WIN31:
	case OS_WINNT:
		/* On Win 95, 98, ME, Win32 subsystem for Windows 3.1,
		and NT use simulated aio. In NT Windows provides async i/o,
		but when run in conjunction with InnoDB Hot Backup, it seemed
		to corrupt the data files. */

		srv_use_native_aio = FALSE;
		break;

	case OS_WIN2000:
	case OS_WINXP:
		/* On 2000 and XP, async IO is available. */
		srv_use_native_aio = TRUE;
		break;

	default:
		/* Vista and later have both async IO and condition variables */
		srv_use_native_aio = TRUE;
		srv_use_native_conditions = TRUE;
		break;
	}

#elif defined(LINUX_NATIVE_AIO)

  if (srv_use_native_aio) {
    ut_print_timestamp(stderr);
    printf("InnoDB: Using Linux native AIO\n");
  }
#else
        /* Currently native AIO is supported only on windows and linux
	and that also when the support is compiled in. In all other
	cases, we ignore the setting of innodb_use_native_aio. */
	srv_use_native_aio = FALSE;

#endif

  /* Assign the default value to srv_undo_dir if it's not specified, as
  my_getopt does not support default values for string options. We also
  ignore the option and override innodb_undo_directory on --prepare,
  because separate undo tablespaces are copied to the root backup
  directory. */

  if (!srv_undo_dir) {
    my_free(srv_undo_dir);
    srv_undo_dir = my_strdup(".", MYF(MY_FAE));
  }

  innodb_log_checksum_func_update(srv_log_checksum_algorithm);

}

// TODO: might not be needed
static void set_charset(void){
  /* Store the default charset-collation number of this MySQL
	installation */

  /* We cannot treat characterset here for now!! */
  data_mysql_default_charset_coll = (ulint)default_charset_info->number;

  ut_a(DATA_MYSQL_LATIN1_SWEDISH_CHARSET_COLL ==
       my_charset_latin1.number);
  ut_a(DATA_MYSQL_BINARY_CHARSET_COLL == my_charset_bin.number);

  /* Store the latin1_swedish_ci character ordering table to InnoDB. For
  non-latin1_swedish_ci charsets we use the MySQL comparison functions,
  and consequently we do not need to know the ordering internally in
  InnoDB. */

  ut_a(0 == strcmp(my_charset_latin1.name, "latin1_swedish_ci"));
  srv_latin1_ordering = my_charset_latin1.sort_order;

  //innobase_commit_concurrency_init_default();

  /* Since we in this module access directly the fields of a trx
  struct, and due to different headers and flags it might happen that
  mutex_t has a different size in this module and in InnoDB
  modules, we check at run time that the size is the same in
  these compilation modules. */
}

static void innodb_init_params(void){
  my_bool ret;

  init_tempdir();
  init_page_size();
  init_log_block_size();


  /* Check that values don't overflow on 32-bit systems. */
  if (sizeof(ulint) == 4) {
    if (xtrabackup_use_memory > UINT_MAX32) {
      printf("mariabackup: use-memory can't be over 4GB"
          " on 32-bit systems\n");
    }

    if (innobase_log_file_size > UINT_MAX32) {
      printf("mariabackup: innobase_log_file_size can't be "
          "over 4GB on 32-bit systemsi\n");
      return FALSE;
    }
  }

  os_innodb_umask = (ulint)0664;

  load_data_files();
  load_log_files();
  init_srv_variables();
  set_charset();

}


UNIV_INTERN
void
srv_general_init(void)
/*==================*/
{
  ut_mem_init();
  /* Reset the system variables in the recovery module. */
  recv_sys_var_init();
  os_sync_init();
  sync_init();
  mem_init(srv_mem_pool_size);
  que_init();
  row_mysql_init();
}


void init(){

  /* temporary setting of enough size */
  srv_page_size_shift = UNIV_PAGE_SIZE_SHIFT_MAX;
  srv_page_size = UNIV_PAGE_SIZE_MAX;

  srv_n_purge_threads = 1;
//  srv_read_only_mode = TRUE;
  srv_read_only_mode = FALSE;


//  srv_backup_mode = TRUE;
//  srv_close_files = (bool)xb_close_files;

  innodb_init_params();
  srv_normalize_init_values();


  if (srv_file_flush_method_str == NULL) {
    /* These are the default options */
    srv_unix_file_flush_method = SRV_UNIX_FSYNC;
  } else if (0 == ut_strcmp(srv_file_flush_method_str, "fsync")) {
    srv_unix_file_flush_method = SRV_UNIX_FSYNC;
  } else if (0 == ut_strcmp(srv_file_flush_method_str, "O_DSYNC")) {
    srv_unix_file_flush_method = SRV_UNIX_O_DSYNC;

  } else if (0 == ut_strcmp(srv_file_flush_method_str, "O_DIRECT")) {
    srv_unix_file_flush_method = SRV_UNIX_O_DIRECT;
    msg("InnoDB: using O_DIRECT\n");
  } else if (0 == ut_strcmp(srv_file_flush_method_str, "littlesync")) {
    srv_unix_file_flush_method = SRV_UNIX_LITTLESYNC;

  } else if (0 == ut_strcmp(srv_file_flush_method_str, "nosync")) {
    srv_unix_file_flush_method = SRV_UNIX_NOSYNC;
  } else if (0 == ut_strcmp(srv_file_flush_method_str, "ALL_O_DIRECT")) {
    srv_unix_file_flush_method = SRV_UNIX_ALL_O_DIRECT;
    msg("InnoDB: using ALL_O_DIRECT\n");
  } else if (0 == ut_strcmp(srv_file_flush_method_str,
                            "O_DIRECT_NO_FSYNC")) {
    srv_unix_file_flush_method = SRV_UNIX_O_DIRECT_NO_FSYNC;
    msg("InnoDB: using O_DIRECT_NO_FSYNC\n");
  } else {
    msg("InnoDB: Unrecognized value %s for "
        "innodb_flush_method\n", srv_file_flush_method_str);
    exit(EXIT_FAILURE);
  }

#ifdef _WIN32
  srv_win_file_flush_method = SRV_WIN_IO_UNBUFFERED;
  srv_use_native_aio = FALSE;
#endif

  if (srv_buf_pool_size >= 1000 * 1024 * 1024) {
    /* Here we still have srv_pool_size counted
    in kilobytes (in 4.0 this was in bytes)
    srv_boot() converts the value to
    pages; if buffer pool is less than 1000 MB,
    assume fewer threads. */
    srv_max_n_threads = 50000;

  } else if (srv_buf_pool_size >= 8 * 1024 * 1024) {

    srv_max_n_threads = 10000;
  } else {
    srv_max_n_threads = 1000;       /* saves several MB of memory,
                                                especially in 64-bit
                                                computers */
  }

  srv_general_init();
  ut_crc32_init();
  crc_init();

#ifdef WITH_INNODB_DISALLOW_WRITES
  srv_allow_writes_event = os_event_create();
  os_event_set(srv_allow_writes_event);
#endif

  {
    ibool	log_file_created;
    ibool	log_created	= FALSE;
    ibool	log_opened	= FALSE;
    ulint	err;
    ulint	i;

    xb_fil_io_init();

    log_init();

    lock_sys_create(srv_lock_table_size);

    for (i = 0; i < srv_n_log_files; i++) {
      err = open_or_create_log_file(FALSE, &log_file_created,
                                    log_opened, 0, i);
      if (err != DB_SUCCESS) {

        //return((int) err);
        exit(EXIT_FAILURE);
      }

      if (log_file_created) {
        log_created = TRUE;
      } else {
        log_opened = TRUE;
      }
      if ((log_opened && log_created)) {
        msg(
            "mariabackup: Error: all log files must be created at the same time.\n"
            "mariabackup: All log files must be created also in database creation.\n"
            "mariabackup: If you want bigger or smaller log files, shut down the\n"
            "mariabackup: database and make sure there were no errors in shutdown.\n"
            "mariabackup: Then delete the existing log files. Edit the .cnf file\n"
            "mariabackup: and start the database again.\n");

        //return(DB_ERROR);
        exit(EXIT_FAILURE);
      }
    }

    /* log_file_created must not be TRUE, if online */
    if (log_file_created) {
      msg("mariabackup: Something wrong with source files...\n");
      exit(EXIT_FAILURE);
    }

  }

//  /* Create logfiles for recovery from 'xtrabackup_logfile', before start InnoDB */
//  srv_max_n_threads = 1000;
//  srv_n_purge_threads = 1;
//  ut_mem_init();
//  /* temporally dummy value to avoid crash */
//  srv_page_size_shift = 14;
//  srv_page_size = (1 << srv_page_size_shift);
//  os_sync_init();
//  sync_init();
  os_io_init_simple();
//  mem_init(srv_mem_pool_size);
//  ut_crc32_init();


}


static
void
innodb_free_param()
{
  srv_free_paths_and_sizes();
  free(internal_innobase_data_file_path);
  internal_innobase_data_file_path = NULL;
  free_tmpdir(&mysql_tmpdir_list);
}


void destroy(){
  mem_close();
  ut_free_all_mem();

  innodb_free_param();
  sync_close();
  sync_initialized = FALSE;

  if(srv_n_file_io_threads < 10) {
    srv_n_read_io_threads = 4;
    srv_n_write_io_threads = 4;
  }


  innodb_end();

  innodb_free_param();

  sync_initialized = FALSE;

  /* re-init necessary components */
  ut_mem_init();
  os_sync_init();
  sync_init();
  os_io_init_simple();

  sync_close();
  sync_initialized = FALSE;
  if (fil_system) {
    fil_close();
  }

  ut_free_all_mem();

  innodb_end();
  innodb_free_param();
}