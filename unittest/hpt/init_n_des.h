//
// Created by kadam on 01/10/20.
//

/** This file cotnains the code which can be used
 * to initialize global variables
 * that are needed to call arbitrary functions from code
 * and similalry destroy those variables at the end */


/** include headers */
#include <hash0hash.h>
#include <tap.h>
#include <univ.i>
#include <ut0crc32.h>
#include <srv0srv.h>
#include <srv0start.h>
#include <btr0types.h>
#include <row0mysql.h>
#include <mysqld.h>


/** define local (to this file) macros */
#define G_PTR uchar*


/** declare undeclared global functions */
extern void os_io_init_simple(void);
extern my_bool(*fil_check_if_skip_database_by_path)(const char* name);
extern void
innodb_log_checksum_func_update(
/*============================*/
    ulint	algorithm)	/*!< in: algorithm */;


/** declare and define local (to this file) variables */

/* === ssytem specific options (TODO: are not used directly) === */
longlong buf_pool_size = 100*1024*1024L;
static hash_table_t* databases_include_hash = NULL;
static hash_table_t* databases_exclude_hash = NULL;

struct xb_filter_entry_struct{
  char*		name;
  ibool		has_tables;
  hash_node_t	name_hash;
};
typedef xb_filter_entry_struct	xb_filter_entry_t;

static ulong max_buf_pool_modified_pct;

ulong innobase_large_page_size = 0;

/* The default values for the following, type long or longlong, start-up
parameters are declared in mysqld.cc: */

long innobase_additional_mem_pool_size = 1*1024*1024L;
long innobase_file_io_threads = 4;
long innobase_read_io_threads = 4;
long innobase_write_io_threads = 4;
long innobase_force_recovery = 0;
long innobase_log_buffer_size = 1024*1024L;
long innobase_log_files_in_group = 2;
long innobase_open_files = 300L;

longlong innobase_page_size = (1LL << 14); /* 16KB */
static ulong innobase_log_block_size = 512;

longlong innobase_log_file_size = 48*1024*1024L;

/* The default values for the following char* start-up parameters
are determined in innobase_init below: */

char*	innobase_data_home_dir			= NULL;
char*	innobase_data_file_path 		= NULL;
/* The following has a misleading name: starting from 4.0.5, this also
affects Windows: */
char*	innobase_unix_file_flush_method		= NULL;

/* Below we have boolean-valued start-up parameters, and their default
values */

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


/** local functions' definitions */
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
    // TODO: change to boolean return
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

  auto ret = (my_bool) srv_parse_data_file_paths_and_sizes(
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

  printf("InnoDB: innodb_log_group_home_dir = %s\n",
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

  srv_read_only_mode = FALSE;
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
  srv_buf_pool_size = (ulint) buf_pool_size;

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

static void innodb_init_params(){
  init_tempdir();
  init_page_size();
  init_log_block_size();


  /* Check that values don't overflow on 32-bit systems. */
  if (sizeof(ulint) == 4) {
    if (buf_pool_size > UINT_MAX32) {
      printf("InnnoDB: use-memory can't be over 4GB"
             " on 32-bit systems\n");
    }

    if (innobase_log_file_size > UINT_MAX32) {
      printf("InnnoDB: innobase_log_file_size can't be "
             "over 4GB on 32-bit systemsi\n");
      exit(EXIT_FAILURE);
    }
  }

  os_innodb_umask = (ulint)0664;

  load_data_files();
  load_log_files();
  init_srv_variables();
  set_charset();

}

static my_bool
innodb_init(void)
{
  int	err;
  srv_is_being_started = TRUE;

  // TODO: check the issue of main thread
  //  which arises when this variable is set ot FALSE
  srv_xtrabackup = TRUE;

  err = innobase_start_or_create_for_mysql();

  if (err != DB_SUCCESS) {
    free(internal_innobase_data_file_path);
    internal_innobase_data_file_path = NULL;
    printf("InnoDB: innodb_init(): Error occured.\n");
    return FALSE;
  }

  /* They may not be needed for now */
//	(void) hash_init(&innobase_open_tables,system_charset_info, 32, 0, 0,
//			 		(hash_get_key) innobase_get_key, 0, 0);
//        pthread_mutex_init(&innobase_share_mutex, MY_MUTEX_INIT_FAST);
//        pthread_mutex_init(&prepare_commit_mutex, MY_MUTEX_INIT_FAST);
//        pthread_mutex_init(&commit_threads_m, MY_MUTEX_INIT_FAST);
//        pthread_mutex_init(&commit_cond_m, MY_MUTEX_INIT_FAST);
//        pthread_cond_init(&commit_cond, NULL);

  return TRUE;
}


static
my_bool
find_filter_in_hashtable(
    const char* name,
    hash_table_t* table,
    xb_filter_entry_t** result
)
{
  xb_filter_entry_t* found = NULL;
  HASH_SEARCH(name_hash, table, ut_fold_string(name),
              xb_filter_entry_t*,
              found, (void) 0,
              !strcmp(found->name, name));

  if (found && result) {
    *result = found;
  }
  return (found != NULL);
}


enum skip_database_check_result {
  DATABASE_SKIP,
  DATABASE_SKIP_SOME_TABLES,
  DATABASE_DONT_SKIP,
  DATABASE_DONT_SKIP_UNLESS_EXPLICITLY_EXCLUDED,
};

/************************************************************************
Checks if a database specified by name should be skipped from backup based on
the --databases, --databases_file or --databases_exclude options.

@return TRUE if entire database should be skipped,
	FALSE otherwise.
*/
static
skip_database_check_result
check_if_skip_database(
    const char* name  /*!< in: path to the database */
)
{
  /* There are some filters for databases, check them */
  xb_filter_entry_t*	database = NULL;

  if (databases_exclude_hash &&
      find_filter_in_hashtable(name, databases_exclude_hash,
                               &database) &&
      !database->has_tables) {
    /* Database is found and there are no tables specified,
       skip entire db. */
    return DATABASE_SKIP;
  }

  if (databases_include_hash) {
    if (!find_filter_in_hashtable(name, databases_include_hash,
                                  &database)) {
      /* Database isn't found, skip the database */
      return DATABASE_SKIP;
    } else if (database->has_tables) {
      return DATABASE_SKIP_SOME_TABLES;
    } else {
      return DATABASE_DONT_SKIP_UNLESS_EXPLICITLY_EXCLUDED;
    }
  }

  return DATABASE_DONT_SKIP;
}


/************************************************************************
Checks if a database specified by path should be skipped from backup based on
the --databases, --databases_file or --databases_exclude options.

@return TRUE if the table should be skipped. */
my_bool
check_if_skip_database_by_path(
    const char* path /*!< in: path to the db directory. */
)
{
  if (databases_include_hash == NULL &&
      databases_exclude_hash == NULL) {
    return(FALSE);
  }

  const char* db_name = strrchr(path, SRV_PATH_SEPARATOR);
  if (db_name == NULL) {
    db_name = path;
  } else {
    ++db_name;
  }

  return check_if_skip_database(db_name) == DATABASE_SKIP;
}


my_bool setup(){
  /* Setup skip fil_load_single_tablespaces callback.*/
  fil_check_if_skip_database_by_path = check_if_skip_database_by_path;

  // TODO: understand this logic
  pthread_key_create(&THR_THD, NULL);
  my_pthread_setspecific_ptr(THR_THD, NULL);

  system_charset_info = &my_charset_utf8_general_ci;

  srv_max_n_threads = 1000;
  srv_n_purge_threads = 1;

  // The commented code is not needed as it is already called by innobase_start_or_create_for_mysql()
  // Still, it is kept here for reference
//  // Initialize universal memory block list (does not allocate memory)
//  ut_mem_init();
//
//  /* temporally dummy value to avoid crash */
//  srv_page_size_shift = 14;
//  srv_page_size = (1 << srv_page_size_shift);
//
//  // Initializes global event and OS mutex lists
//  os_sync_init();
//
//  // Initializes the synchronization data structures i.e. mutex list and
//  // read write lock list
//  sync_init();
//
//
//  // initializes memory pool (allocates memory here)
//  mem_init(srv_mem_pool_size);
// // the above whole block or the below line
//  srv_general_init();

//  // initializes seek mutexes
//  os_io_init_simple();


//  ut_crc32_init();


#ifdef WITH_INNODB_DISALLOW_WRITES
  srv_allow_writes_event = os_event_create();
  os_event_set(srv_allow_writes_event);
#endif


  // initialize all the parameters related to innodb
  innodb_init_params();

  /* increase IO threads */
  if(srv_n_file_io_threads < 10) {
    srv_n_read_io_threads = 4;
    srv_n_write_io_threads = 4;
  }
  srv_max_buf_pool_modified_pct = (double)max_buf_pool_modified_pct;

  if (!innodb_init()){
    return FALSE;
  };
  return TRUE;
}

static
void
innodb_free_params()
{
  srv_free_paths_and_sizes();
  free(internal_innobase_data_file_path);
  internal_innobase_data_file_path = NULL;
  free_tmpdir(&mysql_tmpdir_list);
}

static
void
remove_local_files(){
  // code to remove data, log and undo files
  char file_name[10000];
  int dirnamelen;

  dirnamelen = strlen(srv_data_home);
  memcpy(file_name, srv_data_home, dirnamelen);
  for (unsigned int i=1; i <= srv_n_data_files; i++){
    snprintf(file_name + dirnamelen, sizeof(file_name) - dirnamelen, "ibdata%d", i);
    remove(file_name);
  }

  dirnamelen = strlen(srv_log_group_home_dir);
  memcpy(file_name, srv_log_group_home_dir, dirnamelen);
  for (unsigned int i=0; i < srv_n_log_files; i++){
    snprintf(file_name + dirnamelen, sizeof(file_name) - dirnamelen, "ib_logfile%d", i);
    remove(file_name);
  }

  dirnamelen = strlen(srv_data_home);
  memcpy(file_name, srv_data_home, dirnamelen);
  for (unsigned int i=1; i <= srv_undo_tablespaces; i++){
    snprintf(file_name + dirnamelen, sizeof(file_name) - dirnamelen, "undo%03d", i);
    remove(file_name);
  }

}

// TODO: solve
//  Warning:    8 bytes lost at 0x5617d125a830, allocated by T@0 at mysys/my_malloc.c:241, hpt/init_n_des.h:366, hpt/init_n_des.h:427, hpt/init_n_des.h:610, hpt/dummy-t.cc:46, csu/libc-start.c:344, 0x5617ce53424a
//  Memory lost: 8 bytes in 1 chunks
void destroy(){
// done in innodb_shutdown, still kept for reference
//  mem_close();
//  ut_free_all_mem();
//  if (fil_system) {
//    fil_close();
//  }
//  sync_close();
//  sync_initialized = FALSE;

  innodb_shutdown();

  innodb_free_params();

  remove_local_files();
}

