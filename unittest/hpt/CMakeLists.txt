INCLUDE(CheckFunctionExists)
INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckCSourceRuns)
INCLUDE(lz4)
INCLUDE(lzo)
INCLUDE(lzma)
INCLUDE(bzip2)
INCLUDE(snappy)

MYSQL_CHECK_LZ4()
MYSQL_CHECK_LZO()
MYSQL_CHECK_LZMA()
MYSQL_CHECK_BZIP2()
MYSQL_CHECK_SNAPPY()

IF(CMAKE_CROSSCOMPILING)
    # Use CHECK_C_SOURCE_COMPILES instead of CHECK_C_SOURCE_RUNS when
    # cross-compiling. Not as precise, but usually good enough.
    # This only make sense for atomic tests in this file, this trick doesn't
    # work in a general case.
    MACRO(CHECK_C_SOURCE SOURCE VAR)
        CHECK_C_SOURCE_COMPILES("${SOURCE}" "${VAR}")
    ENDMACRO()
ELSE()
    MACRO(CHECK_C_SOURCE SOURCE VAR)
        CHECK_C_SOURCE_RUNS("${SOURCE}" "${VAR}")
    ENDMACRO()
ENDIF()

# OS tests
IF(UNIX)

    IF(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        CHECK_INCLUDE_FILES (libaio.h HAVE_LIBAIO_H)
        IF (XTRADB_PREFER_STATIC_LIBAIO)
            SET(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
        ENDIF()
        FIND_LIBRARY(AIO_LIBRARY aio)
        MARK_AS_ADVANCED(AIO_LIBRARY)
        IF(AIO_LIBRARY)
            CHECK_LIBRARY_EXISTS(${AIO_LIBRARY} io_queue_init "" HAVE_LIBAIO)
            IF(HAVE_LIBAIO AND HAVE_LIBAIO_H)
                ADD_DEFINITIONS(-DLINUX_NATIVE_AIO=1)
            ENDIF()
            LINK_LIBRARIES(${AIO_LIBRARY})
        ENDIF()
        ADD_DEFINITIONS("-DUNIV_LINUX -D_GNU_SOURCE=1")
    ELSEIF(CMAKE_SYSTEM_NAME MATCHES "HP*")
        ADD_DEFINITIONS("-DUNIV_HPUX")
    ELSEIF(CMAKE_SYSTEM_NAME STREQUAL "AIX")
        ADD_DEFINITIONS("-DUNIV_AIX")
    ELSEIF(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
        ADD_DEFINITIONS("-DUNIV_SOLARIS")
    ENDIF()
ENDIF()

IF(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    # After: WL#5825 Using C++ Standard Library with MySQL code
    #       we no longer use -fno-exceptions
    #	SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")
ENDIF()

# Enable InnoDB's UNIV_DEBUG and UNIV_SYNC_DEBUG in debug builds
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DUNIV_DEBUG -DUNIV_SYNC_DEBUG")

# Add -Wconversion if compiling with GCC
## As of Mar 15 2011 this flag causes 3573+ warnings. If you are reading this
## please fix them and enable the following code:
#IF(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wconversion")
#ENDIF()

CHECK_FUNCTION_EXISTS(sched_getcpu  HAVE_SCHED_GETCPU)

MY_CHECK_AND_SET_COMPILER_FLAG("-Wno-class-memaccess")

IF(NOT MSVC)

    # either define HAVE_IB_GCC_ATOMIC_BUILTINS or not
    # workaround for old gcc on x86, gcc atomic ops only work under -march=i686
    IF(CMAKE_SYSTEM_PROCESSOR STREQUAL "i686" AND CMAKE_COMPILER_IS_GNUCC AND
            CMAKE_C_COMPILER_VERSION VERSION_LESS "4.4.0")
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=i686")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=i686")
    ENDIF()
    CHECK_C_SOURCE(
            "
  int main()
  {
    long	x;
    long	y;
    long	res;

    x = 10;
    y = 123;
    res = __sync_bool_compare_and_swap(&x, x, y);
    if (!res || x != y) {
      return(1);
    }

    x = 10;
    y = 123;
    res = __sync_bool_compare_and_swap(&x, x + 1, y);
    if (res || x != 10) {
      return(1);
    }
    x = 10;
    y = 123;
    res = __sync_add_and_fetch(&x, y);
    if (res != 123 + 10 || x != 123 + 10) {
      return(1);
    }
    return(0);
  }"
            HAVE_IB_GCC_ATOMIC_BUILTINS
    )
    CHECK_C_SOURCE(
            "
  int main()
  {
    long	res;
    char	c;

    c = 10;
    res = __sync_lock_test_and_set(&c, 123);
    if (res != 10 || c != 123) {
      return(1);
    }
    return(0);
  }"
            HAVE_IB_GCC_ATOMIC_BUILTINS_BYTE
    )
    CHECK_C_SOURCE(
            "#include<stdint.h>
  int main()
  {
    int64_t	x,y,res;

    x = 10;
    y = 123;
    res = __sync_sub_and_fetch(&y, x);
    if (res != y || y != 113) {
      return(1);
    }
    res = __sync_add_and_fetch(&y, x);
    if (res != y || y != 123) {
      return(1);
    }
    return(0);
  }"
            HAVE_IB_GCC_ATOMIC_BUILTINS_64
    )
    CHECK_C_SOURCE(
            "#include<stdint.h>
  int main()
  {
    __sync_synchronize();
    return(0);
  }"
            HAVE_IB_GCC_SYNC_SYNCHRONISE
    )
    CHECK_C_SOURCE(
            "#include<stdint.h>
  int main()
  {
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    __atomic_thread_fence(__ATOMIC_RELEASE);
    return(0);
  }"
            HAVE_IB_GCC_ATOMIC_THREAD_FENCE
    )
    CHECK_C_SOURCE(
            "#include<stdint.h>
  int main()
  {
    unsigned char	c;

    __atomic_test_and_set(&c, __ATOMIC_ACQUIRE);
    __atomic_clear(&c, __ATOMIC_RELEASE);
    return(0);
  }"
            HAVE_IB_GCC_ATOMIC_TEST_AND_SET
    )

    IF(HAVE_IB_GCC_ATOMIC_BUILTINS)
        ADD_DEFINITIONS(-DHAVE_IB_GCC_ATOMIC_BUILTINS=1)
        SET(XTRADB_OK 1)
    ENDIF()

    IF(HAVE_IB_GCC_ATOMIC_BUILTINS_BYTE)
        ADD_DEFINITIONS(-DHAVE_IB_GCC_ATOMIC_BUILTINS_BYTE=1)
    ENDIF()

    IF(HAVE_IB_GCC_ATOMIC_BUILTINS_64)
        ADD_DEFINITIONS(-DHAVE_IB_GCC_ATOMIC_BUILTINS_64=1)
    ENDIF()

    IF(HAVE_IB_GCC_SYNC_SYNCHRONISE)
        ADD_DEFINITIONS(-DHAVE_IB_GCC_SYNC_SYNCHRONISE=1)
    ENDIF()

    IF(HAVE_IB_GCC_ATOMIC_THREAD_FENCE)
        ADD_DEFINITIONS(-DHAVE_IB_GCC_ATOMIC_THREAD_FENCE=1)
    ENDIF()

    IF(HAVE_IB_GCC_ATOMIC_TEST_AND_SET)
        ADD_DEFINITIONS(-DHAVE_IB_GCC_ATOMIC_TEST_AND_SET=1)
    ENDIF()

    # either define HAVE_IB_ATOMIC_PTHREAD_T_GCC or not
    CHECK_C_SOURCE(
            "
#include <pthread.h>
#include <string.h>

int main() {
  pthread_t       x1;
  pthread_t       x2;
  pthread_t       x3;

  memset(&x1, 0x0, sizeof(x1));
  memset(&x2, 0x0, sizeof(x2));
  memset(&x3, 0x0, sizeof(x3));

  __sync_bool_compare_and_swap(&x1, x2, x3);

  return(0);
}"
            HAVE_IB_ATOMIC_PTHREAD_T_GCC)

    IF(HAVE_IB_ATOMIC_PTHREAD_T_GCC)
        ADD_DEFINITIONS(-DHAVE_IB_ATOMIC_PTHREAD_T_GCC=1)
    ENDIF()

    CHECK_C_SOURCE_COMPILES("struct t1{ int a; char *b; }; struct t1 c= { .a=1, .b=0 }; main() { }" HAVE_C99_INITIALIZERS)

ENDIF(NOT MSVC)

CHECK_FUNCTION_EXISTS(asprintf  HAVE_ASPRINTF)
CHECK_FUNCTION_EXISTS(vasprintf  HAVE_VASPRINTF)

# Solaris atomics
IF(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    CHECK_FUNCTION_EXISTS(atomic_cas_ulong  HAVE_ATOMIC_CAS_ULONG)
    CHECK_FUNCTION_EXISTS(atomic_cas_32 HAVE_ATOMIC_CAS_32)
    CHECK_FUNCTION_EXISTS(atomic_cas_64 HAVE_ATOMIC_CAS_64)
    CHECK_FUNCTION_EXISTS(atomic_add_long_nv HAVE_ATOMIC_ADD_LONG_NV)
    CHECK_FUNCTION_EXISTS(atomic_swap_uchar HAVE_ATOMIC_SWAP_UCHAR)
    IF(HAVE_ATOMIC_CAS_ULONG AND
            HAVE_ATOMIC_CAS_32 AND
            HAVE_ATOMIC_CAS_64 AND
            HAVE_ATOMIC_ADD_LONG_NV AND
            HAVE_ATOMIC_SWAP_UCHAR)
        SET(HAVE_IB_SOLARIS_ATOMICS 1)
    ENDIF()

    IF(HAVE_IB_SOLARIS_ATOMICS)
        ADD_DEFINITIONS(-DHAVE_IB_SOLARIS_ATOMICS=1)
        SET(XTRADB_OK 1)
    ENDIF()

    # either define HAVE_IB_ATOMIC_PTHREAD_T_SOLARIS or not
    CHECK_C_SOURCE_COMPILES(
            "   #include <pthread.h>
      #include <string.h>

      int main(int argc, char** argv) {
        pthread_t       x1;
        pthread_t       x2;
        pthread_t       x3;

        memset(&x1, 0x0, sizeof(x1));
        memset(&x2, 0x0, sizeof(x2));
        memset(&x3, 0x0, sizeof(x3));

        if (sizeof(pthread_t) == 4) {

          atomic_cas_32(&x1, x2, x3);

        } else if (sizeof(pthread_t) == 8) {

          atomic_cas_64(&x1, x2, x3);

        } else {

          return(1);
        }

      return(0);
    }
  " HAVE_IB_ATOMIC_PTHREAD_T_SOLARIS)
    CHECK_C_SOURCE_COMPILES(
            "#include <mbarrier.h>
  int main() {
    __machine_r_barrier();
    __machine_w_barrier();
    return(0);
  }"
            HAVE_IB_MACHINE_BARRIER_SOLARIS)

    IF(HAVE_IB_ATOMIC_PTHREAD_T_SOLARIS)
        ADD_DEFINITIONS(-DHAVE_IB_ATOMIC_PTHREAD_T_SOLARIS=1)
    ENDIF()
    IF(HAVE_IB_MACHINE_BARRIER_SOLARIS)
        ADD_DEFINITIONS(-DHAVE_IB_MACHINE_BARRIER_SOLARIS=1)
    ENDIF()
ENDIF()


IF(UNIX)
    # this is needed to know which one of atomic_cas_32() or atomic_cas_64()
    # to use in the source
    SET(CMAKE_EXTRA_INCLUDE_FILES pthread.h)
    CHECK_TYPE_SIZE(pthread_t SIZEOF_PTHREAD_T)
    SET(CMAKE_EXTRA_INCLUDE_FILES)
ENDIF()

IF(SIZEOF_PTHREAD_T)
    ADD_DEFINITIONS(-DSIZEOF_PTHREAD_T=${SIZEOF_PTHREAD_T})
ENDIF()

IF(MSVC)
    ADD_DEFINITIONS(-DHAVE_WINDOWS_ATOMICS)
    ADD_DEFINITIONS(-DHAVE_WINDOWS_MM_FENCE)
    SET(XTRADB_OK 1)
ENDIF()


# Include directories under xtradb
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/xtradb/include
        ${CMAKE_SOURCE_DIR}/storage/xtradb/handler)

# Sun Studio bug with -xO2
IF(CMAKE_CXX_COMPILER_ID MATCHES "SunPro"
        AND CMAKE_CXX_FLAGS_RELEASE MATCHES "O2"
        AND NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    # Sun Studio 12 crashes with -xO2 flag, but not with higher optimization
    # -xO3
    SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/rem/rem0rec.cc
            PROPERTIES COMPILE_FLAGS -xO3)
ENDIF()

# Removing compiler optimizations for innodb/mem/* files on 64-bit Windows
# due to 64-bit compiler error, See MySQL Bug #19424, #36366, #34297
IF (MSVC AND CMAKE_SIZEOF_VOID_P EQUAL 8)
    SET_SOURCE_FILES_PROPERTIES(mem/mem0mem.cc mem/mem0pool.cc
            PROPERTIES COMPILE_FLAGS -Od)
ENDIF()


INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/sql
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/mysql
        ${CMAKE_SOURCE_DIR}/storage/xtradb/include
        ${CMAKE_SOURCE_DIR}/unittest/mytap)

ADD_EXECUTABLE(create_tablespace-t create_tablespace-t.cc ../../sql/mf_iocache_encr.cc ../../libmysql/libmysql.c)
TARGET_LINK_LIBRARIES(create_tablespace-t sql mytap)
ADD_DEPENDENCIES(create_tablespace-t GenError)
MY_ADD_TEST(create_tablespace)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/sql
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/storage/xtradb/include
        ${CMAKE_SOURCE_DIR}/storage/xtradb/btr)

ADD_EXECUTABLE(index-t index-t.cc ../../sql/mf_iocache_encr.cc ../../storage/xtradb/btr/btr0btr.cc)

TARGET_LINK_LIBRARIES(index-t sql)
ADD_DEPENDENCIES(index-t GenError)
MY_ADD_TEST(index-t)
