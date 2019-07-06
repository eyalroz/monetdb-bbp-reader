# ---------------------------
# CMake modules and settings
# ---------------------------

cmake_minimum_required(VERSION 3.1)
# Take modules from the top-level project modules dir first, then from the modules dir specific to this project
set(CMAKE_MODULE_PATH
	${CMAKE_MODULE_PATH}
	${CMAKE_SOURCE_DIR}/cmake/Modules/
	${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/
)

include(CheckIncludeFiles)
include(CheckTypeSize)
include(CheckSymbolExists)
include(CheckFunctionExists)
include(CheckCSourceCompiles)
include(TestBigEndian)

# ----------------------
# Project & build type settings
# ----------------------

if(NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "")
	set(CMAKE_BUILD_TYPE "Release" )
endif()

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	# This is the top-level project.
	PROJECT(monetdb-bp-reader)
endif()

# ----------------------------
# General C/C++ build settings
# ----------------------------

include(CompileWithWarnings)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # It seems MonetDB relies on the 128-bit integer extension

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF) # It seems MonetDB relies on the 128-bit integer extension

include_directories( "src/" )

# -------------------- 
# Platform Feature checks
# --------------------

test_big_endian(WORDS_BIGENDIAN)

# Note that not all checkes are required for all versions of the GDK library.


check_include_file( strings.h         HAVE_STRINGS_H        )
check_include_file( dirent.h          HAVE_DIRENT_H         )
check_include_file( dlfcn.h           HAVE_DLFCN_H          )
check_include_file( fcntl.h           HAVE_FCNTL_H          )
check_include_file( inttypes.h        HAVE_INTTYPES_H       )
check_include_file( kvm.h             HAVE_KVM_H            )
check_include_file( mach/mach_init.h  HAVE_MACH_MACH_INIT_H )
check_include_file( mach/task.h       HAVE_MACH_TASK_H      )
check_include_file( semaphore.h       HAVE_SEMAPHORE_H      )
check_include_file( pwd.h             HAVE_PWD_H            )
check_include_file( strings.h         HAVE_STRINGS_H        )
check_include_file( string.h          HAVE_STRING_H         )
check_include_file( time.h            HAVE_TIME_H           )
check_include_file( stdlib.h          HAVE_STDLIB_H         )
check_include_file( sys/file.h        HAVE_SYS_FILE_H       )
check_include_file( sys/mman.h        HAVE_SYS_MMAN_H       )
check_include_file( sys/param.h       HAVE_SYS_PARAM_H      )
check_include_file( sys/stat.h        HAVE_SYS_STAT_H       )
check_include_file( sys/sysctl.h      HAVE_SYS_SYSCTL_H     )
check_include_file( sys/ndir.h        HAVE_SYS_NDIR_H       )
check_include_file( sys/times.h       HAVE_SYS_TIMES_H      )
check_include_file( sys/time.h        HAVE_SYS_TIME_H       )
check_include_file( sys/types.h       HAVE_SYS_TYPES_H      )
check_include_file( sys/dir.h         HAVE_SYS_DIR_H        )
check_include_file( memory.h          HAVE_MEMORY_H         )
check_include_file( malloc.h          HAVE_MALLOC_H         )
check_include_file( ndir.h            HAVE_NDIR_H           )
check_include_file( unistd.h          HAVE_UNISTD_H         )
check_include_file( stdint.h          HAVE_STDINT_H         )
check_include_file( stdarg.h          HAVE_STDARG_H         )
check_include_file( ieeefp.h          HAVE_IEEEFP_H         )
check_include_file( valgrind.h        HAVE_VALGRIND         ) # yes, without the _H.
check_include_file( winsock.h         HAVE_WINSOCK_H        ) # not for networking, but for timeval

set(STDC_HEADERS ${HAVE_STDLIB_H} and ${HAVE_STDARG_H} and ${HAVE_STRING_H} and ${HAVE_FLOAT_H})

check_type_size( "char"            SIZEOF_CHAR        )
check_type_size( "int"             SIZEOF_INT         )
check_type_size( "long"            SIZEOF_LONG        )
check_type_size( "short"           SIZEOF_PTRDIFF_T   )
check_type_size( "ptrdiff_t"       SIZEOF_SHORT       )
check_type_size( "size_t"          SIZEOF_SIZE_T      )
check_type_size( "ssize_t"         SIZEOF_SSIZE_T     )
check_type_size( "void *"          SIZEOF_VOID_P      )
check_type_size( "lng"             SIZEOF_LNG         )
check_type_size( "long long"       SIZEOF_LONG_LONG   )
check_type_size( "wchar_t"         SIZEOF_WCHAR_T     )
check_type_size( "__int64"         SIZEOF___INT64     )
check_type_size( "wchar_t"         SIZEOF_WCHAR_T     )
check_type_size( "__int64"         SIZEOF___INT64     )
check_type_size( "__int128"        SIZEOF___INT128    )
check_type_size( "__uint128"       SIZEOF___UINT128   )
check_type_size( "__int128_t"      SIZEOF___INT128_T  )
check_type_size( "__uint128_t"     SIZEOF___UINT128_T )
check_type_size( "intptr_t"        SIZEOF_INTPTR_T    )

# Now let's repeat that, to get variables named HAVE_CHAR, HAVE_INT etc. which
# don't always appear for some reason
check_type_size( "char"            CHAR        )
check_type_size( "int"             INT         )
check_type_size( "long"            LONG        )
check_type_size( "short"           PTRDIFF_T   )
check_type_size( "ptrdiff_t"       SHORT       )
check_type_size( "size_t"          SIZE_T      )
check_type_size( "ssize_t"         SSIZE_T     )
check_type_size( "void *"          VOID_P      )
check_type_size( "lng"             LNG         )
check_type_size( "long long"       LONG_LONG   )
check_type_size( "wchar_t"         WCHAR_T     )
check_type_size( "__int64"         __INT64     )
check_type_size( "wchar_t"         WCHAR_T     )
check_type_size( "__int64"         __INT64     )
check_type_size( "__int128"        __INT128    )
check_type_size( "__uint128"       __UINT128   )
check_type_size( "__int128_t"      __INT128_T  )
check_type_size( "__uint128_t"     __UINT128_T )
check_type_size( "intptr_t"        INTPTR_T    )


check_function_exists( strtof           HAVE_STRTOF        )
check_function_exists( gettimeofday     HAVE_GETTIMEOFDAY  )
check_function_exists( fork             HAVE_FORK          )
check_function_exists( ftime            HAVE_FTIME         )
check_function_exists( madvise          HAVE_MADVISE       )
check_function_exists( posix_madvise    HAVE_POSIX_MADVISE ) 
check_function_exists( posix_fadvise    HAVE_POSIX_FADVISE )
check_function_exists( uname            HAVE_UNAME         )
check_function_exists( setenv           HAVE_SETENV        )

file(TO_NATIVE_PATH "/" DIR_SEP)

if (${HAVE_TIME_H} AND ${HAVE_SYS_TIME_H})
	check_c_source_compiles(
		"
			#include <time.h>
			#include <sys/time.h>
			int main(void) { return 0; }
		"
		TIME_WITH_SYS_TIME
	)
endif()

# -----
# Boost 
# -----
find_package(Boost 1.56.0 QUIET COMPONENTS filesystem)
include_directories( ${Boost_INCLUDE_DIR} )
# See http://stackoverflow.com/a/30877725/1593077
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_SYSTEM_NO_DEPRECATED")
# These next two lines should not be necessary, but I don't get the proper -L switch
# without them... see http://stackoverflow.com/q/40208960/1593077 
set (CMAKE_SHARED_LINKER_FLAGS ${CMAKE_SHARED_LINKER_FLAGS} "-L${Boost_LIBRARY_DIRS}")
set (CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS} "-L${Boost_LIBRARY_DIRS}")

# -----------------------
# Miscellaneous targets
# -----------------------

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	# This is the top-level project.
	set_source_files_properties( tags PROPERTIES GENERATED true )
	add_custom_target(tags
		COMMAND ctags --langmap=c++:+.hpp -R ./
		WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

	add_custom_target(cclean COMMAND rm -r CMakeCache.txt CMakeFiles/ cmake_install.cmake)
endif()

# -----------------------
# Main target(s)
# -----------------------

include_directories( "src/" "${CMAKE_CURRENT_BINARY_DIR}/include" )

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "lib")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "bin")


set(GDK_VERSION "061041" CACHE STRING "Version identifier of the MonetDB low-level runtime library")
set_property(CACHE GDK_VERSION PROPERTY STRINGS 061033 061037 061040 061041)
message(STATUS "Using GDK library version ${GDK_VERSION}")

set(MONETDB_CONFIG_H_TEMPLATE "from-monetdb-sources/monetdb_config.h.in")
set(MONETDB_CONFIG_H_FILE "${CMAKE_CURRENT_BINARY_DIR}/include/monetdb_config.h")
set(GDK_H_FILE            "from-monetdb-sources/v${GDK_VERSION}/gdk/gdk.h")
set(GDK_PRIVATE_H_FILE    "from-monetdb-sources/v${GDK_VERSION}/gdk/gdk_private.h")
# The following configuration file is intended for non-MonetDB source files of this library
configure_file(src/gdk_version_selection.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/gdk_version_selection.h @ONLY)

set(SOURCES_FROM_MONETDB_ROOT src/monetdb-buffer-pool/from-monetdb-sources)
set(VERSION_SPECIFIC_MONETDB_SOURCES ${SOURCES_FROM_MONETDB_ROOT}/v${GDK_VERSION})

# The following configuration file is intended mainly for the MonetDB source files
configure_file(
	"${SOURCES_FROM_MONETDB_ROOT}/monetdb_config.h.in"
	"${MONETDB_CONFIG_H_FILE}"
	@ONLY
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GDK_VERSION_DEFINE} ${GDK_SELECTOR_DEFINE}")

set(GDK_SOURCE_DIR ${VERSION_SPECIFIC_MONETDB_SOURCES}/gdk)
set(MONETDB_SOURCE_FILES
	${VERSION_SPECIFIC_MONETDB_SOURCES}/monetdb5/modules/atoms/mtime.c
	${GDK_SOURCE_DIR}/gdk_atoms.c
	${GDK_SOURCE_DIR}/gdk_bbp.c
	${GDK_SOURCE_DIR}/gdk_hash.c
	${GDK_SOURCE_DIR}/gdk_posix.c
	${GDK_SOURCE_DIR}/gdk_storage.c
	${GDK_SOURCE_DIR}/gdk_utils.c
	${GDK_SOURCE_DIR}/gdk_heap.c
	${GDK_SOURCE_DIR}/gdk_bat.c
)

if (GDK_VERSION STRGREATER "061040")
	list(
		APPEND MONETDB_SOURCE_FILES 
		${GDK_SOURCE_DIR}/gdk_string.c
	)
endif()

add_library(
	monetdb-buffer-pool
	${MONETDB_SOURCE_FILES}
	src/monetdb-buffer-pool/buffer_pool.cpp
	src/monetdb-buffer-pool/column_proxy.cpp
	src/util/files.cpp
	src/util/string.cpp
)
target_include_directories(
	monetdb-buffer-pool 
	PUBLIC 
	${SOURCES_FROM_MONETDB_ROOT}/v${GDK_VERSION}
)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)

	if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
		set(CXX_FILESYSTEM_LIBRARIES "stdc++fs")
	endif()

	add_executable(
		simple
		EXCLUDE_FROM_ALL
		src/examples/simple.cpp
	)
	target_link_libraries(simple monetdb-buffer-pool ${CXX_FILESYSTEM_LIBRARIES} ${Boost_LIBRARIES})

	add_executable(
		monetdb-bp-reader
		EXCLUDE_FROM_ALL
		src/util/dump.cpp
		src/examples/monetdb-bp-reader.cpp
	)
	target_link_libraries(monetdb-bp-reader monetdb-buffer-pool ${CXX_FILESYSTEM_LIBRARIES} ${Boost_LIBRARIES})

	add_custom_target(examples)
	add_dependencies(examples simple monetdb-bp-reader)
endif()

# We don't do installation for now
# set(INSTALL_LIB_DIR lib CACHE PATH "Installation directory for libraries")

