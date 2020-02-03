# ---------------------------
# CMake modules and settings
# ---------------------------

cmake_minimum_required(VERSION 3.5)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")

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

PROJECT(
	monetdb-bp-reader
	LANGUAGES C CXX
	DESCRIPTION "stand-alone C++ library for reading the MonetDB persistence format"
	HOMEPAGE_URL https://github.com/eyalroz/monetdb-bbp-reader
	VERSION 0.4.0
)

# ---------------------
# Project options
# ---------------------

set(GDK_VERSION "061041" CACHE STRING "Version identifier of the MonetDB low-level runtime library")
set_property(CACHE GDK_VERSION PROPERTY STRINGS 061033 061037 061040 061041)
message(STATUS "Using GDK library version ${GDK_VERSION}")

option(BUILD_EXAMPLES "Build example programs" OFF)

include(CompileWithWarnings)

# ---------------------
# Package dependencies
# ---------------------

find_package(Boost 1.56.0 QUIET COMPONENTS filesystem)
set_property(TARGET Boost::filesystem APPEND PROPERTY INTERFACE_COMPILE_DEFINITIONS BOOST_SYSTEM_NO_DEPRECATED)

find_package(gsl-lite 0.32 REQUIRED)


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

# This command puts the build platform's native directory separator
# into the ${DIR_SEP} CMake variable
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


# -----------------------
# Miscellaneous targets
# -----------------------

# This is the top-level project.
set_source_files_properties( tags PROPERTIES GENERATED true )
add_custom_target(tags
	COMMAND ctags --langmap=c++:+.hpp -R ./
	WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# -----------------------
# Generated config files
# -----------------------

# The following definitions are used within the generated GDK version selection header
set(GDK_H_FILE                  "from-monetdb-sources/v${GDK_VERSION}/gdk/gdk.h")
set(GDK_PRIVATE_H_FILE          "from-monetdb-sources/v${GDK_VERSION}/gdk/gdk_private.h")

set(GENERATED_INCLUDES_DIR      "${CMAKE_CURRENT_BINARY_DIR}/include")
set(GDK_VERSION_SELECTION_FILE  "${GENERATED_INCLUDES_DIR}/gdk_version_selection.h")
# The following configuration file is intended for non-MonetDB source files of this library
configure_file(
	"src/gdk_version_selection.h.in"
	${GDK_VERSION_SELECTION_FILE}
	@ONLY
)

set(MONETDB_CONFIG_H_FILE     "${GENERATED_INCLUDES_DIR}/monetdb_config.h")
set(SOURCES_FROM_MONETDB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/src/monetdb-buffer-pool/from-monetdb-sources")

# The following configuration file is intended mainly for the MonetDB source files
configure_file(
	"${SOURCES_FROM_MONETDB_ROOT}/monetdb_config.h.in"
	${MONETDB_CONFIG_H_FILE}
	@ONLY
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GDK_VERSION_DEFINE} ${GDK_SELECTOR_DEFINE}")

set(VERSION_SPECIFIC_MONETDB_SOURCES ${SOURCES_FROM_MONETDB_ROOT}/v${GDK_VERSION})
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
target_link_libraries(monetdb-buffer-pool PUBLIC Boost::filesystem gsl::gsl-lite)
# See http://stackoverflow.com/a/30877725/1593077 about the next line

set_property(TARGET monetdb-buffer-pool PROPERTY CXX_STANDARD 14)
set_property(TARGET monetdb-buffer-pool PROPERTY CXX_STANDARD_REQUIRED ON)
set_property(TARGET monetdb-buffer-pool PROPERTY CXX_EXTENSIONS OFF)

set_property(TARGET monetdb-buffer-pool PROPERTY C_STANDARD 99)
set_property(TARGET monetdb-buffer-pool PROPERTY C_STANDARD_REQUIRED ON)
set_property(TARGET monetdb-buffer-pool PROPERTY C_EXTENSIONS OFF)

target_include_directories(
	monetdb-buffer-pool
	PUBLIC
	"$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>"
	"$<BUILD_INTERFACE:${GENERATED_INCLUDES_DIR}>"
	"$<BUILD_INTERFACE:${SOURCES_FROM_MONETDB_ROOT}/v${GDK_VERSION}>"
	"$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
	# TODO: May need to explicitly include a subfolder of the CMake install includedir for the "vGDK_VERSION"
)

if (BUILD_EXAMPLES)
	add_subdirectory(examples)
endif()

# -------------------------
# Installation
# -------------------------

include(GNUInstallDirs)

install(
	TARGETS monetdb-buffer-pool
	EXPORT monetdb-bp-reader_export
	RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
	ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
	LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
	INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

install(
	DIRECTORY src/
	DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
	FILES_MATCHING REGEX "\\.(h|hpp)$"
	PATTERN "src/monetdb-buffer-pool/from-monetdb-sources" EXCLUDE
	PATTERN "src/util" EXCLUDE
)
install(
	FILES "src/util/optional.hpp"
	DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/util"
)

install(
	FILES "${GDK_VERSION_SELECTION_FILE}" "${MONETDB_CONFIG_H_FILE}"
	DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

install(
	EXPORT monetdb-bp-reader_export
	DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/monetdb-bp-reader"
	NAMESPACE "monetdb-bp-reader::"
	FILE "monetdb-bp-reader-config.cmake"
)

include(CMakePackageConfigHelpers)

write_basic_package_version_file(
	"monetdb-bp-reader-config-version.cmake"
	VERSION ${PROJECT_VERSION}
	COMPATIBILITY SameMinorVersion
)

install(
	FILES "${CMAKE_CURRENT_BINARY_DIR}/monetdb-bp-reader-config-version.cmake"
	DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/monetdb-bp-reader"
)


