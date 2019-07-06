#pragma once
#ifndef MONETDB_GDK_WRAPPER_H_
#define MONETDB_GDK_WRAPPER_H_

#include "gdk_version_selection.h"
#include "util/optional.hpp"
#include <cstddef>
#include <functional>
#include <string>
#include <ostream>

extern "C" {
// For now, you have to configure MonetDB and copy the resulting
// file to this location:

#include "monetdb_config.h"

// ... but what should really happen is that:
// 1. We filter out only the parts of monetdb_config which are
//    actually necessary for reading BBPs (e.g. no multi-threaded stuff)
// 2. We use the build system of this library to generate an appropriate
//    subset of monetdb_config
// 3. We replace the above include with the generated one
}

extern "C" {
// The implementation file will not need this extra include;
// code using the library - will
#ifdef BBP_READER_IMPLEMENTATION
#include GDK_H_FILE
#else
#include "gdk_snippet.h"
#include "atoms_snippet.h"
#endif
}

namespace monetdb {

namespace gdk {

using util::optional;
using util::nullopt;

using type_t = int;

enum memory_storage_type_t {
	gdk_malloc              = 0,
	mmap                    = 1,
	mmap_copy_on_write      = 2,
	// only possible with GDK versions above 061033 octal
	non_gdk_malloc          = 3,
	unowned                 = 4,
	mmap_with_absolute_path = 5,
};

const char* type_name(type_t gdk_type);
unsigned short type_size(type_t gdk_type);


/**
 * This exposes Tloc for inlining, hopefully reducing overhead
 * (but is not used by the BBP reader library code itself)
 */
inline const void* heap_location(const COLrec* __restrict__ column_record, BUN pos)
{
	return column_record->heap.base + ((pos) << column_record->shift);
}

#if GDK_VERSION <= 061040
inline bool is_dense(const COLrec* column_record)
{
	return (bool) column_record->dense;
}
#else
inline bool is_dense(const COLrec*) { return true; }
#endif


class global_lock_t;

}// namespace gdk

} // namespace monetdb

#endif /* MONETDB_GDK_WRAPPER_H_ */
