
// TODO: Code duplication here, perhaps factor it out to an "implementation_common.h" file?
// ... but then, perhaps include just gdk_wrapper.h rather than the whole buffer pool header
#include "gdk_version_selection.h"
#define BBP_READER_IMPLEMENTATION
	// Note:
	// buffer_pool.h must be included _after_ the previous definition - as
	// it behaves different when included from the reader implementation
	// and from code using the BBP library. But - since that's where
	// we get gdk_wrapper, it must be included _before_ variable_width_heap.
#include "buffer_pool.h"

#include "column_proxy.h"

namespace monetdb {
namespace gdk {

static const COLrec* bat_column_record(const BAT* b)
{
#if GDK_VERSION >= 061035
	return &b->T;
#else
	return b->T;
#endif
}

/**
 * @note descriptor must be non-null
 */
static inline bool is_headless(const BAT* descriptor)
{
#if GDKLIBRARY >= 061035
	(void) descriptor; // since v061035, all BATs are headless.
	return true;
#else
	if (descriptor->H == nullptr) {
		throw std::invalid_argument(
			"Encountered a BAT descriptor with no head COLrec - not even some stub");
	}
	return
		(BAThtype(descriptor) == TYPE_void or
		 BAThtype(descriptor) == TYPE_oid) and
		(BAThdense(descriptor) and descriptor->hseqbase == 0);
#endif
}

/**
 * @note This wrapper exists for two reasons:
 * 1. The const_cast
 * 2. The extra validity check - to ward against evil spirits
 *    and GDK assertions
 */
static BAT* get_descriptor(const BBP* bbp, bat bat_index)
{
	if (not BBPvalid(const_cast<BBP*>(bbp), bat_index)) {
		return nullptr;
	}
	return BBPdescriptor(const_cast<BBP*>(bbp), bat_index);
}

column_proxy::column_proxy(
	const buffer_pool&  buffer_pool,
	pool_index_type     index_in_pool)
:
	buffer_pool_(buffer_pool), index_in_pool_(index_in_pool),
	bat_descriptor_(get_descriptor(buffer_pool.gdk_bbp, index_in_pool))
{ }

const BAT* column_proxy::descriptor() const
{
	// See constructor for (at least some of) the cases when a descriptor may be nullptr
	if (bat_descriptor_ == nullptr) {
		throw std::invalid_argument(
			"The column with index " + std::to_string(index_in_pool_) + " does not have a "
			"valid BAT descriptor");
	}
	return bat_descriptor_;
}

const COLrec* column_proxy::record() const
{
	auto rec = bat_column_record(descriptor());
#if GDK_VERSION < 061035
	if (rec == nullptr) {
		throw std::runtime_error(
			"No column record available for column index " + std::to_string(index_in_pool_));
	}
#endif
	return rec;
}

type_t column_proxy::type() const
{
	return BATttype(descriptor());
}

unsigned short column_proxy::width() const
{
	return record()->width;
}

/**
 * This is stronger than MonetDB's validity, since we're only
 * interested in column data that was found on disk; and we
 * don't want to see any non-headless-BATs
 */
bool column_proxy::is_valid() const
{
	return
		(bat_descriptor_ != nullptr) and // this already "encapsulates" some checks, see the ctor
		is_headless(bat_descriptor_);
			// These days, all BATs are supposed to be headless, but let's be on
			// the safe side and make sure
}

template <>
optional<column_name<column_name_kind::physical>>
column_proxy::name<column_name_kind::physical>() const
{
	auto raw_name = BBP_physical(buffer_pool_.gdk_bbp, (bat) index_in_pool_);
	if (raw_name == nullptr) { return { }; }
	return make_physical_name(raw_name);
}
template <>
optional<column_name<column_name_kind::logical>>
column_proxy::name<column_name_kind::logical>() const
{
	auto raw_name = BBP_logical(buffer_pool_.gdk_bbp, (bat) index_in_pool_);
	if (raw_name == nullptr) { return { }; }
	return make_logical_name(raw_name);
}

template <>
optional<column_name<column_name_kind::sql>>
column_proxy::name<column_name_kind::sql>() const
{
	if (not has_value(buffer_pool_.name_vis_index_bimap_)) return { };
	if (index_in_pool_ >= buffer_pool_.size()) {
		throw std::invalid_argument("BBP column index exceeds the last BBP column's");
	}
	return buffer_pool_.name_vis_index_bimap_.value().sql_name_by_column_index[index_in_pool_];
}

size_t column_proxy::length() const
{
	return descriptor()->batCount;
}

size_t column_proxy::allocated_capacity() const
{
	return descriptor()->batCapacity;
}

memory_storage_type_t column_proxy::storage_type() const
{
	auto heap_storage_type_raw = record()->heap.storage;

	if (heap_storage_type_raw == STORE_INVALID) {
		throw std::runtime_error(
			"Failed obtaining column heap memory storage type for column index " + std::to_string(index_in_pool_));
	}
	return (memory_storage_type_t) heap_storage_type_raw;
}



} // namespace gdk
} // namespace monetdb
