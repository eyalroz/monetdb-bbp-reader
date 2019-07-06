/**
 * @file column_proxy.h
 *
 * Defines the @ref column_proxy_t class, proxying columns within the loaded BBP
 * structure - as they are not C++ structures, plus we want to simplify and
 * regularize the access to them somewhat.
 *
 */
#pragma once
#ifndef MONETDB_BUFFER_POOL_COLUMN_PROXY_H_
#define MONETDB_BUFFER_POOL_COLUMN_PROXY_H_

#include "gdk_wrapper.h"
#include "column_name.h"

#include "util/optional.hpp" // TODO: Try to have a simpler mechanism that this
#ifndef GSL_SPAN_H
#include "gsl/gsl-lite.h" // TODO: Perhaps switch to Microsoft GSL, and only include span?
#endif
#include <string>
#include <iterator>
#include <functional>

namespace monetdb {
namespace gdk {

class buffer_pool;

/**
 * @brief This class is what you obtain as specific contents of the buffer pool;
 * since we don't actually make copies of anything, it's merely a proxy for making
 * MonetDB GDK calls; it holds no state, owns no memory or resources, and may
 * safely passed by value.
 *
 * @note there's no caching of GDK call results (although there might have been)
 *
 * @note Instances of this class are only valid only within the lifetime
 * of the buffer pool which generated them.
 */
class column_proxy {
public:
	using index_type        = BUN; // A type for indices of elements within the column,
	                               // not of the column within the buffer pool
	using pool_index_type   = bat; // Yes, this one's signed, unfortunately. Live with it.

public:
	// types to satisfy the Container concept, to the extent possible,
	// with behavior similar to a sequence of void*'s - not the actual
	// values, which we can't express due to type erasure

	using value_type        = const void *; // shouldn't this be just void*?
	using reference         = value_type; // there's nothing to refer to, pointers aren't kept anywhere
	using const_reference 	= const void *;
	using pointer           = nullptr_t; // the pointers aren't kept anywhere
	using const_pointer     = nullptr_t; // the pointers aren't kept anywhere
	using difference_type   = std::make_signed_t<index_type>;
	using size_type         = index_type;

	/**
	 * @brief A type-erased iterator for column data, which provides pointers to
	 * where elements' data starts, and no additional information.
	 *
	 * For fixed-width data, MonetDB columns are plain and simply - nothing
	 * but the elements, in sequence, in memory - as though it was a plain array.
	 *
	 * For variable-length data, MonetDB keeps two arrays in memory: One containing
	 * the actual column data (and whose layout is somewhat complex and non-uniform),
	 * and an array of offsets into the variable-length data. The offsets array
	 * is the same as a fixed-width column of that width, so accessing it is simple;
	 * but the interpretation of the offsets is slightly tricky too (namely, for some
	 * sizes the offsets have a base value added to them, due to the complex layout
	 * in the variable-width area).
	 *
	 * When you dereference an iterator into this second kind
	 * of column, you'll get a pointer into the variable-width area, after resolution
	 * involving the offset which you don't need to care about
	 *
	 * @note This iterator is somewhat slow, both because of having a branch
	 * (albeit a well-predictable one) and because of not being size-specific. If you
	 * want faster iteration, arrange for that on your own (e.g. by subtyping
	 * column_proxy_t to make it element-type specific).
	 */
	template <typename Reference>
	class iterator_base {
	public:
		// TODO: It may be the case that the first element
		// index is always zero, but I'm not entirely sure - need
		// to look at the MonetDB codebase again
		enum : BUN { first_element_index = 0 };

		iterator_base(const COLrec& column_record) :
			variable_width_column(column_record.vheap != nullptr),
			offset_or_element_size(column_record.width),
			variable_width_data_base_ptr(
				variable_width_column ? column_record.vheap->base : nullptr),
			offsets_or_fixed_width_data_base_ptr(
				column_record.heap.base + first_element_index * offset_or_element_size),
			pos_(first_element_index)
		{ }
		iterator_base(const iterator_base& other) = default;
		iterator_base(iterator_base&& other) = default;

		iterator_base& operator++()      { pos_++; return *this; }
		iterator_base  operator++(int)   { auto ret = *this; pos_++; return ret; }
		iterator_base& operator--()      { pos_--; return *this; }
		iterator_base  operator--(int)   { auto ret = *this; pos_--; return ret; }

		iterator_base& operator+=(size_type n) { pos_ += n; return *this; }
		iterator_base& operator-=(size_type n) { pos_ -= n; return *this; }

	protected:
		reference at_impl(BUN pos) const {
			return variable_width_column ?
				variable_width_data_base_ptr +
				VarHeapValRaw(offsets_or_fixed_width_data_base_ptr, pos, offset_or_element_size) :
				offsets_or_fixed_width_data_base_ptr + pos * offset_or_element_size;
		}

		reference at(BUN pos) { return at_impl(pos); }
		const_reference at(BUN pos) const { return at_impl(pos); }

	protected:
		bool is_comparable_with(const iterator_base& other) const
		{
			return
				variable_width_column                 == other.variable_width_column and
				offset_or_element_size                == other.offset_or_element_size and
				variable_width_data_base_ptr          == other.variable_width_data_base_ptr and
				offsets_or_fixed_width_data_base_ptr  == other.offsets_or_fixed_width_data_base_ptr;
		}

	public:
		Reference  operator*() const { return at(pos_); }
		const_pointer operator->() const { return {}; }
		Reference  operator[](size_type n) const { return at(pos_ + n); }
		iterator_base operator+(size_type n) const { auto it = *this; return (it += n); }
		iterator_base operator-(size_type n) const { auto it = *this; return (it -= n); }
		bool operator==(const iterator_base& other) const { return is_comparable_with(other) and pos_ == other.pos_; }
		bool operator< (const iterator_base& other) const { return is_comparable_with(other) and pos_ <  other.pos_; }
		bool operator> (const iterator_base& other) const { return is_comparable_with(other) and pos_ >  other.pos_; }
		bool operator<=(const iterator_base& other) const { return is_comparable_with(other) and pos_ <= other.pos_; }
		bool operator>=(const iterator_base& other) const { return is_comparable_with(other) and pos_ >= other.pos_; }
		bool operator!=(const iterator_base& other) const { return not(operator==(other));}

	protected: // data members
		bool                     variable_width_column;
		decltype(COLrec::width)  offset_or_element_size;
		decltype(Heap::base)     variable_width_data_base_ptr; // this points into the vheap
		typename std::conditional_t<std::is_const<Reference>::value, const char*, char *>
		                         offsets_or_fixed_width_data_base_ptr;
			// this points into the offsets heap. It's of void* type because
			// offset widths are only known at runtime
		index_type               pos_;
	};


public: // non-mutators
	index_type             index_in_pool() const { return index_in_pool_; }
	template <column_name_kind Kind>
	util::optional<column_name<Kind>>  name() const;

	gdk::type_t            type() const;
	unsigned short         width() const;
		// this _should_ be the same as type_size(type()), but for some reason - it isn't,
		// even for valid columns
	bool                   is_valid() const;
	const COLrec*          record() const;
	memory_storage_type_t  storage_type() const;

	/**
	 * The number of elements actually in the column.
	 *
	 * @note their indices are not guaranteed start at 0 - at least for now
	 */
	size_type              length() const;

	const buffer_pool&     pool() const { return buffer_pool_; }

	/**
	 * The number of elements which can fit inside the allocated area -
	 * bet it GDKmalloced memory or mmapped file bytes
	 *
	 * @note I believe this _should_ just be a rounding-up of the
	 * count value (or rather of the heap size) to memory/disk page boundary
	 */
	size_type              allocated_capacity() const;

	reference       at(BUN pos) { return begin()[pos]; }
	const_reference at(BUN pos) const { return cbegin()[pos]; }
	reference       operator[](size_type n) { return at(n); }
	const_reference operator[](size_type n) const { return at(n); }

	/**
	 * @note Caveat: If this is a variable-width column, you'll be
	 * getting the offsets into the vheap as span elements - not
	 * the actual element data.
	 */
	template <typename T>
	gsl::span<const T>     as_span() const
	{
		return gsl::span<const T>( static_cast<const T*>(at(0)), length() );
	}


public: // wrapped-index iterators

	using iterator               = iterator_base<reference>;
	using const_iterator         = iterator_base<const_reference>;

public: // iterator-based access


	iterator       begin()        { return iterator       ( *record() ); }
	const_iterator begin()  const { return const_iterator ( *record() ); }
	const_iterator cbegin() const { return const_iterator ( *record() ); }
	iterator       end()          { return iterator       ( *record() ) + length(); }
	const_iterator end()    const { return const_iterator ( *record() ) + length(); }
	const_iterator cend()   const { return const_iterator ( *record() ) + length(); }

public: // other Container concept methods

	size_type              size() const  { return length(); }
	bool                   empty() const { return length() == 0; }



protected: // non-mutators

	/**
	 * @return A non-null BAT descriptor (which you probably don't really need
	 * anyway); if there is none - this throws
	 * @throws runtime_error
	 */
	const struct BAT*      descriptor() const;

public: // constructors & destructors
	column_proxy(const buffer_pool& buffer_pool, pool_index_type index_in_pool);
		// The constructor is pretty innocuous, but can't be coded here,
		// since it involves a GDK call not exposed to user code

	column_proxy(const column_proxy&) = default;
	column_proxy(column_proxy&&) = default;

protected: // data members
	const buffer_pool&       buffer_pool_;
	const pool_index_type    index_in_pool_;
	const struct BAT* const  bat_descriptor_;
};

} // namespace gdk
} // namespace monetdb



namespace std {

// TODO: Try to avoid code duplication here

template <typename Reference>
struct iterator_traits<monetdb::gdk::column_proxy::iterator_base<Reference>> {
	using difference_type    = typename monetdb::gdk::column_proxy::difference_type;
	using value_type         = typename monetdb::gdk::column_proxy::value_type;
	using reference          = typename monetdb::gdk::column_proxy::reference;
	using pointer            = typename monetdb::gdk::column_proxy::pointer;
	using iterator_category  = std::bidirectional_iterator_tag;
};
}

#endif /* MONETDB_BUFFER_POOL_COLUMN_PROXY_H_ */
