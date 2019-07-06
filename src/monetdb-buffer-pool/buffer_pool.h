#pragma once
#ifndef MONETDB_BUFFER_POOL_H_
#define MONETDB_BUFFER_POOL_H_

#include "gdk_wrapper.h"
#include "column_name.h"
#include "column_proxy.h"

#include "util/optional.hpp" // TODO: Try to have a simpler mechanism that this
#ifndef GSL_SPAN_H
#include "gsl/gsl-lite.h" // TODO: Perhaps switch to Microsoft GSL, and only include span?
#endif
#include <cstddef>
#include <functional>
#include <string>
#include <ostream>
#include <unordered_map>
#include <vector>

namespace monetdb {

namespace gdk {

/**
 * @brief A structure holding the columns persisted by a MonetDB server,
 * in their state after loading from disk (that is, partially copies in
 * simply-allocated memory, and partially mmap'ed files).
 *
 * @note this class mostly meets the requirements of the Container concept,
 * see http://en.cppreference.com/w/cpp/concept/Container , with random-access
 * ability. Thus you should be able to use it similarly to an `std::vector` of
 * {@ref column_proxy_t}'s.
 */
class buffer_pool {

public: // type definitions

	using index_type       = column_proxy::pool_index_type;
		// Due to the order of definition, we can't have the definition
		// here originally. Also, note that it's a _signed_ type.
	using gdk_version_type = int;

public:

	friend class column_proxy;

public: // types to satisfy the Container concept, to the extent possible
	using value_type        = void;
	using reference 	    = column_proxy;
	using const_reference 	= const column_proxy;
	using pointer           = nullptr_t;
	using const_pointer     = const nullptr_t;
	using difference_type   = std::make_signed_t<index_type>;
	using size_type         = index_type; // see getBBPsize()


public: // constructor & destructor
	buffer_pool(const std::string& db_path, bool with_sql_name_index = true);
	buffer_pool(const buffer_pool&) = delete; // it holds a lock, so no copying.
	buffer_pool(buffer_pool&& other) = default;
	~buffer_pool();

protected: // constructor & destructor
	// These are used internally for actual construction - after verifying
	// that the path the user passed is indeed a valid path to a DB's buffer pool
	struct path_verified_tag_t {  };
	buffer_pool(path_verified_tag_t, const std::string& db_path, bool with_sql_name_index);

public: // mutator & non-mutator methods (no distinction at the moment

	/**
	 * @note The size is value here, since not all column indices are valid
	 * columns we can use.
	 */
	size_type                size() const;

	size_type                max_size() const { throw std::logic_error("not supported"); }
	bool                     empty()
	{
		return false; // A BBP always some internal-use columns in it
	}
	gdk_version_type         version() const;
	static gdk_version_type  library_version();
	const column_proxy       at(index_type index) const
	{
		if (index >= size()) { throw std::invalid_argument("No such column"); }
		return column_proxy { *this, index };
	}
	column_proxy             at(index_type index)
	{
		if (index >= size()) { throw std::invalid_argument("No such column"); }
		return column_proxy { *this, index };
	}
	/**
	 * @note
	 *
	 * @param physical_name should have the format FOLDER_NAME/BASE_FILENAME,
	 * where both FOLDER_NAME and FILE_NAME are printed numbers. Thus "12/135"
	 * is a possible value for this parameter
	 * @return the position in the BBP of the record for the BAT with the
	 * specified physical name, if one exists; nothing (nullopt) otherwise
	 */
	template <column_name_kind Kind>
	optional<index_type>     find_column(const column_name<Kind>& name) const;

	void                     populate_sql_name_bimap();


public: // operators
	column_proxy           operator[](index_type index) { return at(index); }
	const column_proxy     operator[](index_type index) const { return at(index); }

protected:

	template <typename Reference>
	class iterator_base {
	public:
		iterator_base(const buffer_pool* pool, index_type index) : pool_(pool), index_(index) { }
		iterator_base(const iterator_base&) = default;
		iterator_base(iterator_base&&) = default;
		iterator_base& operator++()      { index_++; return *this; }
		iterator_base  operator++(int)   { auto ret = *this; index_++; return ret; }
		iterator_base& operator--()      { index_--; return *this; }
		iterator_base  operator--(int)   { auto ret = *this; index_--; return ret; }

		iterator_base& operator+=(size_type n) { index_+= n; return *this; }
		iterator_base& operator-=(size_type n) { index_-= n; return *this; }

	protected:
		bool is_comparable_to(const iterator_base& other) const { return other.pool_ == other.pool_; }

	public:
		bool operator==(const iterator_base<Reference>& other) { return is_comparable_to(other) and index_ == other.index_; }
		bool operator< (const iterator_base<Reference>& other) { return is_comparable_to(other) and index_ <  other.index_; }
		bool operator> (const iterator_base<Reference>& other) { return is_comparable_to(other) and index_ >  other.index_; }
		bool operator<=(const iterator_base<Reference>& other) { return is_comparable_to(other) and index_ <= other.index_; }
		bool operator>=(const iterator_base<Reference>& other) { return is_comparable_to(other) and index_ >= other.index_; }
		bool operator!=(const iterator_base<Reference>& other) { return not operator==(other); }

		reference operator*() const { return pool_->at(index_); }
		pointer operator->() const { return nullptr; } // sorry, we don't support using pointers

		reference operator[](size_type n) const { return pool_->at(index_ + n); }

	protected:
		const buffer_pool*           pool_;
		index_type                   index_;
	};

public: // wrapped-index iterators

	using iterator               = iterator_base<reference>;
	using const_iterator         = iterator_base<const_reference>;
	using output_iterator        = iterator_base<reference>;
	class input_iterator : public virtual iterator_base<reference> {
	public:
		buffer_pool::value_type operator*() const { return; }
	};
	class forward_iterator : public virtual input_iterator, public virtual output_iterator { };
	using bidirectional_iterator = iterator;
	using random_access_iterator = iterator;

public: // access the pool using iterators

	iterator       begin();
	const_iterator begin()  const;
	const_iterator cbegin() const;
	iterator       end();
	const_iterator end()    const;
	const_iterator cend()   const;

protected:
	struct name_vis_index_bimap_t {
		std::unordered_map<sql_column_name, index_type, sql_column_name::hasher>
			column_index_by_sql_name;
		std::vector<optional<sql_column_name>>
			sql_name_by_column_index;
	};
	optional<name_vis_index_bimap_t> name_vis_index_bimap_ { nullopt };
	const std::string                db_path_;
	global_lock_t* const             lock;
	BBP* const                       gdk_bbp;
	// Note: Not using an std::unique_ptr since it can't be used for
	// incomplete types (i.e. it's type-specific)
};

template <>
optional<buffer_pool::index_type>
buffer_pool::find_column(const column_name<column_name_kind::sql>& column_name) const;

} // namespace gdk

} // namespace monetdb

namespace std {
template <typename Reference>
struct iterator_traits<monetdb::gdk::buffer_pool::iterator_base<Reference>> {
	using difference_type    = typename monetdb::gdk::buffer_pool::difference_type;
	using value_type         = typename monetdb::gdk::buffer_pool::value_type;
	using reference          = typename monetdb::gdk::buffer_pool::reference;
	using pointer            = typename monetdb::gdk::buffer_pool::pointer;
	using iterator_category  = std::bidirectional_iterator_tag;
};

} // namespace std

#endif /* MONETDB_BUFFER_POOL_H_ */
