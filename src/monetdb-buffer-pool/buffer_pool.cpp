
#include "gdk_version_selection.h"
#include "buffer_pool_lock.hpp"

#define BBP_READER_IMPLEMENTATION
	// Note:
	// buffer_pool.h must be included _after_ the previous definition - as
	// it behaves different when included from the reader implementation
	// and from code using the BBP library. But - since that's where
	// we get gdk_wrapper, it must be included _before_ variable_width_heap.
#include "buffer_pool.h"

extern "C" {
#include GDK_PRIVATE_H_FILE
}
#include "util/files.h"
#include "util/string_view.hpp"
#include "util/string.hpp"

#include <cstring>
#include <cassert>
#include <tuple>
#include <iterator>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>

using util::string_view;

namespace monetdb {

namespace gdk {

template <>
optional<buffer_pool::index_type>
buffer_pool::find_column<column_name_kind::sql>(const sql_column_name& column_name) const
{
	if (not has_value(name_vis_index_bimap_)) {
		throw std::logic_error("SQL name index has not yet been generated");
	}
	auto bimap = name_vis_index_bimap_.value();
	auto find_result = bimap.column_index_by_sql_name.find(column_name);
	if (find_result == bimap.column_index_by_sql_name.end()) { return {}; }
	return find_result->second;
}

const char* type_name(type_t gdk_type)
{
	if (gdk_type >= GDKatomcnt) {
		throw std::invalid_argument("No such GDK type name: " + std::to_string(gdk_type));
	}
	return BATatoms[gdk_type].name;
}

unsigned short type_size(type_t gdk_type)
{
	if (gdk_type >= GDKatomcnt) {
		throw std::invalid_argument("No such GDK type: " + std::to_string(gdk_type));
	}
	if (BATatoms[gdk_type].size <= 0) {
		throw std::invalid_argument("Invalid size value for GDK type " + std::to_string(gdk_type));
	}
	return BATatoms[gdk_type].size;
}

static const std::string& verify_db_path(const std::string& db_path) {
	static constexpr const char* directory_file_name = "BBP.dir";

	filesystem::path glorified_db_path(db_path);
	if (not filesystem::exists(glorified_db_path)) {
		throw std::invalid_argument("Non-existent DB path " + db_path);
	}
	if (not filesystem::is_directory(glorified_db_path)) {
		throw std::invalid_argument("Not a directory: " + db_path);
	}
	if (not util::is_recursable(glorified_db_path.string())) {
		throw std::invalid_argument("Non-recursable DB directory " + db_path);
	}
	auto contents_directory_file_path = glorified_db_path / BAKDIR / directory_file_name;
	if (not filesystem::exists(contents_directory_file_path)) {
		throw std::invalid_argument("Invalid DB path " + db_path
			+ ": Missing the BBP contents directory file "
			+ contents_directory_file_path.string());
	}
	if (not util::is_readable(contents_directory_file_path)) {
		throw std::invalid_argument("Unreadable BBP contents directory at "
			+ contents_directory_file_path.string());
	}

	return db_path;
}

buffer_pool::buffer_pool(const std::string& db_path, bool with_sql_name_index) :
	buffer_pool(path_verified_tag_t{}, verify_db_path(db_path), with_sql_name_index) { }

buffer_pool::buffer_pool(path_verified_tag_t, const std::string& db_path, bool with_sql_name_index) :
	db_path_(db_path),
	lock(new global_lock_t(db_path_, MONETDB_MODE)),
	gdk_bbp(BBPinit(db_path_.c_str()))
{
	if (with_sql_name_index) { populate_sql_name_bimap(); }
}

// TODO: Drop the use of index_in_pool() here in favor of indices appearing directly in
// the index structure
template <column_name_kind Kind>
optional<buffer_pool::index_type> buffer_pool::find_column(const column_name<Kind>& name) const
{
	static_assert(Kind != column_name_kind::sql,
		"We should have a specialization for SQL column names - but have reached the unspecialized case");

	// It might be possible to do this with a binary search, but let's not bother -
	// since there are so many invalid BATs anyway.

	auto match_name = [&name](column_proxy column) {
		auto col_name = column.name<Kind>();
		return
			column.is_valid() and
			has_value(col_name) and
			col_name.value() == name;
	};

	auto find_result = std::find_if(cbegin(), cend(), match_name );
	if (find_result == end()) { return {}; }
	return (*find_result).index_in_pool();
}

buffer_pool::~buffer_pool()
{
	BBPexit(gdk_bbp); // which also should release allocations it has made
	delete lock;
}

buffer_pool::size_type buffer_pool::size() const
{
	return gdk_bbp->size;
}

buffer_pool::iterator       buffer_pool::end()
{
	return iterator ( this, gdk_bbp->size );
}

buffer_pool::const_iterator buffer_pool::end() const
{
	return const_iterator ( this, gdk_bbp->size );
}

buffer_pool::const_iterator buffer_pool::cend() const
{
	return const_iterator ( this, this->size() );
}

buffer_pool::iterator       buffer_pool::begin()
{
	// The BBP can never be accessed at index 0 (for historical reasons).
	return iterator ( this, 1 );
}

buffer_pool::const_iterator buffer_pool::begin() const
{
	// The BBP can never be accessed at index 0 (for historical reasons).
	return const_iterator ( this, 1 );
}

buffer_pool::const_iterator buffer_pool::cbegin() const
{
	// The BBP can never be accessed at index 0 (for historical reasons).
	return const_iterator ( this, 1 );
}

buffer_pool::gdk_version_type buffer_pool::version() const
{
	return gdk_bbp->version;
}

buffer_pool::gdk_version_type buffer_pool::library_version()
{
	return GDKLIBRARY;
}

#if 0
/**
 * The special SQL catalog column-names BAT containers strings each of which
 * contains the schema, the table name and the column name - and possibly
 * other information for BATs which regard tables-only, or primary/foreign key
 * indices etc. While we could just go on from these BATs to the sys.storage SQL
 * table's BATs, we stick with it, and use this function for heuristic decyphering
 * of the names.
 *
 * @note no longer used, but may be useful in the future
 */
static optional<sql_column_name> demangle_sql_column_name(const std::string& combined_sql_column_name)
{
	enum : char { separator = '_' };

	auto first_separator = combined_sql_column_name.find(separator);
	if (first_separator == std::string::npos) {
		auto column_name = combined_sql_column_name.substr(0, first_separator);
		return sql_column_name({ std::string(), std::string(), column_name });
	}
	auto second_separator = combined_sql_column_name.find(separator, first_separator + 1);
	// Lame hack for table names beginning with underscores, such as _tables ...

	std::string schema_name = sql_column_name::default_schema;
	std::string table_name;
	std::string column_name;
	if (second_separator == std::string::npos) {
		table_name = combined_sql_column_name.substr(0, first_separator);
		column_name = combined_sql_column_name.substr(first_separator + 1);
	}
	else if (second_separator == combined_sql_column_name.length() - 1) {
		return {}; // we can't have an empty remainder!
	}
	else {
		schema_name = combined_sql_column_name.substr(0, first_separator);
		table_name = combined_sql_column_name.substr(first_separator + 1, second_separator - (first_separator + 1));
		column_name = combined_sql_column_name.substr(second_separator + 1);
	}
	if (schema_name.empty() or table_name.empty() or column_name.empty()) {
		return {}; // that's invalid
	}
	if (schema_name == "D") {
		return {}; // there's this sort of a dummy BAT for each table; it's supposed to be empty
	}
	if (table_name == "_columns" or table_name == "_tables") {
		table_name = table_name.substr(1); // drop the heading underscore
	}
	return sql_column_name({ schema_name, table_name, column_name });
}
#endif


template <typename S, typename T>
struct bidirectional_unordered_map {
	std::unordered_map<S, T> in_first_direction;
	std::unordered_map<T, S> in_second_direction;
};

static const column_proxy column_by_logical_name_safe(const buffer_pool& pool, const char* raw_logical_name) {
	auto column_index  = pool.find_column(make_logical_name(raw_logical_name));
	if (not has_value(column_index)) {
		throw std::runtime_error("Cannot locate column by logical name in the persisted DB data");
	}
	return pool.at(column_index.value());
}

namespace detail {

/**
 * This should should essentially be instantiated for fixed-width types,
 * where there's no vheap, and the data is in the actual tail.
 */
template <typename T>
static T unerase_type(const void* erased)
{
	return *static_cast<const T *>(erased);
}


/**
 * Our only acknowledged special case variable-length data - strings. We
 * won't copy the entire string - a string_view is non-owning. If you want
 * to use other variable-length data - just add another specialization.
 *
 * @note MonetDB doesn't use nullptr for string values which are NULL
 * in SQL terminology. Rather, and somewhat strangely, it points into
 * actual data, where is encoded an invalid UTF-8 string: A 0x80 (which
 * indicates a multi-byte character) followed by a 0x00, terminating
 * the string in mid-character. Obviously we won't carry this quaint
 * value on into the @ref string_view
 */
template <>
string_view unerase_type<string_view>(const void* erased)
{
	auto c_str = static_cast<const char*>(erased);
	if (c_str == nullptr) return { };
	bool is_encoded_sql_null =
		(c_str[0] == str_nil[0]) and
		(c_str[1] == str_nil[1]);
	if (is_encoded_sql_null) { return { }; }
	return { c_str };
}

} // namespace detail

/**
 * Use this to obtain a value of the original type from the `void *` you get
 * when dereferncing a column iterator. This is necessary since conditioning
 * on the type would trigger a syntax error on one of the branches, while
 * "if constexpr" is not yet available to use with C++14.
 */
template <typename T>
static typename std::remove_reference<T>::type unerase_type(const void* erased)
{
	return gdk::detail::unerase_type<typename std::remove_reference<T>::type>(erased);
}


namespace detail {

template <typename StringishContainer, typename Index>
string_view string_view_at(const StringishContainer& container, Index i)
{
	return container[i];
}

template <typename Index>
string_view string_view_at(const column_proxy& container, Index i)
{
	return unerase_type<string_view>(container[i]);
}

// TODO: This definitely belong in some utility header.
template <typename K, typename V>
struct true_functor {
	bool operator()(const K& k, const V& v) { return true; }
};

} // namespace detail

/**
 * Returns a map from values in the first column to their corresponding values
 * in the second column. At the moment - implemented rather naively.
 */
template <typename Key, typename Value, typename DH, typename P = detail::true_functor<Key, Value>>
static std::unordered_map<Key, Value>
make_map(const column_proxy& keys, column_proxy values, DH dupe_handler, P filter_predicate = { })
{
	if (keys.length() != values.length()) {
		throw std::invalid_argument(
			"Keys column and values column are not of the same length - cannot make a map out of them");
	}
	static_assert(
		(not std::is_pointer<Key>::value) and
		(std::is_trivial<Key>::value or std::is_same<Key,string_view>::value),
		"Invalid type combination");
	static_assert(
		(not std::is_pointer<Value>::value) and
		(std::is_trivial<Value>::value or std::is_same<Value,string_view>::value),
		"Invalid type combination");
	std::unordered_map<Key,Value> map;

	for(column_proxy::index_type i = 0; i < keys.length(); i++) {
		auto key = unerase_type<Key>(keys[i]);
		auto value = unerase_type<Value>(values[i]);

		if (not filter_predicate(key, value)) { continue; }

		auto insert_result = map.insert(std::make_pair(key, value));
		if (not insert_result.second) { dupe_handler(key, value, *insert_result.first); }

	}
	return map;
}


/**
 * Mangle a three-part SQL column name into the single string used for it in the BBP's SQL catalog.
 */
static std::string mangle(
	const string_view schema_name,
	const string_view table_name,
	const string_view column_name)
{
	auto& oss = util::detail::get_ostringstream();
	oss << schema_name << '_' << table_name << '_' << column_name;
	return oss.str();
}

static std::string mangle(const sql_column_name& name)
{
	return mangle(name.schema, name.table, name.column);
}

using pool_sql_catalog_map = std::unordered_map<string_view, buffer_pool::index_type>;

using sql_schema_id_t = buffer_pool::index_type;
using sql_table_id_t  = buffer_pool::index_type;
using sql_column_id_t = buffer_pool::index_type;

/**
 * A buffer pool's "system tables" encode information about SQL tables stored in the pool
 * (the system tables _are_ mostly such SQL tables, so this is a bit self-referential).
 * Part of this information are numeric ids.
 *
 * @note this isn't an column id, nor an index into the buffer pool - but
 * since there's a pool column for every SQL table column, and theoretically,
 * we could have as many SQL tables as SQL columns, the type is the same.
 */
static pool_sql_catalog_map get_sql_catalog_map(const buffer_pool& pool)
{
	auto mangled_sql_names = column_by_logical_name_safe(pool, "sql_catalog_nme");
	auto pool_indices      = column_by_logical_name_safe(pool, "sql_catalog_bid");

	if (mangled_sql_names.size() != pool_indices.size()) {
		throw std::runtime_error(
			"BBP SQL catalog map columns have differing lengths: 'sql_catalog_nme' length = "
			+ std::to_string(mangled_sql_names.length()) + ", 'sql_catalog_bid' length = "
			+ std::to_string(pool_indices.length()));
	}
	if (mangled_sql_names.empty()) {
		throw std::runtime_error("BBP SQL catalog map columns ('sql_catalog_nme', pool index "
			+ std::to_string(mangled_sql_names.index_in_pool()) + " and 'sql_catalog_bid', pool index "
			+ std::to_string(pool_indices.index_in_pool()) + ") are empty");
	}

	auto dupe_handler =
		[&pool](const auto& mangled_name, const auto& pool_index, auto& existing_catalog_entry) {
			auto& existing_entry_pool_index = existing_catalog_entry.second;

			// let's at least try to make a sane choice (since we actually do see these pseudo-dupes in the wild
			if (pool[pool_index].empty() and pool[existing_entry_pool_index].empty() ) { return; }
			else if (not pool[pool_index].empty() and pool[existing_entry_pool_index].empty() ) {
				// Our newer pair has preference, let's replace the older pair
				existing_entry_pool_index = pool_index;
			}
			else {
				auto &oss = util::detail::get_ostringstream();
				oss << "Different buffer pool columns (pool indices " << pool_index << " and " << existing_entry_pool_index
					<< ") have the same mangled SQL column name \"" << mangled_name << "\"";
				throw std::logic_error(oss.str());
			}
		};
	auto only_insert_valid_columns =
		[&pool](string_view, buffer_pool::index_type pool_index) {
			return pool[pool_index].is_valid();
		};
	// We're assuming pool_indices contains numbers we can safely cast to buffer_pool::index_type
	return make_map<string_view, buffer_pool::index_type>(
		mangled_sql_names, pool_indices, dupe_handler, only_insert_valid_columns);
}

template <typename ColumnMap, typename SchemaNameByIdMap>
auto make_tables_map(
	const ColumnMap&                            necessary_system_tables,
	const SchemaNameByIdMap&                    schema_name_by_id,
	const std::unordered_set<sql_schema_id_t>&  schemata_to_skip )
{
	auto ids          = necessary_system_tables.at({ "sys" , "_tables"  , "id"        });
	auto schema_ids   = necessary_system_tables.at({ "sys" , "_tables"  , "schema_id" });
	auto names        = necessary_system_tables.at({ "sys" , "_tables"  , "name"      });
	auto queries      = necessary_system_tables.at({ "sys" , "_tables"  , "query"     });

	if (ids.length()        != names.length() or
		names.length()      != schema_ids.length() or
		schema_ids.length() != queries.length() )
	{
		throw std::invalid_argument(
			"The stored columns of the \"sys._tables\" table are not all of the same length.");
	}
	static std::unordered_map<sql_table_id_t, std::tuple<string_view, string_view>> tables_map;

	for(column_proxy::index_type i = 0; i < ids.length(); i++) {

		auto id        = unerase_type< sql_table_id_t  >(ids        [i]);
		auto name      = unerase_type< string_view     >(names      [i]);
		auto schema_id = unerase_type< sql_schema_id_t >(schema_ids [i]);
		auto query     = unerase_type< string_view     >(queries    [i]);

		if (not query.empty()) {
			// Tables with associated queries are never persisted
			continue;
		}
		if (schemata_to_skip.find(schema_id) != schemata_to_skip.end()) {
			// Tables in the "tmp" schema (e.g. tmp._tables, tmp.keys, tmp.triggers and others);
			// are never persisted, so no columns in the pool correspond to them
			continue;
		}
		auto schema_name = schema_name_by_id.at(schema_id);
		auto insert_succeeded =
			tables_map.insert(std::make_pair(id, std::make_tuple( name, schema_name))).second;
		if (not insert_succeeded) {
			throw std::logic_error("Duplicates encountered when examining the \"sys._tables\" table");
		}
	}
	return tables_map;
}

/**
 * @note not merely a wrapper around a map lookup with exception on failure:
 * The keys are manged.
 *
 * @note The other direction of this operation is not immediately possible
 * due to the non-injective name mangling.
 */
static buffer_pool::index_type safe_lookup_in_sql_catalog(
	const pool_sql_catalog_map  catalog,
	sql_column_name             column_name)
{
	auto it = catalog.find(mangle(column_name));
	if (it == catalog.end()) {
		throw std::logic_error("Column \"" + static_cast<std::string>(column_name) + "\" is missing from the buffer pool's SQL catalog");
	}
	return it->second;
}

static auto get_system_tables(
	const buffer_pool&           pool,
	const pool_sql_catalog_map&  sql_catalog_map)
{
	std::vector<sql_column_name> relevant_columns_of_system_tables {
		{ "sys" , "schemas"  , "id"        },
		{ "sys" , "schemas"  , "name"      },
		{ "sys" , "_tables"  , "id"        },
		{ "sys" , "_tables"  , "name"      },
		{ "sys" , "_tables"  , "schema_id" },
		{ "sys" , "_tables"  , "query"     }, // tables with associated queries are not persisted
		{ "sys" , "_columns" , "name"      },
		{ "sys" , "_columns" , "table_id"  },
	};

	std::unordered_map<sql_column_name, const column_proxy, sql_column_name::hasher > system_tables { };
	std::transform(
		std::cbegin(relevant_columns_of_system_tables),
		std::cend(relevant_columns_of_system_tables),
		std::inserter(system_tables, system_tables.end()),
		[&](const auto& col_name){
			auto index_in_pool = safe_lookup_in_sql_catalog(sql_catalog_map, col_name);
			return std::make_pair(col_name, pool[index_in_pool]);
		}
	);
	return system_tables;
}

void buffer_pool::populate_sql_name_bimap()
{

	auto pool_sql_catalog = get_sql_catalog_map(*this);
		// If the catalog BATs used injectively-mangled named, we wouldn't need all of the other stuff here...
	if (pool_sql_catalog.empty()) {
		throw std::runtime_error("Got an empty BBP SQL catalog (name -> index) map. Either the BBP is corrupt on disk or we have some bug.");
	}

	auto necessary_system_tables = get_system_tables(*this, pool_sql_catalog);

	auto schema_ids         = necessary_system_tables.at({ "sys" , "schemas"  , "id"   });
	auto schema_names       = necessary_system_tables.at({ "sys" , "schemas"  , "name" });
	optional<sql_schema_id_t> tmp_schema_id {};
	auto schema_name_by_schema_id =
		make_map<sql_schema_id_t, string_view>(schema_ids, schema_names,
			[&](const auto& schema_id, const auto& schema_name, const auto& existing_map_entry) {
				auto &oss = util::detail::get_ostringstream();
				oss << "The same schema id ( " << schema_id
					<< ") is associated with multiple schema names: \"" << schema_name << "\" and \""
					<< existing_map_entry.second << "\" in the sys.schemas table "
					<< "persisted in this buffer pool";
				throw std::logic_error(oss.str());
			},
			[&](const auto& index, const auto& name) {
				if (name == "tmp") { tmp_schema_id = index; }
				return true;
			}
		);
	// We assume we have indeed set the value of tmp_schema_id

	std::unordered_set<sql_schema_id_t> schemata_to_discard;
	if (has_value(tmp_schema_id)) { schemata_to_discard.insert(tmp_schema_id.value()); }
	auto tables_info_map = make_tables_map(
		necessary_system_tables, schema_name_by_schema_id, schemata_to_discard);

	name_vis_index_bimap_.emplace();
		// So now it's not nullopt, it's a pair of empty structures we can use.
	name_vis_index_bimap_.value().sql_name_by_column_index.resize(size());
		// On the vector side, we need one entry for every column in the pool.

	auto sys_columns_table_ids = necessary_system_tables.at({ "sys", "_columns", "table_id" });
	auto sys_columns_names     = necessary_system_tables.at({ "sys", "_columns", "name" });

	for(column_proxy::index_type i = 0; i < sys_columns_names.length(); i++) {

		auto column_name_within_table = unerase_type< string_view    >(sys_columns_names     [i]);
		auto table_id                 = unerase_type< sql_table_id_t >(sys_columns_table_ids [i]);

		auto table_info_it = tables_info_map.find(table_id);
		if (table_info_it == tables_info_map.end()) {
			// We're assuming this is an irrelevant table (rather than, say, data corruption...
			continue;
		}
		string_view table_name, schema_name;
		std::tie(table_name, schema_name) = table_info_it->second;
		auto full_column_name = sql_column_name(
			schema_name.data(), table_name.data(), column_name_within_table.data()
		);
		auto it = pool_sql_catalog.find(mangle(full_column_name));
		if (it == pool_sql_catalog.end()) {
			// throw std::logic_error("A registered SQL column has no storage within the buffer pool.");
			std::cout << "Column " << full_column_name << " has no BBP storage.\n";
			continue;
		}
		else {
			auto column_index_in_pool = it->second;
			name_vis_index_bimap_.value().sql_name_by_column_index[column_index_in_pool].emplace( full_column_name );
			name_vis_index_bimap_.value().column_index_by_sql_name.insert({ full_column_name, column_index_in_pool });
		}
	}
}

template
optional<buffer_pool::index_type>
buffer_pool::find_column<column_name_kind::logical>(const column_name<column_name_kind::logical>& name) const;
template
optional<buffer_pool::index_type>
buffer_pool::find_column<column_name_kind::physical>(const column_name<column_name_kind::physical>& name) const;

} // namespace gdk

} // namespace monetdb
