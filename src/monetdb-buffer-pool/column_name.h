#pragma once
#ifndef MONETDB_COLUMN_NAMES_H_
#define MONETDB_COLUMN_NAMES_H_

#include <functional>
#include <string>

#include <ostream>
#include <string>
#include <cstddef> // for size_t
using std::size_t;

namespace monetdb {

/*
 * Some exampels of BBP column name combinations, to illustrate what the kinds of names actually means:
 *
 *
 * Logical Name:  M5system_auth_passwd_v2  Physical name: 4           SQL name: (unset)
 * Logical Name:  tmp_124                  Physical name: 01/124      SQL name: sys.args.type_scale
 * Logical Name:  stat_opt_timings         Physical name: 05/557      SQL name: (unset)
 * Logical Name:  tmp_700                  Physical name: 07/700      SQL name: (unset)
 * Logical Name:  tmp_701                  Physical name: 07/701      SQL name: sys.supplier.s_phone
 * Logical Name:  tmp_1121                 Physical name: 11/1121     SQL name: sys.lineitem.l_suppkey
 *
 * So, you'll note that:
 * 
 * - Physical names correspond to names of the files in the BBP directory in which columns are stored;
 *   and these can be within a subdir, or outside of a subdir. The subdir names correspond to the top
 *   digits of the basic physical "name"
 * - The "basic" part of a physical name is a number in base 8 (octal); these are assigned
 *   sequentially as columns are created over the course of a DB's lifetime.
 * - The logical name is either set to something meaningful or defaults to "tmp_" followed by the
 *   basic physical name
 * - Names are typically "conservative" in the use of weird characters - try not to be too creative.
 * - Some columns are not SQLish at all; some are, but are built-in to a DBMS and will not reflect
 *   the tables the user explicitly created; this latter kind is all in the `sys` schema.
 * - If the user doesn't specify a schema for a tible he/she creates, the `sys` schema gets used
 *   as a default value.
 *   basic physical name
 */
enum class column_name_kind { physical, logical, sql };
template <column_name_kind>
struct column_name {
	const std::string str_;
	operator const char*() const { return str_.c_str(); }
	operator std::string() const { return str_; }
	const std::string& get() const { return str_; }

	column_name(const std::string& name) : str_(name) { };
	column_name(std::string&& name) : str_(name) { };
	column_name(const column_name& other) = default;
	column_name(column_name&& other) = default;
};

template <column_name_kind Kind>
inline bool operator==(
		const column_name<Kind>& lhs,
		const column_name<Kind>& rhs)
{
	return lhs.get() == rhs.get();
}

/**
 * @note In MonetDB, all SQL columns exist within schemas; anything
 * that's "schema-less" is actually in the default schema, which is
 * named "sys" (yes, it's confusing - "system tables" are also in the
 * "sys" schema). *
 */
template <>
struct column_name<column_name_kind::sql> {
public: // constants and types
	static constexpr const char* default_schema = "sys";

	struct hasher {
        size_t operator()(const column_name<column_name_kind::sql>& n) const;
    };

public: // operators
	operator std::string() const { return schema + '.' + table + '.' + column; }

protected:
	void verify_validity() const
	{
		if (table.empty()) {
			throw std::invalid_argument("Attempt to construct an full SQL column name with an empty table");
		}
		if (column.empty()) {
			throw std::invalid_argument("Attempt to construct an full SQL column name with an empty column name coponent");
		}
	}
public: // constructors
	// TODO: Verify the validity of the input strings (e.g. no use of dots!

	column_name<column_name_kind::sql>(
		const std::string& schema_name,
		const std::string& table_name,
		const std::string& column_name)
	:
		schema(schema_name.empty() ? default_schema : schema_name),
		table(table_name),
		column(column_name)
	{
		verify_validity();
	}

	column_name<column_name_kind::sql>(
		std::string&& schema_name,
		std::string&& table_name,
		std::string&& column_name)
	:
		schema(schema_name.empty() ? default_schema : schema_name),
		table(table_name),
		column(column_name)
	{
		verify_validity();
	}
	column_name<column_name_kind::sql>(const column_name<column_name_kind::sql>& other)
	: column_name<column_name_kind::sql>(other.schema, other.table, other.column) { }

	column_name<column_name_kind::sql>(const std::string& table_name, const std::string& column_name)
    : column_name<column_name_kind::sql>(default_schema, table_name, column_name) { }

public: // data members
	std::string schema;
	std::string table;
	std::string column;
};

inline bool operator==(
	const column_name<column_name_kind::sql>& lhs,
	const column_name<column_name_kind::sql>& rhs)
{
	return
		lhs.schema == rhs.schema and
		lhs.table  == rhs.table  and
		lhs.column == rhs.column;
}

using sql_column_name = column_name<column_name_kind::sql>;

inline std::ostream& operator<<(std::ostream& os, column_name<column_name_kind::sql> scn)
{
	return os << scn.schema << '.' << scn.table << '.' << scn.column;
}

template <typename T>
inline column_name<column_name_kind::logical> make_logical_name(const T& s)
{
	return column_name<column_name_kind::logical>{ s };
}

template <typename T>
inline column_name<column_name_kind::physical> make_physical_name(const T& s)
{
	return column_name<column_name_kind::physical> { s };
}

inline sql_column_name make_sql_name(
	const std::string& schema, const std::string& table, const std::string& column)
{
	return sql_column_name {schema, table, column};
}

inline sql_column_name make_sql_name(
	const std::string& table, const std::string& column)
{
	return sql_column_name {table, column};
}

inline size_t column_name<column_name_kind::sql>::hasher::operator()(
	const column_name<column_name_kind::sql>& n) const
{
	static auto hash_combine = [](std::size_t& seed, const std::string& str) {
		std::hash<std::string> hasher;
		seed ^= hasher(str) + 0x9e3779b9 + (seed<<6) + (seed>>2);
	};
	auto result = std::hash<std::string>()(n.schema);
	hash_combine(result, n.table);
	hash_combine(result, n.column);
	return result;
}

} // namespace monetdb




#endif /* MONETDB_COLUMN_NAMES_H_ */
