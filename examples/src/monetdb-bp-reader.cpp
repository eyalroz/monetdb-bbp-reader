#include "monetdb-buffer-pool/column_name.h"
#include "monetdb-buffer-pool/buffer_pool.h"
#include "util/files.h"
#include "util/string.hpp"
#include "util/dump.h"
#include "util/type_name.hpp"

#include <iostream>
#include <iomanip>
#include <cstdlib> // for size_t, setlocale
#include <vector>
#include <regex>
#include <cmath>
#include <system_error>
#include <numeric>
#include <stdexcept>

using util::optional;
using std::string;
using std::flush;
using std::endl;
using std::cerr;
using std::cout;
using std::setw;

#ifdef UTIL_EXCEPTION_H_
using util::logic_error;
using util::invalid_argument;
#else
using std::logic_error;
using std::invalid_argument;
#endif

using namespace monetdb;
using gdk::buffer_pool;
using monetdb::gdk::column_proxy;

using monetdb::column_name_kind;
using monetdb::sql_column_name;

optional<std::string> translate_type_name(const char* monetdb_type_name)
{
	static const std::unordered_map<typename std::string, std::string> type_translation_for_printing = {
		{ "void",      util::type_name<void>()          },
	//	{ "bit",       "bit"                            },
		{ "bte",       util::type_name<signed char>(),  }, // should this really be signed?
		{ "sht",       util::type_name<int16_t>()       },
	//	{ "BAT",       "bat"                            },
		{ "int",       util::type_name<int>()           },
#ifdef MONET_OID32
		{ "oid",       util::type_name<unsigned int>()  },
typedef unsigned int oid;
#else
		{ "oid",       util::type_name<size_t>()        },
#endif
#if GDK_VERSION == 061033
#if SIZEOF_WRD == SIZEOF_INT
		{ "wrd",       util::type_name<int32_t>()       },
#else
		{ "wrd",       util::type_name<int32_t>()       },
#endif
#endif /* GDK_VERSION == 061033 */
	// we always have long long in C++11
		{ "ptr",      "pointer" },
		{ "flt",       util::type_name<float>()         },
		{ "dbl",       util::type_name<double>()        },
		{ "lng",       util::type_name<long long int>() },
#ifdef HAVE_HGE
#ifdef HAVE___INT128
		{ "hge",       util::type_name<hge>()      },
#else
		{ "hge",       util::type_name<__int128_t>()    },
#endif
#endif
//		{ "str",       "string"                         },
//		{ "date",      "date"                           },
//		{ "daytime",   "daytime"                        },
//		{ "timestamp", "timestamp"                      },
	};

	// using an std::string since maps and char*'s don't play well together;
	// it's possible but it ain't pretty and a waste of code for a minor
	// optimization
	std::string column_type_as_str(monetdb_type_name);
	auto it = type_translation_for_printing.find(column_type_as_str);
	if (it == type_translation_for_printing.end()) return {};
	return it->second;
}


[[noreturn]] bool die(const string& message)
{
	cerr << message << endl;
	exit(EXIT_FAILURE);
}

void list_column(buffer_pool& pool, buffer_pool::index_type index_in_pool)
{
	auto column = pool[index_in_pool];
	if (!column.is_valid()) {
		throw std::invalid_argument("Attempt to list an invalid entry in the buffer pool");
	}
	auto logical_name = column.name<column_name_kind::logical>();
	auto physical_name = column.name<column_name_kind::physical>();
	auto sql_name = column.name<column_name_kind::sql>();
	auto column_record = column.record();
	cout <<     "BBP Index: " << std::right << setw(5) << index_in_pool
	     << "  | Logical Name:  " << std::left << setw(24) <<logical_name
	     << "  | Physical name: " << std::left << setw(10) << physical_name
	     << "  | SQL name: " << std::left << sql_name
	     << (column_record == nullptr ? "  (Can't obtain column record)" : "")
	     << "\n" << std::flush;
}

void list_all_columns(
		buffer_pool& pool,
		optional<buffer_pool::index_type> num_valid_columns_to_list = {})
{
	buffer_pool::size_type num_valid_columns_encountered { 0 };

	cout << "Columns in the buffer pool (the BBP):\n";
	for(const auto& column : pool)
	{
		if (not column.is_valid()) { continue; }
		num_valid_columns_encountered++;
		if (num_valid_columns_to_list and
		    num_valid_columns_encountered >= num_valid_columns_to_list.value())
		{
			cout << "(skipping remaining columns)"  << endl;
			continue;
		}
		if (not column.name<column_name_kind::physical>()) {
			die("Shouldn't get here - a valid column with no physical name");
		}
		list_column(pool, column.index_in_pool());
	}
	cout  << "\n"
	      << "Number of buffer pool entries                    " << pool.size() << "\n"
	      << "Number of _valid_ entries (with actual columns)  " << num_valid_columns_encountered << "\n"
	      << endl;

}

std::pair<filesystem::path, optional<sql_column_name>> parse_cmdline(int argc, char** argv)
{
	std::string column_physical_name;
	auto binary_name = util::leaf_of(filesystem::path(argv[0]));
	if (not (argc == 2 or argc == 4)) {
		die(std::string("Usage:\n") +
			binary_name.string() + " DATABASE_ROOT_FOLDER [TABLE_NAME COLUMN_NAME]");
	}
	filesystem::path db_path(argv[1]);
	if (argc == 2) { return { db_path, { } }; }
	auto column_name = sql_column_name{ argv[2], argv[3] };
	return { db_path, column_name };
}

void enable_all_locales()
{
	// GDK needs to use UTF-8 locals, and apparently calling the following is
	// necessary to make those available (see man setlocale or the POSIX standard
	// for details)
	if (setlocale(LC_CTYPE, "") == NULL) {
		die("Failed setting the LC_CTYPE locale to empty");
	}
}

/**
 * This doesn't dump a heap's contents, only lists meta-data about it.
 */
void list_heap(const std::string& title, const Heap& heap, unsigned indent_level = 0)
{
	static const std::unordered_map<typename std::underlying_type<storage_t>::type, std::string> storage_mode_names = {
		{ STORE_MEM,     "GDKmalloc'ed memory"                        },
		{ STORE_MMAP,    "mmap'ed virtual memory"                     },
		{ STORE_PRIV,    "mmap'ed virtual memory with copy-on-write"  },
#if GDK_VERSION >= 061035
		{ STORE_CMEM,    "non-GDK malloc'ed memory"                   },
		{ STORE_NOWN,    "memory not owned by the BAT"},
		{ STORE_MMAPABS, "mmap'ed virtual memory - using an absolute"
		                 "path (not relative to the DB farm)"         },
#endif
		{ STORE_INVALID, "invalid storage mode value"                 },
	};

	auto indent = std::string(indent_level, '\t');
	cout << indent << title << ":\n";
	indent = std::string(indent_level+1, '\t');
	// I'm sure there's a better way to do this, but:
	auto storage_filename = (heap.filename == nullptr ?
		"(none)" : (strlen(heap.filename) == 0 || strcmp(heap.filename, str_nil) == 0) ?
	    	"(none)" :
			heap.filename
		);
	//
 	cout << indent << "Virtual size (in bytes):                 " << heap.size << "\n"
 	     << indent << "Actual memory taken up (in bytes):       " << (heap.storage == STORE_MEM ? heap.size : 0) << "\n"
	     << indent << "Base pointer:                            " << (void*) heap.base << "\n"
 	     << indent << "Storage filename:                        " << storage_filename << "\n"
 	     << indent << "Storage mode:                            " << storage_mode_names.at(heap.storage) << "\n"
	     << indent << "Offset in bytes of the heap 'free area': " << ((char*) heap.free - (char*) heap.base) << "\n"
 	     ;
}

void list_hash(const std::string& title, const Hash& hash_table, unsigned indent_level = 0)
{
	auto indent = std::string(indent_level, '\t');
	cout << indent << title << ":\n";
	indent = std::string(indent_level+1, '\t');
 	cout << indent << "Type:                       " << gdk::type_name(hash_table.type) << "\n"
	     << indent << "Hash entry width:           " << hash_table.width  << "\n"
 	     << indent << "NULL value representation:  " << hash_table.nil<< "\n"
 	     << indent << "Collision limit size:       " << hash_table.lim << "\n"
 	     << indent << "Number of hash buckets - 1  " << hash_table.mask << "\n"
 	     << indent << "Hash base pointer           " << hash_table.Hash << "\n"
 	     << indent << "Collision list 'link'       " << hash_table.Link << "\n"
 	     ;
}

void list_column_record(const std::string& title, const COLrec* column_record, unsigned indent_level = 0)
{
	auto indent = std::string(indent_level, '\t');
	cout << indent << title << ":";
	if (column_record == nullptr) { cout << " (null)\n"; return; }
	else { cout << "\n"; }
	indent = std::string(indent_level+1, '\t');
	cout << indent << "Id:                        " << column_record->id << "\n";
	cout << indent << "Type:                      " << gdk::type_name(column_record->type) << " ("
	                                                << std::to_string(column_record->type) << ")\n";
	cout << indent << "Element ('atom') width:    " << std::to_string(column_record->width) << "\n";
	cout << indent << "Key (2 bits):              " << column_record->key << "\n";
	cout << indent << "Nulls:                     " << (column_record->nonil ? "Non-null column" : column_record->nil ? "known to have nulls" : "no guarantees") << "\n";
	cout << indent << "Sortedness:                " << ((bool) column_record->sorted ? "ascending" : (bool) column_record->revsorted ? "descending" : "unsorted") << "\n";
	bool is_dense = monetdb::gdk::is_dense(column_record);
	cout << indent << "Dense:                     " << is_dense;
 	if (is_dense) {
 		cout << " (sequence starting at ";
 		if (column_record->seq == oid_nil) { std::cout << "(nil)"; }
 		else { std::cout << column_record->seq; }
 		std::cout << " )";
 	}
 	cout << "\n";
 	if (!is_dense ) {
 		list_heap("Main heap", column_record->heap, indent_level + 1);
 	}
 	if (column_record->hash) {
 		list_hash("Hash table for the variable-size heap", *column_record->hash, indent_level + 1);
 	}
 	if (column_record->vheap != nullptr) {
 		list_heap("Variable-size heap", *(column_record->vheap), indent_level + 1);
 	}
}

void list_column_fully(const column_proxy& column)
{
	auto column_type = column.type();
	cout << std::boolalpha
	     << "Persisted column listing \n"
	     << "-------------------------\n"
	     << "BBP entry index:    " << column.index_in_pool() << "\n"
	     << "Physical name:      " << column.name<column_name_kind::physical>() << "\n"
	     << "Logical name:       " << column.name<column_name_kind::logical>() << "\n"
	     << "SQL name:           " << column.name<column_name_kind::sql>() << "\n"
	     << "Length in elements: " << column.length() << "\n"
	     << "Capacity:           " << column.allocated_capacity() << "\n"
	     << "Width:              " << column.width() << '\n'
	     << "Element Type:       " << gdk::type_name(column_type) << " (" << (int) column_type << ")\n";
	cout << flush;
	list_column_record("Tail", column.record());
}


namespace util {
// This comes from dump.h ... ugly secret dependence, fix it
void sanitize_to(std::ostream& os, char c, unsigned int field_width = 0);
}

// TODO: Super-ugly code here, sorry
namespace detail {

template <typename Iterator>
inline void sanitize_to(std::ostream& os, Iterator begin, Iterator end, unsigned int field_width = 0)
{
	std::for_each(begin,end, [&os, field_width](typename std::add_const<decltype(*begin)>::type & e) {
		util::sanitize_to(os, e, field_width);
	});
}

template <typename Iterator>
inline std::string sanitize(Iterator begin, Iterator end, unsigned int field_width = 0) {
    auto& oss = util::detail::get_ostringstream();

    sanitize_to(oss, begin, end, field_width);
    return oss.str();
}

} // namespace detail


void dump_string_column(
	const column_proxy       column,
	const size_t             max_num_element_to_print,
	util::dump_parameters_t  dump_params)
{
	enum { default_num_elements_to_print = 1024 };

	auto width = column.width();
	// So, the data doesn't necessarily start there - it's just the "first"
	// record; but for our purposes we can say that it does

	std::string tail_type_name;
	switch(width) {
	case 1: tail_type_name = util::type_name<uint8_t>();  break;
	case 2: tail_type_name = util::type_name<uint16_t>(); break;
	case 4: tail_type_name = util::type_name<uint32_t>();  break;
	case 8: tail_type_name = util::type_name<uint64_t>(); break;
	default:
		throw invalid_argument("Dump requested for a string column "
			"with unsupported tail width: " + std::to_string(width));
	}
	//util::dump(cout, column.at(0), tail_type_name, count, "Tail data - offsets into the vheap", dump_params);
	cout << '\n';

	auto max_num_strings_to_print = std::min(max_num_element_to_print, dump_params.subrange_to_print.length());
	cout << "Tail data - strings\n"
	     << "-------------------\n";
	if (max_num_element_to_print == 0) { cout << "(No strings)\n\n"; return ;}
	else if (max_num_element_to_print == 1) {
		cout << "There is 1 string. Printing it as ASCII with hex-escapes.\n";
	}
	else {
		cout << "There are " << max_num_element_to_print << " string(s). Printing them as ASCII with hex-escapes.\n";
	}
	if (max_num_strings_to_print < max_num_element_to_print) {
		cout << "Printing " << max_num_strings_to_print << " elements - [ 0.." << max_num_strings_to_print-1 << " ] :\n";
	}
	cout << "\n";
	size_t max_length = std::accumulate(
		column.cbegin(),  column.cbegin() + max_num_strings_to_print, (size_t) 0,
		[](size_t ml, const void* ptr) {
			auto str = static_cast<const char*>(ptr);
			return std::max(ml, std::strlen(str));
		}
	);
	size_t index_width = std::ceil(std::log(max_num_strings_to_print) / std::log(10));
	size_t element_index = 0;
	for(const void* ptr : column) {
		if (element_index >= max_num_strings_to_print) { break; }
		auto str = (const char*) ptr;
		auto length = strlen(str);
		cout << ' ' << std::right << setw(index_width) << element_index << ": "
		     << setw(max_length+2) << std::left << ('"' + detail::sanitize(str, str + length) + '"')
		     << "  (length " << std::right << setw(2) << length << ")\n";
		element_index++;
	}
}

void dump_string_column(
	const column_proxy     column,
	util::dump_parameters_t  dump_params = {})
{
	dump_string_column(column, column.length(), dump_params);
}

void dump_column(
	const column_proxy& column,
	optional<size_t> max_num_elements_to_print = {})
{

	enum { default_num_elements_to_print = 1024 };
	size_t count = column.length();

	if (count == 0) {
		cout << "BAT (Column) " << column.index_in_pool() << " is empty (has count = 0)\n";
		return;
	}

	auto tail_type = column.type();

	util::dump_parameters_t dump_params;
	dump_params.subrange_to_print.set_length(
		max_num_elements_to_print.value_or(default_num_elements_to_print)
	);

	auto tail_type_name = gdk::type_name(tail_type);

	// Currently, "str" is the only variable-width type supported by the library,
	// so it's the only one that gets special-cased.
	if (std::string(tail_type_name) == "str") {
		dump_string_column(column, dump_params);
		return;
	}
	auto translated_tail_type_name = translate_type_name(tail_type_name);
	if (not has_value(translated_tail_type_name)) {
		cout << "Not listing any elements, since the tail type is "
		     << tail_type_name << " (" << (int) tail_type << ") - "
		     << "which we can't translate into a type we know how to dump.\n";
		return;
	}
	util::dump(cout, column.at(0), translated_tail_type_name.value(), count, "Tail data", dump_params);
}

void list_and_dump_all_columns_fully(buffer_pool& pool)
{
	for(auto column : pool) {
		if (not column.is_valid()) {
			std::cout << "Skipping invalid column at index " << column.index_in_pool();
		}
		else if (not column.name<column_name_kind::physical>()) {
			std::cout << "Skipping column with no physical name at index " << column.index_in_pool();
		}
		else {
			cout << '\n';
			list_column_fully(pool[column.index_in_pool()]);
			cout << endl;
			dump_column(column);
			cout << "\n"
			     << "---------------------------------------------------------------------------\n";
		}
		cout << endl;
	}
}

buffer_pool get_buffer_pool(const std::string& db_path)
{
	try { return buffer_pool(db_path); }
	catch(std::system_error& se) {
		if (se.code() == std::error_code( EAGAIN, std::system_category() )) {
			std::cerr << se.what() << "\n... you probably need to stop the running instance of MonetDB for this DB.\n";
			exit(EXIT_FAILURE);
		}
		throw se;
	}
}

int main(int argc, char** argv)
{
	auto path_and_name = parse_cmdline(argc, argv);
	auto db_path { std::get<0>(path_and_name) } ;
	optional<sql_column_name> column_name {std::get<1>(path_and_name) } ;

	enable_all_locales();

	cout << "Retrieving persisted data from the MonetDB database at: " << db_path.string() << "\n";
	cout << "Reader library version:  0" << std::oct << buffer_pool::library_version()
	     << " (decimal: " << std::dec << buffer_pool::library_version() << ")\n";

	try {
		auto bbp = get_buffer_pool(db_path.string());

		auto version = bbp.version();
		cout
			<< "DB library version:      0" << std::oct << version
			<< " (decimal: " << std::dec << version << ")\n\n";

		if (column_name) {
			auto bat_index_in_db_pool = bbp.find_column(column_name.value());
			if (!bat_index_in_db_pool) {
				cout << "Couldn't find a column named \"" << column_name << "\" in the BBP.\n";
				return EXIT_FAILURE;
			}
			list_column_fully(bbp[bat_index_in_db_pool.value()]);
			cout << '\n';
			dump_column(bbp[bat_index_in_db_pool.value()]);
		}
		else {
			list_all_columns(bbp);
			cout << '\n';
			list_and_dump_all_columns_fully(bbp);
		}
		return EXIT_SUCCESS;
	}
	catch(const std::invalid_argument& e) {
		std::cerr
			<< "Cannot read from a persisted MonetDB database at " << db_path << ":\n"
			<< e.what() << '\n';
		return EXIT_FAILURE;
	}
}

