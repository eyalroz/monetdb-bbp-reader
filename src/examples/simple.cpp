#include "monetdb-buffer-pool/buffer_pool.h"

#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <stdexcept>
#include <iostream>
#include <string>

using buffer_pool_t = monetdb::gdk::buffer_pool;
using sql_column_name_t = monetdb::sql_column_name;
using column_name_kind_t = monetdb::column_name_kind;
using namespace std::string_literals;

enum { max_number_of_elements_to_print = 20 };

[[noreturn]] bool die(const std::string& message)
{
	std::cerr << message << std::endl;
	exit(EXIT_FAILURE);
}

int main(int argc, char** argv)
{
	(argc == 4) or die("Usage: "s + argv[0] + " <db path> <table name> <column name>");
	auto db_path      = argv[1];
	auto table_name   = argv[2];
	auto column_name  = argv[3];

	auto full_column_name = sql_column_name_t(table_name, column_name);

	auto buffer_pool = buffer_pool_t(db_path);

	std::cout
		<< "DB persistence layout version: 0" << std::oct << buffer_pool.version()
		<< " (decimal " << std::dec << buffer_pool.version() << ")\n"
		<< "This BAT Buffer pool contains " << buffer_pool.size() << " columns.\n";

	auto index_in_pool =  buffer_pool.find_column(full_column_name);
	index_in_pool or die ("Couldn't find column \"" + std::string(full_column_name) + "\"");

	auto column = buffer_pool[index_in_pool.value()];
	auto column_type_name = monetdb::gdk::type_name(column.type());
	std::cout
		 << "Column index in pool: " << index_in_pool << '\n'
		 << "Physical name:        " << column.name<column_name_kind_t::physical>() << '\n'
		 << "Logical name:         " << column.name<column_name_kind_t::logical>() << '\n'
		 << "SQL name:             " << column.name<column_name_kind_t::sql>() << '\n'
		 << "Element count:        " << column.length() << '\n'
		 << "Type:                 " << column_type_name << '\n';

	if (column_type_name == "int"s) {
		auto as_typed_span = column.as_span<int>();
		auto num_elements_to_print = std::min<size_t>(
			as_typed_span.size(), max_number_of_elements_to_print );
		std::cout << "\nFirst " << num_elements_to_print  << " elements: ";
		for(auto x : column.as_span<int>()) {
			std::cout << x << ' ';
			if (--num_elements_to_print == 0) break;
		}	
		std::cout << '\n';
	}
	return EXIT_SUCCESS;
}
