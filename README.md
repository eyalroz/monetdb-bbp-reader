# MonetDB BBP Database persistence format reader library

A small C++14 library for reading SQL [columns](https://www.monetdb.org/content/column-store-features) within persisted [MonetDB](https://www.monetdb.org/) databases.

## What is this library for? ##

[MonetDB](https://www.monetdb.org/) is the de-facto standard analytics-oriented, columnar, relational, Free Software DBMS. MonetDB's persistence format is, admittedly, a bit contrived (since it is also used in-memory - most of it is mmap()ed as-is); but it's a neat little semi-standard storage format - for non-textual columnar relational data, unless you're living in the Hadoop-and-Spark world. you want to experiment with columnar data and not be laden with the entire DBMS (or with other large software systems like R etc.) 

## Example usage ##

A small program is worth a thousand words... :-)

```
#include "monetdb-buffer-pool/buffer-pool.h"

#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <exception>
#include <iostream>
#include <string>

using buffer_pool_t = monetdb::gdk::buffer_pool_t;
using sql_column_name_t = monetdb::sql_column_name_t;
using column_name_kind_t = monetdb::column_name_kind_t;
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
	auto type_name = monetdb::gdk::type_name(column.type());
	std::cout
		 << "Column index in pool: " << index_in_pool << '\n'
		 << "Physical name:        " << column.name<column_name_kind_t::physical>() << '\n'
		 << "Logical name:         " << column.name<column_name_kind_t::logical>() << '\n'
		 << "SQL name:             " << column.name<column_name_kind_t::sql>() << '\n'
		 << "Element count:        " << column.length() << '\n'
		 << "Type:                 " << type_name << '\n';

	if (type_name == "int"s) {
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
```

Notes:

* The MonetDB server process for the DB you're reading most not be running. If it is running, the library will fail to obtain a lock and throw an exception.
* Only one version of MonetDB is supported at any given time (or rather, one version of MonetDB's GDK library; occasionally, subsequent MonetDB versions are compatible enough not to bump the GDK library's version number).
* Not all versions of MonetDB (/GDK) are supported; but the latest ones are, at the time of writing.
* There's not much support in the library for variable-width types other than by using the lower-level, MonetDB-specific data structures (through the `.record()` method of column proxies); but you can have a look at `src/examples/monetdb-bp-reader.cpp` which prints the contents of string columns to the console - to see how it's done.

## How do I get set up? ##

### Hardware prerequisites ###

* An x86_64 machine capable of running GNU/Linux - hopefully this restriction can be lifted soon.

### Software prerequisites ###

* CMake 3.1
* GCC 5.4.0 or up (probably any version starting from 5.0 should work really, and clang or MSVC may also work - but I haven't tested)
* A GNU/Linux x86_64 operating system - hopefully a temporary restriction.
* A recent MonetDB version, e.g. 11.33.3. Download it:
  * as compiled binaries from the [Download section](https://www.monetdb.org/downloads/), or
  * as a source tarball from [here](https://www.monetdb.org/downloads/sources/), then [build from sources](https://www.monetdb.org/wiki/MonetDB:Building_from_sources)

### Building ###

Just use CMake to configure, generate build files and build. Both in-source and out-of-source building is supported.

The library will be built as `$BUILD_DIRECTORY/lib/monetdb-bbp-reader.a`, with some generated include files under `$BUILD_DIRECTORY/include` in addition to those under `src/` 

Installation is currently not supported, so that part is on you for now.


## Who do I talk to? ##


If you have...

* **A bug / feature request** -> File a new issue in the [Issues section](https://bitbucket.org/eyalroz/monetdb-bbp-reader/issues?status=new&status=open). 
* A question about **MonetDB itself** -> Try one of the [MonetDB mailing lists](https://www.monetdb.org/Developers/Mailinglists).
* Feedback questions or other interest in the **reader library** -> Write [Eyal](mailto:eyalroz@technion.ac.il), the developer and maintainer.

## Licensing ##

This library as a whole is available under a [BSD 3-clause license](LICENSE). However, some code was obtained from other sources, and is covered by other licenses - mostly (or solely) the MonetDB code. The original MonetDB code is available from the MonetDB website in its original form; and the modified form here is made available under MPL 2.0. Please contact [Eyal](mailto:eyalroz@technion.ac.il) if further clarification is necessary or for the possibility of use under a different license.
