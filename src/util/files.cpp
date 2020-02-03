#include "util/files.h"
#include "util/string.hpp"
#include "util/optional.hpp"

#include <ext/stdio_filebuf.h>
#include <pwd.h>
#include <cstdio>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <climits>
#include <unistd.h>
#include <sstream>
#include <fstream>
#include <stdexcept>
#ifdef DEBUG
#include <cstring>
#include <iostream>
#endif

namespace monetdb {

namespace util {

namespace detail {

/* Code due to this StackOverflow answer:
 * http://stackoverflow.com/a/19749019/1593077
 * Note it will probably fails for non-GNU compilers
 ****************************************************
 */
typedef std::basic_ofstream<char>::__filebuf_type file_buffer_it;
typedef __gnu_cxx::stdio_filebuf<char> io_buffer_t;
static FILE* cfile_impl(const file_buffer_it* fb){
	auto io_buf { const_cast<file_buffer_it*>(fb) };
	return (static_cast<io_buffer_t*>(io_buf))->file(); //type std::__c_file
}

static FILE* cfile(std::ofstream const& ofs) {return cfile_impl(ofs.rdbuf());}
static FILE* cfile(std::ifstream const& ifs) {return cfile_impl(ifs.rdbuf());}

static FILE* cfile(std::ostream const& os)
{
	if(std::ofstream const* ofsP = dynamic_cast<std::ofstream const*>(&os)) return cfile(*ofsP);
	if(&os == &std::cerr) return stderr;
	if(&os == &std::cout) return stdout;
	if(&os == &std::clog) return stderr;
	if(dynamic_cast<std::ostringstream const*>(&os) != 0){
	   throw std::runtime_error("don't know cannot extract FILE pointer from std::ostringstream");
	}
	return nullptr; // stream not recognized
}

static FILE* cfile(std::istream const& is)
{
	if(std::ifstream const* ifsP = dynamic_cast<std::ifstream const*>(&is)) return cfile(*ifsP);
	if(&is == &std::cin) return stdin;
	if(dynamic_cast<std::ostringstream const*>(&is) != 0){
		throw std::runtime_error("don't know how to extract FILE pointer from std::istringstream");
	}
	return nullptr; // stream not recognized
}

} // namespace detail

/*
 ****************************************************
 */

namespace detail {
// TODO: Avoid having to ioctl every time; either
// run this just once by the app, or perhaps
// allow static caching here of the results? Hmm.
optional<unsigned> get_terminal_width(int file_descriptor)
{
	struct winsize w;
	auto result = ioctl(file_descriptor, TIOCGWINSZ, &w);
	if (result != 0) {
#ifdef DEBUG
		std::cout << std::flush;
		std::cerr << "ioctl TIOCGWINSZ for file descriptor " << file_descriptor
			<< " failed. Reported error: " << std::strerror(errno) << '\n' << std::flush;
#endif
		return optional<unsigned>();
	}
	return w.ws_col;
}

optional<unsigned> get_terminal_width(FILE* file)
	{
	auto fd = fileno(file);
	if (!isatty(fd)) { return nullopt; }
	return get_terminal_width(fd);
}

} // namespace detail

optional<unsigned> get_terminal_width(std::ostream& os) {
	try {
		return detail::get_terminal_width(detail::cfile(os));
	}
	catch(std::exception& e) {
		return optional<unsigned>();
	}
}

optional<unsigned> get_terminal_width()
{
	using namespace detail;
	using std::exception;
	try { return get_terminal_width(cfile(std::cout)); } catch(exception& e) { };
	try { return get_terminal_width(cfile(std::cerr)); } catch(exception& e) { };
	try { return get_terminal_width(cfile(std::cin));  } catch(exception& e) { };
	// We could try some system calls here, or opening /dev/tty -
	// but that would be quite unportable, I think, so let's
	// just give up.
	return nullopt;
}

optional<filesystem::path> get_home_directory() {
	struct passwd *pw = getpwuid(getuid());
		// Note: this is not newly-allocated memory and we shouldn't free it
	if (pw == nullptr) { return { }; }
	return filesystem::path(pw->pw_dir);
}


std::string file_contents(const std::string& path)
{
	std::ifstream ifs(path);
	std::stringstream ss;
	ss << ifs.rdbuf();
	return ss.str();
}

bool is_recursable(const filesystem::path& path)
{
	if (not filesystem::is_directory(path)) { return false; }
	try {
		auto di = filesystem::directory_iterator(path);
		return true;
	}
	catch (std::exception& e) {
		return false;
	}
}

bool is_readable(const filesystem::path& path)
{
    std::ifstream infile(path.string());
    return infile.good();
//	return (access(path.string.c_str(), R_OK) == 0);
}

} // namespace util

} // namespace monetdb

