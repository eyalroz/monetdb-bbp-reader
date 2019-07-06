/**
 * @file Utility code for working with files, tty's and paths.
 * It should really be switched to relying on the standard library's
 * <filesystem> header, but that doesn't go into the standard until
 * C++17 and we're mostly C++11 here
 *
 */
#pragma once
#ifndef SRC_UTIL_FILES_H_
#define SRC_UTIL_FILES_H_

#include "util/optional.hpp"

#if __cplusplus >= 201701L
#include <filesystem>
namespace filesystem = std::filesystem;
#elif __cplusplus >= 201402L
#include <experimental/filesystem>
namespace filesystem = std::experimental::filesystem;
#else
#include <boost/filesystem.hpp>
#define SRC_UTIL_FILES_USING_BOOST
namespace filesystem = boost::filesystem;
#endif


#include <iostream>
#include <string>
#include <vector>

namespace util {

optional<unsigned> get_terminal_width(std::ostream& os);
optional<unsigned> get_terminal_width();

optional<filesystem::path> get_home_directory();

/**
 * Return the entire contents of a(n ASCII?) file as a string
 *
 * @param path The file whose contents is to be read
 * @return a string with the entire contents of the specified file
 */
std::string file_contents(const std::string& path);

/**
 * Check if a path is a recursable (= searchable) directory
 *
 * @param path a path
 * @return true if @p path is a recursable directory, false otherwise
 * (including if @p path is not at all a directory or doesn't exist)
 */
bool is_recursable(const filesystem::path& path);

/**
 * Check if a path is a readable file (or readable-anything)
 */
bool is_readable(const filesystem::path& path);

inline auto leaf_of(const filesystem::path& path) {
#ifdef SRC_UTIL_FILES_USING_BOOST
	return path.leaf();
#else 
	return path.filename();
#endif
}

} /* namespace util */

#endif /* SRC_UTIL_FILES_H_ */
