#pragma once
#ifndef BUFFER_POOL_LOCK_HPP_
#define BUFFER_POOL_LOCK_HPP_

#include "util/files.h"

#include <cstring>
#include <unistd.h>
#include <stdexcept>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace monetdb {

namespace gdk {

static inline std::system_error make_system_error(const std::string& message)
{
    return std::system_error(std::error_code(errno, std::system_category()), message);
}

class global_lock_t {
protected:
	const filesystem::path lock_file_path_;
	int file_descriptor_; // make const?
	mode_t lock_file_open_mode_;

protected: // constants
	enum : off_t { lock_offset = 4, lock_length = 1 };
	static constexpr const char* lock_file_name() { return ".gdk_lock"; }
		// TODO: Do I really need to hardcode that name here, rather
		// than getting it from someplace else?

public: // constructor & destructor
	global_lock_t(const filesystem::path& db_path, mode_t lock_file_open_mode) :
		lock_file_path_(db_path / lock_file_name()), lock_file_open_mode_(lock_file_open_mode)
	{
		// The O_TEXT appears in the MonetDB code; it may well be that it's meaningless,
		// but let's be on the safe side
	#ifdef O_TEXT
		auto open_flags = O_CREAT | O_RDWR | O_TEXT ;
	#else
		auto open_flags = O_CREAT | O_RDWR;
	#endif

		file_descriptor_ = ::open(lock_file_path_.c_str(), open_flags, lock_file_open_mode_);

		if (file_descriptor_ < 0) {
			throw make_system_error(std::string("Opening the lock file ") +
				lock_file_name() + " for the Database at " + db_path.string());
		}
		if (lseek(file_descriptor_, lock_offset, SEEK_SET) != lock_offset) {
			::close(file_descriptor_);
			throw make_system_error("Seeking to offset " +
				std::to_string(lock_offset) +
				" in lock file " + lock_file_path_.string());
		}
		if (::lockf(file_descriptor_, F_TLOCK, lock_length) != 0) {
			::close(file_descriptor_);
			throw make_system_error("Non-blocking lock attempt within DB lock "
				"file " + lock_file_path_.string());

		}
		// move back to the beginning of the file
		::lseek(file_descriptor_, 0, SEEK_SET);
	}

	~global_lock_t() noexcept(false)
	{
		if (lseek(file_descriptor_, lock_offset, SEEK_SET) != lock_offset) {
			throw make_system_error("Seeking to offset " +
				std::to_string(lock_offset) +
				" in lock file " + lock_file_path_.string());
		}
		if (lockf(file_descriptor_, F_ULOCK, lock_length) != 0) {
			throw make_system_error("Seeking to offset " +
				std::to_string(lock_offset) +
				" in lock file " + lock_file_path_.string());
		}
		close(file_descriptor_);
	}
};

} // namespace gdk

} // namespace monetdb

#endif /* BUFFER_POOL_LOCK_HPP_*/
