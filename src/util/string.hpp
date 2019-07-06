#pragma once
#ifndef UTIL_STRING_HPP_
#define UTIL_STRING_HPP_

/**
 * This file contains string-related utility code.
 */

#include <cctype>
#include <iostream>
#include <sstream>
#include <cstring>
#include <string>
#include <iomanip>
#include <algorithm>
#include <streambuf>
#include <type_traits>
#include <stdexcept>
#include <vector>
#include <iterator>
#include <algorithm>

namespace util {

/**
 * A RAII gadget to make sure functions which modify streams' ios flags
 * restore them when returning.
 *
 * TODO: Consider having functions use this class as a parameter, wrapping
 * the stream (with an appropriate ctor and getter)
 */
class ios_flags_saver {
public:
    explicit ios_flags_saver(std::ostream& _ios): ios(_ios), format_flags(_ios.flags()) { }
    ~ios_flags_saver() { ios.flags(format_flags); }

    ios_flags_saver(const ios_flags_saver &rhs) = delete;
    ios_flags_saver& operator= (const ios_flags_saver& rhs) = delete;

private:
    std::ostream& ios;
    std::ios::fmtflags format_flags;
};

inline void print_variadic_args(std::ostream&) { }

template <typename T>
inline void print_variadic_args(std::ostream& os, T t) { os << t; }

template <typename T, typename U, typename... Args>
inline void print_variadic_args(std::ostream& os, T t, U u, Args... args)
{
    os << t << ", ";
    print_variadic_args(os, u, args...);
}

// Unify the previous with the following - parameterize the inter-string separator...

inline void add_to_ostream(std::ostringstream&) { }

template<typename T, typename... Args>
inline void add_to_ostream(std::ostringstream& a_stream, T&& a_value, Args&&... a_args)
{
    a_stream << std::forward<T>(a_value);
    add_to_ostream(a_stream, std::forward<Args>(a_args)...);
}

/**
 * Very often you want the convenience of streaming values into an ostream, but
 * you don't want to go to the trouble of defining a variable creating it for just a
 * few << operators. So, instead, you can do:
 *
 *   foo_taking_a_string(util::concat_string( "Hello ", can_be_streamed, ' ', "world));
 *
 * @note: There's some overhead here, so obviously this is not for anything
 * performance-critical.
 *
 * @returns an std::string() holding the properly-stringified values.
 */
template<typename... Args>
inline std::string concat_string(Args&&... a_args)
{
    std::ostringstream s;
    add_to_ostream(s, std::forward<Args>(a_args)...);
    return s.str();
}

namespace detail {

inline std::istringstream& get_istringstream(){
	static thread_local std::istringstream stream;
	stream.str("");
	stream.clear();
	return stream;
}

inline std::istringstream& get_istringstream_for(const std::string& s){
	auto& iss = get_istringstream();
	iss.str(s);
	return iss;
}

inline std::ostringstream& get_ostringstream(){
	static thread_local std::ostringstream stream;
	stream.str("");
	stream.clear();
	return stream;
}

} // namespace detail


// returns true if $needle is a substring of $haystack
inline bool contains(const std::string& haystack, const std::string& needle) {
	return haystack.find(needle) != std::string::npos;
}
inline bool contains(const std::string& haystack, const char* needle) {
	return haystack.find(needle) != std::string::npos;
}


std::string sanitize(char c, unsigned int field_width = 0, bool enclose_in_single_quotes = false);

/**
 * Trim the leading spaces off a string, in-place
 * @param s a string with potential leading spaces
 * @return the input string
 */
static inline std::string ltrim(const std::string &s) {
	auto wsfront = std::find_if_not(s.begin(),s.end(),[](int c){return std::isspace(c);});
	return std::string(wsfront, s.end());
}

/**
 * Trim the trailing spaces off a string, in-place
 * @param s a string with potential trailing spaces
 * @return the input string
 */
static inline std::string rtrim(const std::string &s) {
	auto wsback = std::find_if_not(s.rbegin(),s.rend(),[](int c){return std::isspace(c);}).base();
	return (std::string(s.begin(),wsback));
}

/**
 * Trim the leading and traling spaces off a string, in-place
 * @param s a string with potential leading or traling spaces
 * @return the input string
 */
static inline std::string trim(const std::string &s) {
	auto wsfront = std::find_if_not(s.begin(),s.end(),[](int c){return std::isspace(c);});
	auto wsback = std::find_if_not(s.rbegin(),s.rend(),[](int c){return std::isspace(c);}).base();
	return (wsback<=wsfront ? std::string() : std::string(wsfront,wsback));
}

/**
 * Remove the prefix of a string up to and including the first occurence of a delimiter. The
 * search is carried out from the beginning.
 *
 * @param s a string from which to remove the prefix
 * @param delimiter the delimiter between the prefix and the rest of the string
 * @return The input string without the prefix and without the (first occurrence of)
 * the delimited.
 */
std::string substr_after(const std::string& s, const std::string& delimiter);

/**
 * The standard library's to_string doesn't actually support string arguments,
 * i.e. it can't work idempotently. This fixes it... but you need to prepend use
 * with:
 *
 *   using std::to_string;
 *   using util::to_string;
 *
 * @param s the string to "convert" (i.e. not convert)
 * @return
 */
inline std::string const& to_string(std::string const& s) { return s; }

/**
 * Replace a single occurrence of a substring in another string, by
 * an alternative substring.
 *
 * @param str The string in which to look and perform the replacement
 * @param from The substring to look for
 * @param to The replacement for the {@ref from} substring
 * @return true if a replacement was made, false if {@ref from} was not
 * found within {@ref str}
 */
bool replace(std::string& str, const std::string& from, const std::string& to);

/**
 * Same as {@ref std::replace}, but replaces all occurrences of from in str, and
 * returns the number of replacements made.
 */
size_t replace_all(std::string& str, const std::string& from, const std::string& to);

/**
 * Same as {@ref std::erase}, but erases all occurrences of from in str, and
 * returns the number of occurrences encountered.
 */
size_t erase_all(std::string& str, const std::string& to_erase);

/**
 * Return a copy of a string without any space, tab and newline characters at its end.
 * @param str The original string
 * @return The string without the trailing white space
 */
std::string chomp(const std::string& str);

/**
 * Return a copy of a string without any space, tab and newline characters at its beginning
 * @param str The original string
 * @return The string without the heading white space
 */
std::string lchomp(const std::string& str);

/**
 * Return a copy of a string without any space, tab and newline characters at both its ends
 * @param str The original string
 * @return The string without any heading or trailing white space
 */
std::string lrchomp(const std::string& str);

template <typename N>
constexpr char highest_metric_prefix_for(N x);

template <typename N>
std::string rough_form_decimal(N x, const std::string& suffix, unsigned char precision = 2);

template <typename N>
std::string shorten_with_highest_prefix(N x, unsigned char precision = 2);

template <typename N>
std::string value_and_shortened_form(N x, unsigned char precision = 2);

const char* ordinal_suffix(int n);
template <typename N = int>
inline std::string xth(N n) { return std::to_string(n) + ordinal_suffix(n); }

std::vector<std::string> explode(const std::string &s, char delimiter);

template <typename RandomAccessIterator>
inline std::string implode(RandomAccessIterator first, RandomAccessIterator last, char delimiter)
{
	if (first == last)     { return {}; }
    if (first + 1 == last) { return *first; }
	std::ostringstream oss;
	char delimiter_c_string[] = { delimiter, '\0' };
	std::copy(first, last-1, std::ostream_iterator<std::string>(oss, delimiter_c_string));
	oss << *(last-1);
	return oss.str();
}

template <typename Container>
inline std::string implode(const Container& c, char delimiter)
{
	return implode(c.begin(), c.end(), delimiter);
}

inline std::pair<std::string,std::string> split(const std::string &s, char delimiter)
{
	auto pos = s.find(delimiter);
	if (pos == std::string::npos) { return { s, "" }; }
	return { s.substr(0, pos), s.substr(pos + 1) };
}

template< typename T >
struct istream_traits
{
	inline static T read(std::istream& is)
	{
		T x;
		// assuming the stream is in good state
		is >> x;
		if (!is) { throw std::runtime_error("failed reading from a stream"); }
		return x;
	}
};

template<> struct istream_traits<bool>
{
	inline static bool read(std::istream& is)
	{
		is >> std::boolalpha;
		bool x;
		// assuming the stream is in good state
		is >> x;
		if (!is) { throw std::runtime_error("failed reading from a stream"); }
		return x;
	}
};

template<typename T>
inline T read(std::istream& is)
{
	T x = istream_traits<T>::read(is);
	return x;
}

// TODO: What about const char* 's ?
template <typename T>
T from_string(const std::string& s){
   auto& iss = ::util::detail::get_istringstream();
   iss.str(s);
   return util::read<T>(iss);
}

template <typename T>
void get_from_string(T& result, const std::string& s){
   auto& iss = ::util::detail::get_istringstream();
   iss.str(s);
   result = util::read<T>(iss);
}


inline const std::string singular_or_plural(size_t num_things, const char* singular, const char* plural)
{
	return num_things == 1 ? singular : plural;
}

inline const std::string is_or_are(size_t num_things) { return singular_or_plural(num_things, "is", "are"); }
inline const std::string maybe_plural_s(size_t num_things) { return singular_or_plural(num_things, "", "s"); }


namespace detail {
template <typename ToBeStreamed>
struct promoted_for_streaming { using type = ToBeStreamed; };
template<> struct promoted_for_streaming<char>{ using type = short; };
template<> struct promoted_for_streaming<signed char>{ using type = signed short; };
template<> struct promoted_for_streaming<unsigned char> { using type = unsigned short; };

} // namespace detail
/*
 * The following structs are used for streaming data to iostreams streams.
 * They have a tendancy to try to outsmart you, e.g. w.r.t. char or unsigned
 *  char data - they assume you're really passing ISO-8859-1 code points
 *  rather than integral values, and will print accordingly. Using this
 *  generic promoter, you van avoid that.
 */
template <typename ToBeStreamed>
typename detail::promoted_for_streaming<ToBeStreamed>::type promote_for_streaming(const ToBeStreamed& tbs)
{
	return static_cast<typename detail::promoted_for_streaming<ToBeStreamed>::type>(tbs);
}

template <typename I, bool UpperCase = false>
std::string as_hex(
	typename std::enable_if<std::is_unsigned<I>::value, I>::type x,
	unsigned hex_string_length = (2*sizeof(I)) << 1 )
{
	enum { bits_per_hex_digit = 4 }; // = log_2 of 16
	static const char* digit_characters =
		UpperCase ? "0123456789ABCDEF" : "0123456789abcdef" ;
	// maybe use an std::array instead? it would be initialized to (for uppercase):
	// { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

	std::string result(hex_string_length,'0');
	for (auto digit_index = 0; digit_index < hex_string_length ; digit_index++)
	{
		size_t bit_offset = (hex_string_length - 1 - digit_index) * bits_per_hex_digit;
		auto hexadecimal_digit = (x >> bit_offset) & 0xF;
		result[digit_index] = digit_characters[hexadecimal_digit];
	}
    return result;
}

template <typename T>
inline unsigned ilog2(typename std::enable_if<std::is_unsigned<T>::value, T>::type x);

template <typename I, bool UpperCase = false>
std::string as_hex(
	typename std::enable_if<std::is_unsigned<I>::value, I>::type x)
{
	constexpr const unsigned bits_per_hex_digit = 4; // = log_2 of 16
	unsigned necessary_digits = 1 + util::ilog2<I>(x) / bits_per_hex_digit;
	return as_hex<I, UpperCase>(x, necessary_digits);
}

// to_string extensions with fallback...

template <class T>
inline std::string to_string(const T& val)
{
    return std::to_string(val);
}

template <class T>
std::string to_string(T* const& val, ...)
{
    auto& oss(detail::get_ostringstream());
    oss << val;
    return oss.str();
}

inline void remove_spaces(std::string& s) {
	s.erase(std::remove_if(s.begin(), s.end(), isspace), s.end());
}

/**
 * Split the input string into two substrings, dropping the delimiter
 * character between them - which is the first occurrence of a delimiter
 * (out of the specified set) in the string
 *
 * @param s string to split
 * @param delimiters individual characters, in sequence (null-terminated)
 * which can serve as delimiters
 * @return the two substrings - upto but not including the first delimiter,
 * and after (and not including) the first delimiter
 * @throws std::invalid_argument if the string contains no delimiting characters
 */
inline std::pair<std::string, std::string> split_by_first(const std::string& s, const char* delimiters)
{
	auto pos = s.find_first_of(delimiters);
	if (pos == std::string::npos) {
		throw std::invalid_argument("No delimiters were found when trying to split a string");
	}
	return std::pair<std::string, std::string>(s.substr(0, pos), s.substr(pos+1, s.length() - pos - 1));
}

inline std::pair<std::string, std::string> split_by_first(const std::string& s, char delimiter)
{
	auto pos = s.find(delimiter);
	if (pos == std::string::npos) {
		throw std::invalid_argument("No delimiters were found when trying to split a string");
	}
	return std::pair<std::string, std::string>(s.substr(0, pos), s.substr(pos+1, s.length() - pos - 1));
}


/**
 * A (relatively) efficient implementation of a string tokenizer,
 * with multiple possible delimiters.
 *
 * @note code originally from @link http://stackoverflow.com/a/1493195/1593077
 */
template <class Container>
void tokenize(const std::string& str, Container& tokens, const std::string& delimiters = " ", bool trim_empty = false)
{
	// delimiter positions
	std::string::size_type pos, prev_pos = 0;

	using value_type = typename Container::value_type;
	using size_type  = typename Container::size_type;

	while (true) {
		pos = str.find_first_of(delimiters, prev_pos);
		if (pos == std::string::npos) {
			pos = str.length();
			if (pos != prev_pos || !trim_empty) {
				tokens.push_back(value_type(str.data() + prev_pos, (size_type) pos - prev_pos));
			}
			break;
		} else {
			if (pos != prev_pos || !trim_empty) {
				tokens.push_back(value_type(str.data() + prev_pos, (size_type) pos - prev_pos));
			}
		}
		prev_pos = pos + 1; // move past delimiter
	}
}

/**
 * A (relatively) efficient implementation of a string tokenizer,
 * with a single possible delimiters.
 */
template <class Container>
void tokenize(const std::string& str, Container& tokens, char delimiter = ' ', bool trim_empty = false)
{
	std::string::size_type pos, previous_pos = 0;

	using value_type = typename Container::value_type;
	using size_type  = typename Container::size_type;

	while (true) {
		pos = str.find(delimiter, previous_pos);
		if (pos == std::string::npos) {
			pos = str.length();
			if (pos != previous_pos || !trim_empty)
				tokens.push_back(value_type(str.data() + previous_pos, (size_type) pos - previous_pos));
			break;
		} else {
			if (pos != previous_pos || !trim_empty)
				tokens.push_back(value_type(str.data() + previous_pos, (size_type) pos - previous_pos));
		}

		previous_pos = pos + 1;
	}
}

namespace detail {

class escaped_char {
	unsigned char  unescaped_;
	bool           within_double_quotes_;

public:
	friend std::ostream& operator<<(std::ostream& os, const util::detail::escaped_char& ec);
	escaped_char(unsigned char unescaped, bool within_double_quotes) :
		unescaped_(unescaped), within_double_quotes_(within_double_quotes) { }
};

class escaped_string {
	const std::string& unescaped_;
	bool               place_within_double_quotes_;

public:
	friend std::ostream& operator<<(std::ostream& os, const escaped_string& es);
	escaped_string(const std::string& unescaped, bool place_within_double_quotes) :
		unescaped_(unescaped), place_within_double_quotes_(place_within_double_quotes) { }
};

std::ostream& operator<<(std::ostream& os, const escaped_char& ec);
std::ostream& operator<<(std::ostream& os, const escaped_string& es);

} // namespace detail

inline detail::escaped_char escape_nonprinting(unsigned char c, bool within_double_quotes = false)
{
	return detail::escaped_char(c, within_double_quotes);
}
inline detail::escaped_string escape_nonprinting(const std::string& s, bool place_within_double_quotes = false)
{
	return detail::escaped_string(s, place_within_double_quotes);
}

inline bool begins_with(const std::string& s, const std::string& prefix)
{
	return s.substr(0, prefix.length()) == prefix;
}

inline bool ends_with(const std::string& s, const std::string& suffix)
{
	if (s.length() < suffix.length()) { return false; }
	return s.substr(s.length() - suffix.length(), suffix.length()) == suffix;
}

inline std::string make_spaces(std::string::size_type num_spaces) { return std::string(num_spaces, ' '); }

} // namespace util

#endif /* UTIL_STRING_HPP_ */
