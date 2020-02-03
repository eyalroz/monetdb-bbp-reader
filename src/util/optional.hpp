#pragma once
#ifndef SRC_UTIL_OPTIONAL_HPP_
#define SRC_UTIL_OPTIONAL_HPP_

#if __cplusplus >= 201701L
#include <optional>
namespace monetdb {
namespace util {
template <typename T>
using optional = std::optional<T>;
using nullopt = std::nullopt;
} // namespace util
#else
#if __cplusplus >= 201402L
#include <experimental/optional>
namespace monetdb {
namespace util {
template <typename T>
using optional = std::experimental::optional<T>;
using nullopt_t = std::experimental::nullopt_t;
constexpr auto nullopt = std::experimental::nullopt;
} // namespace util
} // namespace monetdb
#else /* pre-C++14 - must be C++11 or earlier */
#error "C++11 and earlier not supported - you'll need to get your own implementation of std::optional (e.g. Boost's or https://github.com/akrzemi1/Optional)"
#endif /* __cplusplus >= 201402L */
#endif /* __cplusplus >= 201701L */

namespace std {
// A bit of a lame hack here
template <typename T>
inline const T& min(const T& a, const monetdb::util::optional<T>& b) { return b ? min(a, *b) : a; }

template <typename T>
inline const T& min(const monetdb::util::optional<T>& a, const T& b) { return a ? min(*a, b) : b; }

template <typename T>
inline const T& min(const monetdb::util::optional<T>& a, const monetdb::util::optional<T>& b)
{
	if (!a && !b) {
		throw logic_error("Attempt to compare two optionals with no values");
	}
	return a ? min(*a, b) : min(a, *b);
}

template <typename T>
inline const T& max(const T& a, const monetdb::util::optional<T>& b) { return b ? max(a, *b) : a; }

template <typename T>
inline const T& max(const monetdb::util::optional<T>& a, const T& b) { return a ? max(*a, b) : b; }

template <typename T>
inline const T& max(const monetdb::util::optional<T>& a, const monetdb::util::optional<T>& b)
{
	if (!a && !b) {
		throw logic_error("Attempt to compare two optionals with no values");
	}
	return a ? max(*a, b) : max(a, *b);
}

template <typename T, typename U = T>
constexpr T& update_min(T& minimum_to_update, const monetdb::util::optional<U>& maybe_new_value)
{
	return (maybe_new_value and minimum_to_update > maybe_new_value.value()) ?
		minimum_to_update = maybe_new_value.value() : minimum_to_update;
}

template <typename T, typename U = T>
constexpr T& update_max(T& maximum_to_update, const monetdb::util::optional<U>& maybe_new_value)
{
	return (maybe_new_value and maximum_to_update < maybe_new_value.value()) ?
		maximum_to_update = maybe_new_value.value() : maximum_to_update;
}

template <typename T, const char* MissingValueIndicator>
inline string to_string(const monetdb::util::nullopt_t& n)
{
	return MissingValueIndicator;
}

template <typename T>
inline string to_string(const monetdb::util::nullopt_t& n)
{
	return to_string<T,"(unset)">(n);
}


template <typename T, const char* MissingValueIndicator>
inline string to_string(const monetdb::util::optional<T>& x)
{
	if (!x) { return MissingValueIndicator; }
	return std::to_string(x.value());
}

template <typename T>
inline string to_string(const monetdb::util::optional<T>& x)
{
	if (!x) { return "(unset)"; }
	return std::to_string(x.value());
}

template <typename T>
inline std::ostream& operator<<(std::ostream& os, const monetdb::util::optional<T>& x)
{
	if (!x) { return os << std::string("(unset)"); }
	return os << x.value();
}

template <typename T>
inline std::ostream& operator<<(std::ostream& os, const monetdb::util::nullopt_t& n)
{
	return os << std::string("(unset)");
}

template<class T, class ...Args>
T& emplace_or(monetdb::util::optional<T>& opt, Args&&...args) {
	if (!opt) { opt.emplace(forward<Args>(args)...); }
	return *opt;
}

} // namespace std

namespace monetdb {

template<class T>
inline util::optional<T> value_if(bool condition, T&& value) {
	if (!condition) return {};
	return value;
}

#if __cplusplus <= 201500L
/**
 * Checks whether an optional is engaged, i.e. whether it has a value or not
 *
 * @param o any optional
 * @return true if @p o has a value, false otherwise
 */
template <typename T>
constexpr inline bool has_value(const util::optional<T>& o)
{
	return o != util::nullopt;
}
#endif /* __cplusplus <= 201402L */

template <typename T>
bool is_disengaged(const util::optional<T>& o) { return not has_value(o); }

template <typename T>
bool is_engaged(const util::optional<T>& o) { return has_value(o); }

template <typename T>
util::optional<T>& set_if_disengaged(
	util::optional<T>&  o,
	const T&            value)
{
	if (is_disengaged(o)) { o = value; }
	return o;
}

template <typename T>
util::optional<T>& set_if_disengaged(
	util::optional<T>&        o,
	const util::optional<T>&  maybe_value)
{
	if (is_disengaged(o)) { o = maybe_value; }
	return o;
}

} // namespace monetdb

#endif /* SRC_UTIL_OPTIONAL_HPP_ */
