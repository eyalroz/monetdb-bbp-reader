
#include "util/string.hpp"
#include "util/poor_mans_reflection.h"
#include "util/math.hpp"

#include <unordered_map>
#include <sstream>
#include <iomanip>
#include <utility>
#include <locale>

namespace monetdb {

namespace util {

const char* ordinal_suffix(int n)
{
	static const char suffixes [4][5] = {"th", "st", "nd", "rd"};
	auto ord = n % 100;
	if (ord / 10 == 1) { ord = 0; }
	ord = ord % 10;
	return suffixes[ord > 3 ? 0 : ord];
}

bool replace(std::string& str, const std::string& from, const std::string& to)
{
	size_t start_pos = str.find(from);
	if(start_pos == std::string::npos)
		return false;
	str.replace(start_pos, from.length(), to);
	return true;
}

size_t replace_all(std::string& str, const std::string& from, const std::string& to)
{
	size_t num_replacements = 0;
	size_t start_pos = str.find(from);
	while(start_pos != std::string::npos) {
		str.replace(start_pos, from.length(), to);
		num_replacements++;
		start_pos = str.find(from, start_pos + to.length());
	}
	return num_replacements;
}

size_t erase_all(std::string& str, const std::string& to_erase)
{
	size_t num_replacements = 0;
	size_t start_pos = str.find(to_erase);
	while(start_pos != std::string::npos) {
		str.erase(start_pos, to_erase.length());
		num_replacements++;
		start_pos = str.find(to_erase, start_pos);
	}
	return num_replacements;
}


std::string substr_after(const std::string& s, const std::string& delimiter)
{
	auto pos = s.find(delimiter);
	return (pos == std::string::npos) ? s : s.substr(pos + delimiter.length());
}


std::string chomp(const std::string& str)
{
	std::string::size_type pos = str.find_last_not_of("\n \t");
	return (pos != std::string::npos) ? str.substr(0, pos + 1) : str;
}

std::string lchomp(const std::string& str)
{
	std::string::size_type pos = str.find_first_not_of("\n \t");
	return (pos != std::string::npos) ? str.substr(pos) : str;
}

std::string lrchomp(const std::string& str)
{
	std::string::size_type rpos = str.find_last_not_of("\n \t");
	std::string::size_type lpos = str.find_first_not_of("\n \t");
	if (lpos ==  std::string::npos) lpos = 0;
	return (rpos ==  std::string::npos) ? str.substr(lpos) :  str.substr(lpos, rpos - lpos + 1);
}

template <typename N>
std::string rough_form_decimal(N x, const std::string& suffix, unsigned char precision)
{
	static const std::unordered_map<char, unsigned long long> powers_of_10 =
	{
		{ 'K', 1000 },
		{ 'M', 1000000 },
		{ 'G', 1000000000 },
		{ 'T', 1000000000000 },
		{ 'P', 1000000000000000 },
		{ 'E', 1000000000000000000 },
	};
	auto power_of_10 = powers_of_10.at(suffix[0]);
	if (precision == 0) { return std::to_string(x / power_of_10) + ' ' + suffix; }
	std::ostringstream ss;
	ss << std::fixed << std::setprecision(precision) << (double) x / power_of_10 << ' ' << suffix;
	return ss.str();
}

template <typename N>
constexpr char highest_metric_prefix_for(N x)
{
	constexpr char suffixes[] = {' ', 'K', 'M', 'G', 'T', 'P',  'E'};
		// for (nothing), Kilo, Mega, Giga, Tera, Peta, Eta

	auto num_trailing_zeros = util::log10_constexpr(x);
	if (num_trailing_zeros < 3) { return ' '; }
	return suffixes[std::min<unsigned>(sizeof(suffixes) - 1, num_trailing_zeros / 3 )];
}

template <typename N>
std::string shorten_with_highest_prefix(N x, unsigned char precision)
{
	auto suffix = highest_metric_prefix_for(x);
	return (suffix != ' ') ? rough_form_decimal(x, std::string(1, suffix), precision) :
		std::to_string(x);
}

template <typename N>
std::string value_and_shortened_form(N x, unsigned char precision)
{   
	return (x < 1000) ? 
		std::to_string(x) : std::to_string(x) + " ~= " + shorten_with_highest_prefix(x, precision);
} 

std::vector<std::string> explode(std::string const& s, char delimiter)
{
	std::vector<std::string> result;
	std::istringstream iss(s);

	for (std::string token; std::getline(iss, token, delimiter); )
	{
		result.push_back(std::move(token));
	}

	return result;
}

namespace detail {

std::ostream& operator<<(std::ostream& os, const escaped_char& ec)
{
	auto c = ec.unescaped_;
	// Cryptic, but makes sure the character is nice printable ASCII
	// (and not an escaping char)
	// TODO: Should we escape
	if (' ' <= c and c <= '~' and c != '\\' and (!ec.within_double_quotes_ or c != '"')) {
		os << c;
		return os;
	}
	os << '\\';
	switch(c) {
	case '"':  os << '"';  break;
	case '\\': os << '\\'; break;
	case '\t': os << 't';  break;
	case '\r': os << 'r';  break;
	case '\n': os << 'n';  break;
	default:
		char const* const hexdig = "0123456789ABCDEF";
		os << 'x';
		os << hexdig[c >> 4];
		os << hexdig[c & 0xF];
	}
	return os;
}

std::ostream& operator<<(std::ostream& os, const escaped_string& es)
{
	auto s = es.unescaped_;
	if (es.place_within_double_quotes_) { os << '"'; }
	for (std::string::const_iterator i = s.begin(), end = s.end(); i != end; ++i) {
		os << escape_nonprinting(*i, es.place_within_double_quotes_);
	}
	if (es.place_within_double_quotes_) { os << '"'; }
	return os;
}

} // namespace detail

std::string sanitize(char c, unsigned int field_width, bool enclose_in_single_quotes)
{
	std::ostringstream oss;
	if(std::isgraph(c) or c == ' ') {
		if (enclose_in_single_quotes) {
			if (field_width > 3) { oss << std::setw(field_width - 3) << ' '; }
			oss << '\'' << c << '\'';
		}
		else {
			oss << std::right << std::setw(field_width) << c;
		}
	}
	else {
		oss << std::setw(field_width - 2)  << "\\x"
			<< std::hex << std::uppercase << (int) ((unsigned char) c / 0xF)
			<< std::hex << std::uppercase << (int) ((unsigned char) c % 0xF);
	}
	return oss.str();
}


MAP_BINARY(INSTANTIATE_FREESTANDING_FUNCTION, rough_form_decimal, ALL_INTEGRAL_TYPES_NO_DUPES)
MAP_BINARY(INSTANTIATE_FREESTANDING_FUNCTION, shorten_with_highest_prefix, ALL_INTEGRAL_TYPES_NO_DUPES)
MAP_BINARY(INSTANTIATE_FREESTANDING_FUNCTION, value_and_shortened_form, ALL_INTEGRAL_TYPES_NO_DUPES)

} // namespace util

} // namespace monetdb



