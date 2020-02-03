#pragma once
#ifndef UTIL_DUMP_H_
#define UTIL_DUMP_H_

#include "util/optional.hpp"
#include <ostream>
#include <functional>
#include <string>

namespace util {

struct dump_parameters_t {
	using this_t = struct dump_parameters_t;
	using width_t = unsigned;
	bool dump_bits { false };
		// when false, the iterated elements are printed;
		// when true, the elements are treated as bit containers,
		// and their bits are printed in some order as though tey
		// were the elements (with bit indices etc.)
	struct {
		optional<size_t> start, end;
		void set_length(size_t length)
		{
			if (start) {
				end = start.value() + length;
			}
			else {
				start = 0;
				end = length;
			}
		}
		optional<size_t> length() const
		{
			if (start and end) { return end.value() - start.value(); }
			return {};
		}
		bool is_full_range() const { return !start and !end; }
	} subrange_to_print;
	struct column_width_t {
		// this class should really be a variant...
		enum class setting_method_t {
			by_extrema_in_data,
			by_rule_of_thumb_for_type,
//			by_line_width_and_num_fields,
			fixed
		};
		setting_method_t   setting_method { setting_method_t::by_extrema_in_data };
		optional<width_t>  fixed_value;
	} column_width;
	struct num_elements_per_line_t {
		// Note: Yes, I know, I know, I should have probably had this class
		// be a variant of bit and non-bit structs with the defaults burned
		// in. But I'm too busy/lazy to do that right now.
		enum class defaults          : width_t { modulus = 5, preferred = 10 };
		enum class bit_dump_defaults : width_t { modulus = 4, preferred = 32 };
		optional<width_t> min, max;
		optional<width_t> preferred;
		optional<width_t> modulus;
		// This for the code passing a dump_parameters_t struct, not for the implementation
		void force_single_element() { min = max = preferred = modulus = 1; }
	} num_elements_per_line;
	bool print_data_on_single_line { false };
	bool print_char_values_as_characters { false };
	bool right_align_within_field { true }; // not yet actually supported
	struct {
		struct {
			optional<width_t> start, end;
		} row_margins; // within the terminal row
		optional<width_t> index; // including colon and the essential first space
		optional<width_t> index_to_first_column; // including colon and the essential first space
		optional<width_t> between_columns; // including the essential single-space
		optional<width_t> between_column_groups;
			// including the essential single-space;
			// and replaces the "between columns" spacing
	} widths;
	struct numeric_t {
		enum printing_base_t { bin, oct, dec, hex };
		bool   print_char_values_as_characters { false };
		bool   uppercase_alphanumeric_digits   { true  };
		bool   fill_with_zeros                 { false };
		optional<width_t> floating_point_precision;
		printing_base_t printing_base { dec };
		decltype(&std::hex) base_setter() const
		{
			switch(printing_base) {
			case dec: return std::dec; // sorry, can't do any better
			case oct: return std::oct; // sorry, can't do any better
			case hex: return std::hex; // sorry, can't do any better
			default: return std::dec; // sorry, can't do any betterP
			}
		}
	} numeric;

	struct bit_glyph_pair_t {
		const char glyphs[2];
		char zero() const { return glyphs[0]; }
		char one() const { return glyphs[1]; }
		char operator[](int i) const { return glyphs[i]; }
	};

    bit_glyph_pair_t bit_glyphs =  {
        { '-', '+' }
        // some other options:
        // { '0', '1' };
        // { ' ', '+' };
        // { ' ', '*' };
    };
   
//	struct {
//		optional<width_t> group_size; // extra spacing every this many columns
//		bool              apply;  // whether or not to space groups of columns
//	} column_grouping;
	struct {
		bool  column_indices         { true  };
		bool  row_indices            { true  };
		bool  length_and_data_type   { true  };
		bool  total_size_in_bytes    { true  };
		bool  subrange_to_print      { true  };
		bool  extrema                { false };
		bool  title                  { true  };
//		bool  address                { false }; // should only be used for POD types
	} extra_info;
	bool need_header() const {
		return
			extra_info.length_and_data_type or
			extra_info.total_size_in_bytes or
			extra_info.title or
			extra_info.extrema or
			(extra_info.subrange_to_print and subrange_to_print.is_full_range());
	}
};

#ifndef STREAMABLE_GADGET
#define STREAMABLE_GADGET

struct streamable_gadget_t {
	std::function<void(std::ostream&)> printer;
};

inline std::ostream& operator<<(std::ostream& os, const streamable_gadget_t& pg)
{
	pg.printer(os);
	return os;
}

#endif /* STREAMABLE_GADGET */

namespace detail {

// This extra redundancy will make it easier for us to instantiate -
// there will be no overload resolution
template<typename RandomAccessIterator>
void dump_(
	std::ostream&            stream,
	RandomAccessIterator     data_start,
	RandomAccessIterator     data_end,
	const std::string&       title,
	const dump_parameters_t&   params);

} // namespace detail

template<typename RandomAccessIterator>
inline streamable_gadget_t dump(
	RandomAccessIterator     data_start,
	RandomAccessIterator     data_end,
	const std::string&       title,
	const dump_parameters_t&   params = dump_parameters_t())
{
	auto f = [&](std::ostream& os) {
		detail::dump_(os, data_start, data_end, title, params);
	};
	return { f };
}

template<typename Container>
inline streamable_gadget_t dump(
	const Container&         container,
	const std::string&       title,
	const dump_parameters_t&   params = dump_parameters_t())
{
	auto f = [&](std::ostream& os) {
		detail::dump_(os, std::begin(container), std::end(container), title, params);
	};
	return { f };
}

void dump(
    std::ostream&            stream,
    const void*              data,
    const std::string&       data_type,
    size_t                   length, // in units of the data type!
    const std::string&       title,
    const dump_parameters_t&   params = dump_parameters_t());

inline streamable_gadget_t dump(
	const void*              data,
	const std::string&       data_type,
	size_t                   length, // in units of the data type!
	const std::string&       title,
	const dump_parameters_t&   params = dump_parameters_t())
{
	auto f = [&](std::ostream& os) {
		dump(os, data, data_type, length, title, params);
	};
	return { f };
}

// what about "promotion for printing"?

} // namespace util

#endif /* UTIL_DUMP_H_ */
