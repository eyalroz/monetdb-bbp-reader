
#include "util/dump.h"
#include "util/files.h"
#include "util/math.hpp"
#include "util/poor_mans_reflection.h"
#include "util/string.hpp"
#include "util/type_name.hpp"

#include <sstream>
#include <climits>
#include <iomanip>
#include <iterator>
#include <type_traits>
#include <unordered_map>
#include <functional>

#include <cstdlib> // For determining output terminal width in columns
#include <cstddef>

#ifdef UTIL_EXCEPTION_H_
using util::logic_error;
using util::invalid_argument;
#else
using std::logic_error;
using std::invalid_argument;
#endif

namespace util {

enum { bits_per_byte = CHAR_BIT, bits_per_char = CHAR_BIT, log_bits_per_char = 3 };

/**
 * The number bits in the representation of a value of type T; should be
 * available elsewhere, but we want to limit header dependency
 */
template <typename T>
struct size_in_bits { enum : size_t { value = sizeof(T) << log_bits_per_char }; };



using std::setw;
using std::left;
using std::right;
using std::dec;
using std::hex;
using std::string;
using std::ostream;

void sanitize_to(std::ostream& os, char c, unsigned int field_width)
{
	ios_flags_saver flag_saver(os);
	if(std::isgraph(c) or c == ' ') {
		// Since it's not a vertical or horizontal tab, nor a carriage return, nor a line feed;
		// we're pretty safe space-wise printing it out
		os << std::right << std::setw(field_width) << c;
	}
	else {
		os << std::setw(field_width - 2)  << "\\x"
		   << std::hex << std::uppercase << (int) ((unsigned char) c / 0xF)
		   << std::hex << std::uppercase << (int) ((unsigned char) c % 0xF);
	}
}

void sanitize_to(std::ostream& os, unsigned char c, unsigned int field_width)
{
	sanitize_to(os, (char) c, field_width);
}

void sanitize_to(std::ostream& os, signed char c, unsigned int field_width)
{
	sanitize_to(os, (char) c, field_width);
}


// The following several functions are an awkward replacement of char's operator<<
// into an ostream with a specific function

// This is quite unportable, and not very C++-like. drop/rewrite it with vectors
//void sanitize_to(std::ostream& os, char c, unsigned int field_width = 0);

// TODO: perhaps drop this?
template <typename Container>
inline std::string sanitize(Container const& c, unsigned int field_width = 0) {
	auto& oss = detail::get_ostringstream();

	sanitize(oss, c.cbegin(), c.cend(), field_width);
	return oss.str();
}

// TODO: Use optional instead of 0 for unlimited width
template <typename Iterator>
inline void sanitize(std::ostream& os, Iterator start, Iterator end, unsigned int field_width = 0)
{
	std::for_each(start,end, [&os, field_width](typename std::add_const<decltype(*start)>::type & e) {
		sanitize_to(os, e, field_width);
	});
}

inline void sanitize_to(std::ostream& os, const char* cp, unsigned int field_width = 0)
{
	sanitize(os, cp, cp + std::strlen(cp), field_width);
}

template <typename C>
inline void sanitize_to(std::ostream& os, C const& c, unsigned int field_width = 0) {
	sanitize(os, c.begin(), c.end(), field_width);
}

/**
 * Returns the number of characters required to display a value of a specified type.
 * The default implementation assumes we're simply displaying it as an signed or unsigned number,
 *
 * TODO: Rewrite me!
 *
 *
 * @tparam T the type whose field width we need
 * @param dummy a reference passed merely for the type inference, not actually necessrey; perhaps
 * we can use decltype instead
 */
template <typename T, typename = std::enable_if<std::is_arithmetic<T>::value>>
inline unsigned rule_of_thumb_field_length() {
	enum : unsigned { digits_per_byte = 3 };
	return (unsigned) ((double) sizeof(T) * digits_per_byte)  + 1; // for a sign character
	// not adding more for floats or doubles, they have plenty already (I think)
}
template<> inline unsigned rule_of_thumb_field_length<char, std::true_type>()          { return 4; }
// template<> inline unsigned rule_of_thumb_field_length<unsigned char, std::true_type>() { return 3; }


template <typename T>
inline void print_single_element(ostream& stream, const T& element, unsigned int field_width) {
	stream << setw(field_width) << util::promote_for_streaming(element);

}


static void set_printing_base(
	std::ostream&   stream,
	dump_parameters_t::numeric_t::printing_base_t
	                base,
	bool            uppercase_alphanumerics = false)
{
	switch(base) {
	case dump_parameters_t::numeric_t::printing_base_t::bin :
	case dump_parameters_t::numeric_t::printing_base_t::dec :
		stream << std::dec; break;
	case dump_parameters_t::numeric_t::printing_base_t::oct :
		stream << std::oct; break;
	case dump_parameters_t::numeric_t::printing_base_t::hex :
		stream << std::hex; break;
	}
	if (uppercase_alphanumerics) { stream << std::uppercase; }
}


template <typename T>
dump_parameters_t::width_t length_when_printing(
	const T&                                             x,
	util::dump_parameters_t::numeric_t::printing_base_t  base,
	optional<util::dump_parameters_t::width_t>           precision = {})
{
	auto& ss = detail::get_ostringstream();
	if (std::is_floating_point<T>::value and has_value(precision)) {
		ss << std::fixed;
		ss << std::setprecision(precision.value());
	}
	// if (std::is_arithmeric<T>::value) { set_printing_base(ss, base); }
	set_printing_base(ss, base);
	ss << util::promote_for_streaming(x);
	return ss.str().length();
}

/**
 * This is how the dump output looks like (ignoring header lines for now),
 * within a terminal window:
 *
 *
 *            index            (within a
 *   row        to            column group,)      between            (unused  row
 *  margin    first             between           column              space) margin
 *  start     column            columns            groups                     end
 *   +==+-----+====+--------------+=+--------------+===+--------------+======+==+
 *   |  :     :    :              : :              :   :              :      :  |
 *   |  :     :    :              : :              :   :              :      :  |
 *   |  :     :    [  col header 0] [  col header 1]   [  col header 2]      :  |
 *   |  ----------------------------------------------------------------------  |
 *   |  [index]    [colgrp 0 col 1] [colgrp 0 col 1]   [colgrp 1 col 0]      :  |
 *   |  [index]    [colgrp 0 col 1] [colgrp 0 col 1]   [colgrp 1 col 0]      :  |
 *   |  [index]    [colgrp 0 col 1] [colgrp 0 col 1]   [colgrp 1 col 0]      :  |
 *   |  [index]    [colgrp 0 col 1] [colgrp 0 col 1]   [colgrp 1 col 0]      :  |
 *   |  [index]    [colgrp 0 col 1] [colgrp 0 col 1]   [colgrp 1 col 0]      :  |
 *   |  :     :    :              : :              :   :              :      :  |
 *   +--------------------------------------------------------------------------+
 */

struct range_t {
	size_t start, end; // the range is start upto end, not inclusive, i.e. end is not in the range
	size_t length() const { return end - start; }
};

template <typename Datum>
struct augmented_params_t : dump_parameters_t {

	using parent = dump_parameters_t;
	using width_t = dump_parameters_t::width_t;

	range_t subrange_to_print;
	optional<range_t> bit_container_element_access_range;
	size_t data_length; // in elements; and not to be confused with the length of the printed subrange!
	width_t num_elements_per_line;
	struct extrema_t {
		struct extremum_t {
			const Datum* value; // Would have been better if I could have a reference instead
			size_t pos;
		} min, max;
	};
	optional<extrema_t> extrema;
	char separator_line_char = '-';
	bool print_title_separator = true;
	bool print_footer_separator = false;
	width_t floating_point_precision;
	bool right_align_column_headings { true }; // TODO: Support doing something to change that...
	struct {
        struct {
			width_t start { 0 };
			width_t end   { 1 };
		} row_margins; // within the terminal row
		width_t index                  { 20 };
		width_t index_to_first_column  {  3 }; // That's ":  "
		width_t column;
		width_t between_columns        {  2 };
		width_t between_column_groups  {  4 }; // replaces, rather than adds to, between_columns
		width_t full_data_row;
	} widths;


//	struct { size_t start, end; } actual_bit_container_element_access_range;

	optional<width_t> num_columns_per_group; // nullopt means no colgroups

protected:
	width_t compute_full_data_row_width() const
	{
		auto row_indices_width_contribution  =
			(extra_info.row_indices ? widths.index + widths.index_to_first_column : 0);
		auto data_columns_width = num_elements_per_line * widths.column;
		auto num_column_group_beginnings = has_value(num_columns_per_group) ?
			util::div_rounding_up(num_elements_per_line, num_columns_per_group.value()) : 0;
		auto num_inter_column_group_stretches =
			has_value(num_columns_per_group) ?
			num_column_group_beginnings - 1 : 0;
		auto num_intra_column_group_inter_column_stretches =
			num_elements_per_line - 1 - num_inter_column_group_stretches ;

		return
			row_indices_width_contribution +
			data_columns_width +
			num_inter_column_group_stretches * widths.between_column_groups +
			num_intra_column_group_inter_column_stretches * widths.between_columns;
				// and _not_ including the margins
	}

	width_t resolve_num_elements_per_line(std::ostream& stream) const
	{
		// TODO: Code for bitmaps - which may have different defaults and moduli

		struct {
			optional<width_t>  max;
			width_t            min;
		} bounds;
		bounds.min = parent::num_elements_per_line.min.value_or(1); // gotta print something...
		bounds.max = parent::num_elements_per_line.max;

		auto terminal_width = util::get_terminal_width(stream);
		if (not has_value(terminal_width)) {
			// fallback, which may very well fail as well...
			terminal_width = util::get_terminal_width();
		}

		if (terminal_width) {
			auto terminal_width_usable_for_columns =
				terminal_width.value() - widths.row_margins.start - widths.row_margins.end -
				widths.index - widths.index_to_first_column;
			width_t num_columns_which_fit_in_terminal;
			if (num_columns_per_group) {
				auto width_of_column_group =
					(widths.column + widths.between_columns) * num_columns_per_group.value() -
					widths.between_columns;
				auto num_column_groups_which_fit_in_terminal =
					(terminal_width_usable_for_columns + widths.between_column_groups) /
					(width_of_column_group + widths.between_column_groups);
				auto remaining_width_after_last_group = terminal_width.value() -
					num_column_groups_which_fit_in_terminal *
					(width_of_column_group + widths.between_column_groups);
				auto num_extra_columns =
					(remaining_width_after_last_group + widths.between_columns) /
					(widths.column + widths.between_columns);
				num_columns_which_fit_in_terminal = num_columns_per_group.value() *
					num_column_groups_which_fit_in_terminal +
					num_extra_columns;
			}
			else {
				num_columns_which_fit_in_terminal =
					(terminal_width_usable_for_columns + widths.between_columns) /
					(widths.column + widths.between_columns);
			}
			bounds.max = std::min(bounds.max, num_columns_which_fit_in_terminal);
		}

		if (has_value(parent::num_elements_per_line.preferred)) {
			auto preferred = parent::num_elements_per_line.preferred.value();
			if (preferred >= bounds.min and
			   (not has_value(bounds.max) or preferred <= bounds.max) )
			{
				return preferred;
			}
		}
		if (not has_value(parent::num_elements_per_line.modulus)) {
			return std::max(bounds.min, bounds.max);
		}

		if (has_value(bounds.max)) {
			width_t candidate = util::round_down(
				bounds.max.value(), parent::num_elements_per_line.modulus.value());
			return (candidate >= bounds.min ? candidate : bounds.max.value());
		}
		return util::round_up(bounds.min, parent::num_elements_per_line.modulus.value());
	}

	width_t width_of(const Datum& x) const
	{
		if (std::is_floating_point<Datum>::value) {
			auto width = length_when_printing(x, parent::numeric.printing_base, 0);
			if (std::round(x) == 0) { width += 1; }
			width += floating_point_precision > 0 ? floating_point_precision + 1 : 0;
			return width;
		}
		else return length_when_printing(x, parent::numeric.printing_base);
	}

	width_t resolve_column_width() const
	{
		switch(parent::column_width.setting_method) {
		case parent::column_width_t::setting_method_t::fixed:
			return parent::column_width.fixed_value.value();
		case parent::column_width_t::setting_method_t::by_extrema_in_data: {
			struct { parent::width_t min, max; } extremal_widths = {
				width_of(*(extrema.value().max.value)),
				width_of(*(extrema.value().max.value))
			};
			return std::max(1u, std::max(extremal_widths.min, extremal_widths.max));
		}
		case parent::column_width_t::setting_method_t::by_rule_of_thumb_for_type:
			return rule_of_thumb_field_length<Datum>();
		}
		throw logic_error("We shouldn't be able to get here.");
	}



public:
	template <typename ForwardIterator>
	augmented_params_t(
			std::ostream&             stream, // it's actually const
			const dump_parameters_t&  base_params,
			ForwardIterator           data_start,
			ForwardIterator           data_end,
			const string&             title)
	: dump_parameters_t(base_params)
	{
		static_assert(std::is_same<typename std::iterator_traits<ForwardIterator>::value_type, Datum>::value,
			"Invalid iterator type");
		// TODO: Check that base_params are valid for us!

		UNUSED(title);
		floating_point_precision = numeric.floating_point_precision.value_or(stream.precision());

		// The following code should probably have gone up into the dump_parameters_t
		// constructor, or been embedded into the structure of that class; but I'm
		// too busy/lazy to do that right now
		parent::num_elements_per_line.preferred = dump_bits ?
			(width_t) dump_parameters_t::num_elements_per_line_t::bit_dump_defaults::preferred :
			(width_t) dump_parameters_t::num_elements_per_line_t::defaults::preferred;
		parent::num_elements_per_line.modulus = dump_bits ?
			(width_t) dump_parameters_t::num_elements_per_line_t::bit_dump_defaults::modulus :
			(width_t) dump_parameters_t::num_elements_per_line_t::defaults::modulus;

		data_length = (data_end - data_start) * (dump_bits ? size_t{size_in_bits<Datum>::value} : size_t{1});
		subrange_to_print.start = util::clip<size_t>(
			parent::subrange_to_print.start.value_or(0), 0, parent::subrange_to_print.end.value_or(data_length));
		if (subrange_to_print.start == data_length) {
			throw logic_error("Invalid subrange of elements to print");
		}
		if (subrange_to_print.start == std::numeric_limits<size_t>::max()) {
			throw logic_error("Invalid subrange of elements to print");
		}
		subrange_to_print.end   = util::clip<size_t>(
			parent::subrange_to_print.end.value_or(data_length), subrange_to_print.start + 1, data_length);

		if (dump_bits) {
			if (subrange_to_print.start == subrange_to_print.end) {
				throw logic_error("Invalid subrange of elements to print");
			}
			bit_container_element_access_range = range_t {
				subrange_to_print.start / size_in_bits<Datum>::value,
				(subrange_to_print.end - 1) / size_in_bits<Datum>::value + 1
			};
		}


		widths.row_margins.start     = parent::widths.row_margins.start.value_or(widths.row_margins.start);
		widths.index                 = length_when_printing<size_t>(subrange_to_print.end,
		util::dump_parameters_t::numeric_t::printing_base_t::dec);
			// we could theoretically make this shorter (e.g. imagine printing elements 0...10 on a single line),
			// but maybe it's better this way
		widths.index_to_first_column = parent::widths.index_to_first_column.value_or(widths.index_to_first_column);
		widths.between_columns       = parent::widths.between_columns.value_or(widths.between_columns);
		widths.between_column_groups = parent::widths.between_column_groups.value_or(widths.between_column_groups);
		widths.row_margins.end       = parent::widths.row_margins.end.value_or(widths.row_margins.end);
		// the width of the index comes later...

		if (dump_bits) {
			widths.column = 1; // TODO: Support strings rather than chars for the bits
		}
		else if (extra_info.extrema or
		    column_width.setting_method == dump_parameters_t::column_width_t::setting_method_t::by_extrema_in_data)
		{
			// TODO: The code here is a bit ugly, e.g. due to the use of pointers,
			// and the fact that we'renost passing extrema to the resolve function
			// but rather the result of minmax_element.
			auto mme = std::minmax_element(data_start, data_end);
			extrema = extrema_t {
			 { &(*(mme.first)),  (size_t) (mme.first  - data_start) },
			 { &(*(mme.second)), (size_t) (mme.second - data_start) }
			};
			widths.column = resolve_column_width();
		}
		else {
			widths.column = resolve_column_width();
		}
		num_elements_per_line = std::min<width_t>(data_length, resolve_num_elements_per_line(stream));
		widths.full_data_row = compute_full_data_row_width();
	}
};


template<typename T>
inline void print_single_element_wrapper(
	ostream&                          stream,
	const T&                          element,
	util::dump_parameters_t::width_t  width,
	bool                              print_char_values_as_characters = false)
{
	if (std::is_same<T, char>::value) {
		if (print_char_values_as_characters) {
			sanitize_to(stream, reinterpret_cast<const char&>(element), std::max(width,3u));
			return;
		}
	}
	print_single_element(stream, element, width);
}

template<typename T>
inline void print_single_element_wrapper(
	ostream&                      stream,
	const T&                      element,
	const augmented_params_t<T>&  params)
{
	print_single_element_wrapper(
		stream, element, params.widths.column,
		params.print_char_values_as_characters);
}

static void initialize_stream(
	std::ostream&           stream,
	const dump_parameters_t&  params)
{
	const auto& precision = params.numeric.floating_point_precision;
	if (precision) { stream << std::setprecision(precision.value()); }
	stream.fill(' ');
}

template <typename E>
static void print_separator_line(
	std::ostream&                             stream,
	const augmented_params_t<E>&              params,
	optional<typename augmented_params_t<E>::width_t>  separator_line_width = {})
{
	set_if_disengaged(separator_line_width, params.widths.full_data_row);
	stream << make_spaces(params.widths.row_margins.start);
	stream
		<< std::string(separator_line_width.value(), params.separator_line_char)
		<< '\n';
}

std::ostream& operator<<(std::ostream& os, const range_t& range)
{
	return os << "[ " << range.start << ".." << range.end - 1 << " ]";
}

template<typename E>
static void print_header(
	std::ostream&                 stream,
	const string&                 title,
	const augmented_params_t<E>&  params)
{
	if (not params.need_header()) { return; }
	bool printed_anything = false;
	if (params.extra_info.title) {
		// TODO: What if the header is longer than the terminal length? Or our line width?
		stream << title << '\n';
		print_separator_line(stream, params, title.length());
	}
	if (params.extra_info.length_and_data_type) {
		printed_anything = true;
		stream << dec;
		stream << "Consists of " << params.data_length;
		if (params.dump_bits) { stream << " bits in container"; }
		stream << " elements of type \"" << type_name<E>()  << "\" (sized " << sizeof(E) <<  " bytes).";
	}
	if (params.extra_info.total_size_in_bytes) {
		if (printed_anything) { stream << ' '; }
		size_t total_size_in_bytes = params.dump_bits ?
			params.data_length / CHAR_BIT :  params.data_length * sizeof(E);
		stream << "Takes up " << total_size_in_bytes << " bytes altogether.";
	}
	if (printed_anything) { stream << "\n"; printed_anything = false; }
	if (params.extra_info.length_and_data_type and
	    params.subrange_to_print.length() != params.data_length)
	{
		stream << "Printing the " << params.subrange_to_print.length();
		if (params.dump_bits) {
			stream << " bits at positions " << params.subrange_to_print;
			if (params.bit_container_element_access_range.value().length() == 1) {
				stream << " (in a single " << size_in_bits<E>::value << "-bit container element)";
			}
			else {
				stream << " (in " << size_in_bits<E>::value << "-bit container elements "
				<< params.bit_container_element_access_range.value() << ")";
			}
		}
		else {
			stream << " elements at positions " << params.subrange_to_print;
		}
		stream << ".\n";
		printed_anything = true;
	}
	if (params.extra_info.extrema) {
		const auto& alignment_choice = params.right_align_column_headings ? std::right : std::left;
		// TODO: Avoid calling this twice; also, fail gracefully
		// for unordered-domain data
		auto digit_type_setter = (params.numeric.printing_base ==
			dump_parameters_t::numeric_t::printing_base_t::hex) ? std::hex : std::dec;
		stream.fill(' ');
		stream.precision(params.floating_point_precision); // redundant with the header
		const auto& min = *params.extrema.value().min.value;
		const auto& max = *params.extrema.value().min.value;
		auto extrema_width = std::max(
			length_when_printing<E>(min, params.numeric.printing_base, params.floating_point_precision),
			length_when_printing<E>(max, params.numeric.printing_base, params.floating_point_precision)
		);
		stream << alignment_choice  << digit_type_setter;  // What about a hex/dec setting here?
		stream << "Minimum element: ";
		print_single_element_wrapper<E>(stream, min, extrema_width, params.print_char_values_as_characters);
		stream << " (at index " << setw(params.widths.index) << (params.extrema.value().min.pos) << ")\n";
		stream	<< "Maximum element: ";
		print_single_element_wrapper<E>(stream, max, extrema_width, params.print_char_values_as_characters);
		stream << " (at index " << setw(params.widths.index) << (params.extrema.value().max.pos) << ")\n";
	}
	stream << '\n';
}

template<typename E>
static void print_column_headings(
	std::ostream&                 stream,
	const augmented_params_t<E>&  params)
{
	if (not params.extra_info.column_indices) { return; }

	const auto& alignment_choice = params.right_align_column_headings ? std::right : std::left;
	auto num_spaces_at_start_of_row =
		params.widths.row_margins.start +
		( params.extra_info.row_indices ?
		 ( params.widths.index + params.widths.index_to_first_column) : 0);
	// TODO: Need to make sure the column width is no shorter than the width of the column index,
	// i.e. if we have >= 10 columns their width can't be 1... or we go into the inter-column space
	auto column_index_width =
		std::max(params.widths.column, length_when_printing(params.num_elements_per_line,
			dump_parameters_t::numeric_t::printing_base_t::dec));
	struct { dump_parameters_t::width_t between_columns, between_column_groups; } adjusted =
		{ params.widths.between_columns, params.widths.between_column_groups };
	auto adjustment = column_index_width - params.widths.column;
	if (column_index_width > params.widths.column) {
		adjusted.between_columns -= adjustment;
		adjusted.between_column_groups -= adjustment;
		num_spaces_at_start_of_row -= adjustment;
	}
	stream.fill(' '); stream << std::dec; // TODO: Support hexadecimal indices
	stream << make_spaces(num_spaces_at_start_of_row);
	for(size_t column_index = 0; column_index < params.num_elements_per_line; column_index++)
	{
		stream << alignment_choice << setw(column_index_width) << column_index;
		bool at_end_of_line = (column_index == params.num_elements_per_line - 1);
		if (at_end_of_line) { break; }
		auto between_column_groups =
			has_value(params.num_columns_per_group) and
			util::divides(params.num_columns_per_group.value(), column_index + 1);
		dump_parameters_t::width_t post_column_space = between_column_groups ?
			adjusted.between_column_groups : adjusted.between_columns;
		stream << make_spaces(post_column_space);
	}
	stream << '\n';
	print_separator_line(stream, params);
}

template <typename E>
void print_footer(
	ostream&                      stream,
	const augmented_params_t<E>&  params)
{
	if (params.print_footer_separator) {
		print_separator_line(stream, params);
	}
}

template<typename ForwardIterator>
void print_data_rows(
	ostream&           stream,
	ForwardIterator    data,
	const augmented_params_t<typename std::iterator_traits<ForwardIterator>::value_type>&
	                   params)
{
	using element_type = typename std::iterator_traits<ForwardIterator>::value_type;

	// every row has: potential index info, some separation, and a number of elements
	// (which is the same on every line except the last one)

	stream.fill(' ');
	// The first line might have some blanks before the first element we print, since
	// the column index corresponds to the index in the printed array modulo the
	// number of columns
	struct {
		size_t start, end;
	} indexed_positions = {
		util::round_down(params.subrange_to_print.start, params.num_elements_per_line),
		util::round_up(params.subrange_to_print.end, params.num_elements_per_line)
	};
	data += params.dump_bits ?
		params.bit_container_element_access_range.value().start :
		params.subrange_to_print.start;
	stream << dec << right; // TODO: Drop this?
	stream.fill(' ');
	stream.precision(params.floating_point_precision); // redundant with the header
	uint64_t promoted_bit_container_element = *data;
	for(size_t pos = indexed_positions.start; pos < indexed_positions.end; pos++)
	{
		auto at_start_of_new_line = util::divides(params.num_elements_per_line, pos);
		if (at_start_of_new_line) {
			stream << make_spaces(params.widths.row_margins.start);
			if (std::is_arithmetic<element_type>::value and
			    params.numeric.fill_with_zeros) { stream.fill(' '); }
			stream.fill(' ');
			if (params.extra_info.row_indices) {
				// TODO: What about hexadecimal indices support?
				stream << std::dec << right;
				// Consider printing both the bit index and the container element index
				// when we're dumping bits
				stream << setw(params.widths.index) << pos << ':';
				stream << make_spaces(params.widths.index_to_first_column - 1);
				set_printing_base(
					stream,
					params.numeric.printing_base,
					params.numeric.uppercase_alphanumeric_digits);
			}
			if (std::is_arithmetic<element_type>::value and
			    params.numeric.fill_with_zeros) { stream.fill('0'); }
		}
		if (params.dump_bits) {
			auto bit_index_in_container = pos % (CHAR_BIT * sizeof(element_type));
			// TODO: Support other orders of bits in the container, e.g. bytes
			// in both little-endian and big-endian orer and bits-in-a-byte
			// proceeding either from lowest to highest or vice versa
			if (bit_index_in_container == 0) {
				promoted_bit_container_element = *(data++);
			}
		}
		bool in_printing_range =
			pos >= params.subrange_to_print.start and pos < params.subrange_to_print.end;
		if (in_printing_range) {
			if (params.dump_bits) {
				auto bit_index_in_container = pos % (CHAR_BIT * sizeof(element_type));
				unsigned bit = (
					promoted_bit_container_element &
						((uint64_t) 1 << bit_index_in_container)) == 0;
					// we could actually just use value(), since there's no chance
					// of the optional being disengaged, but GCC is complaining
				stream << setw(params.widths.column) << params.bit_glyphs[bit];
			} else {
				print_single_element_wrapper(stream, *(data++), params);
			}
		}
		else { stream << make_spaces(params.widths.column); }
		auto next_pos = pos + 1;
		auto at_end_of_line = (not params.print_data_on_single_line) and
			(util::divides(params.num_elements_per_line, next_pos) or
			 (next_pos == params.subrange_to_print.end));
		if (at_end_of_line) {
			stream << make_spaces(params.widths.row_margins.end) << '\n';
		}
		else {
			auto at_end_of_column_group = params.num_columns_per_group and
				util::divides(params.num_columns_per_group.value(), next_pos);
			stream << make_spaces(at_end_of_column_group ?
				params.widths.between_column_groups : params.widths.between_columns);
		}
	}
}

namespace detail {

template<typename ForwardIterator>
void dump_(
	ostream&                  stream,
	ForwardIterator           data_start,
	ForwardIterator           data_end,
	const string&             title,
	const dump_parameters_t&  params)
{
	using element_type = typename std::decay<decltype(*data_start)>::type;

	augmented_params_t<element_type> augmented_params(stream, params, data_start, data_end, title);

	util::ios_flags_saver flag_saver(stream);
	initialize_stream(stream, augmented_params);

	print_header<element_type>(stream, title, augmented_params);
	print_column_headings(stream,  augmented_params);
	print_data_rows(stream, data_start, augmented_params);
	print_footer(stream, augmented_params);
}

} // namespace detail

template<typename E>
void type_unerasure_dump(
	ostream&                stream,
	const void*             array,
	size_t                  length,
	const string&           title,
	const dump_parameters_t&  params)
{
	if (alignof(decltype(array)) < alignof(E)) {
		auto& ss = detail::get_ostringstream();
		ss	<< "Cannot dump the array named \"" << title << "\", at address " << array << " as an array of "
			<< util::type_name<E>() << "'s, as it's not aligned on a " << alignof(E) << "-byte boundary "
			<< "(its alignment is " << alignof(decltype(array)) << ").";
		throw logic_error(ss.str());
	}
	detail::dump_(stream, static_cast<const E*>(array), static_cast<const E*>(array) + length, title, params);
}

POOR_MANS_CONSTEXPR_REFLECTION_RESOLVER(get_type_unerasure_dump, type_unerasure_dump)

void dump(
	ostream&                   stream,
	const void*                data,
	const string&              data_type,
	size_t                     length, // in units of the data type!
	const string&              title,
	const dump_parameters_t&   params)
{
	const auto& type_unerasure_dump = get_type_unerasure_dump(data_type.c_str());
	type_unerasure_dump(stream, data, length, title, params);
}

namespace detail {

#define INSTANTIATE_DUMP_VAR(_type) \
template void dump_<_type *>(ostream&, _type *, _type *, const string&, const dump_parameters_t&)

#define INSTANTIATE_DUMP_CONST(_type) INSTANTIATE_DUMP_VAR(const _type)

#define INSTANTIATE_DUMP_BOTH(_type) \
INSTANTIATE_DUMP_CONST(_type); \
INSTANTIATE_DUMP_VAR(_type);

//MAP(INSTANTIATE_DUMP_BOTH, ALL_FUNDAMENTAL_NONVOID_TYPES_NO_DUPES)
MAP(INSTANTIATE_DUMP_BOTH, int, unsigned int, long, char, unsigned char)

}

} // namespace util
