/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @t New Temporal Module
 * @a Peter Boncz, Martin van Dinther
 * @v 1.0
 *
 * Temporal Module
 * The goal of this module is to provide adequate functionality for
 * storing and manipulated time-related data. The minimum requirement
 * is that data can easily be imported from all common commercial
 * RDBMS products.
 *
 * This module supersedes the 'temporal' module that has a number of
 * conceptual problems and hard-to-solve bugs that stem from these
 * problems.
 *
 * The starting point of this module are SQL 92 and the ODBC
 * time-related data types.  Also, some functionalities have been
 * imported from the time classes of the Java standard library.
 *
 * This module introduces four basic types and operations on them:
 * @table @samp
 * @item date
 * a @samp{date} in the Gregorian calendar, e.g. 1999-JAN-31
 *
 * @item daytime
 * a time of day to the detail of milliseconds, e.g. 23:59:59:000
 *
 * @item timestamp
 * a combination of date and time, indicating an exact point in
 *
 * time (GMT). GMT is the time at the Greenwich meridian without a
 * daylight savings time (DST) regime. Absence of DST means that hours
 * are consecutive (no jumps) which makes it easy to perform time
 * difference calculations.
 *
 * @item timezone
 * the local time is often different from GMT (even at Greenwich in
 * summer, as the UK also has DST). Therefore, whenever a timestamp is
 * composed from a local daytime and date, a timezone should be
 * specified in order to translate the local daytime to GMT (and vice
 * versa if a timestamp is to be decomposed in a local date and
 * daytime).
 *
 * @item rule
 * There is an additional atom @samp{rule} that is used to define when
 * daylight savings time in a timezone starts and ends. We provide
 * predefined timezone objects for a number of timezones below (see
 * the init script of this module).  Also, there is one timezone
 * called the local @samp{timezone}, which can be set to one global
 * value in a running Monet server, that is used if the timezone
 * parameter is omitted from a command that needs it (if not set, the
 * default value of the local timezone is plain GMT).
 * @end table
 *
 * Limitations
 * The valid ranges of the various data types are as follows:
 *
 * @table @samp
 * @item min and max year
 * The maximum and minimum dates and timestamps that can be stored are
 * in the years 5,867,411 and -5,867,411, respectively. Interestingly,
 * the year 0 is not a valid year. The year before 1 is called -1.
 *
 * @item valid dates
 * Fall in a valid year, and have a month and day that is valid in
 * that year. The first day in the year is January 1, the last
 * December 31. Months with 31 days are January, March, May, July,
 * August, October, and December, while April, June, September and
 * November have 30 days. February has 28 days, expect in a leap year,
 * when it has 29. A leap year is a year that is an exact multiple of
 * 4. Years that are a multiple of 100 but not of 400 are an
 * exception; they are no leap years.
 *
 * @item valid daytime
 * The smallest daytime is 00:00:00:000 and the largest 23:59:59:999
 * (the hours in a daytime range between [0,23], minutes and seconds
 * between [0,59] and milliseconds between [0:999]).  Daytime
 * identifies a valid time-of-day, not an amount of time (for denoting
 * amounts of time, or time differences, we use here concepts like
 * "number of days" or "number of milliseconds" denoted by some value
 * of a standard integer type).
 *
 * @item valid timestamp
 * is formed by a combination of a valid date and valid daytime.
 * @item difference in days
 * For difference calculations between dates (in numbers of days) we
 * use signed integers (the @i{int} Monet type), hence the valid range
 * for difference calculations is between -2147483648 and 2147483647
 * days (which corresponds to roughly -5,867,411 and 5,867,411 years).
 * @item difference in msecs
 * For difference between timestamps (in numbers of milliseconds) we
 * use 64-bit longs (the @i{lng} Monet type).  These are large
 * integers of maximally 19 digits, which therefore impose a limit of
 * about 106,000,000,000 years on the maximum time difference used in
 * computations.
 * @end table
 *
 * There are also conceptual limitations that are inherent to the time
 * system itself:
 * @table @samp
 * @item Gregorian calendar
 * The basics of the Gregorian calendar stem from the time of Julius
 * Caesar, when the concept of a solar year as consisting of 365.25
 * days (365 days plus once in 4 years one extra day) was
 * introduced. However, this Julian Calendar, made a year 11 minutes
 * long, which subsequently accumulated over the ages, causing a shift
 * in seasons. In medieval times this was noticed, and in 1582 Pope
 * Gregory XIII issued a decree, skipped 11 days. This measure was not
 * adopted in the whole of Europe immediately, however.  For this
 * reason, there were many regions in Europe that upheld different
 * dates.
 *
 * It was only on @b{September 14, 1752} that some consensus was
 * reached and more countries joined the Gregorian Calendar, which
 * also was last modified at that time. The modifications were
 * twofold: first, 12 more days were skipped. Second, it was
 * determined that the year starts on January 1 (in England, for
 * instance, it had been starting on March 25).
 *
 * Other parts of the world have adopted the Gregorian Calendar even
 * later.
 *
 * This module implements the Gregorian Calendar in all its
 * regularity. This means that values before the year 1752 probably do
 * not correspond with the dates that people really used in times
 * before that (what they did use, however, was very vague anyway, as
 * explained above). In solar terms, however, this calendar is
 * reasonably accurate (see the "correction seconds" note below).
 *
 * @item timezones
 * The basic timezone regime was established on @b{November 1, 1884}
 * in the @emph{International Meridian Conference} held in Greenwich
 * (UK). Before that, a different time held in almost any city. The
 * conference established 24 different time zones defined by regular
 * longitude intervals that all differed by one hour. Not for long it
 * was that national and political interest started to erode this
 * nicely regular system.  Timezones now often follow country borders,
 * and some regions (like the Guinea areas in Latin America) have
 * times that differ with a 15 minute grain from GMT rather than an
 * hour or even half-an-hour grain.
 *
 * An extra complication became the introduction of daylight saving
 * time (DST), which causes a time jump in spring, when the clock is
 * skips one hour and in autumn, when the clock is set back one hour
 * (so in a one hour span, the same times occur twice).  The DST
 * regime is a purely political decision made on a country-by-country
 * basis. Countries in the same timezone can have different DST
 * regimes. Even worse, some countries have DST in some years, and not
 * in other years.
 *
 * To avoid confusion, this module stores absolute points of time in
 * GMT only (GMT does not have a DST regime). When storing local times
 * in the database, or retrieving local times from absolute
 * timestamps, a correct timezone object should be used for the
 * conversion.
 *
 * Applications that do not make correct use of timezones, will
 * produce irregular results on e.g. time difference calculations.
 *
 * @item correction seconds
 * Once every such hundred years, a correction second is added on new
 * year's night.  As I do not know the rule, and this rule would
 * seriously complicate this module (as then the duration of a day,
 * which is now the fixed number of 24*60*60*1000 milliseconds,
 * becomes parametrized by the date), it is not implemented. Hence
 * these seconds are lost, so time difference calculations in
 * milliseconds (rather than in days) have a small error if the time
 * difference spans many hundreds of years.
 * @end table
 *
 * TODO: we cannot handle well changes in the timezone rules (e.g.,
 * DST only exists since 40 years, and some countries make frequent
 * changes to the DST policy). To accommodate this we should make
 * timezone_local a function with a year parameter. The tool should
 * maintain and access the timezone database stored in two bats
 * [str,timezone],[str,year].  Lookup of the correct timezone would be
 * dynamic in this structure. The timezone_setlocal would just set the
 * string name of the timezone.
 *
 * Time/date comparison
 */

#include "monetdb_config.h"
#include "mtime.h"

#define get_rule(r)	((r).s.weekday | ((r).s.day<<6) | ((r).s.minutes<<10) | ((r).s.month<<21))
#define set_rule(r,i)							\
	do {										\
		(r).asint = int_nil;					\
		(r).s.weekday = (i)&15;					\
		(r).s.day = ((i)&(63<<6))>>6;			\
		(r).s.minutes = ((i)&(2047<<10))>>10;	\
		(r).s.month = ((i)&(15<<21))>>21;		\
	} while (0)

/* phony zero values, used to get negative numbers from unsigned
 * sub-integers in rule */
#define WEEKDAY_ZERO	8
#define DAY_ZERO	32
#define OFFSET_ZERO	4096

/* as the offset field got split in two, we need macros to get and set them */
#define get_offset(z)	(((int) (((z)->off1 << 7) + (z)->off2)) - OFFSET_ZERO)
#define set_offset(z,i)	do { (z)->off1 = (((i)+OFFSET_ZERO)&8064) >> 7; (z)->off2 = ((i)+OFFSET_ZERO)&127; } while (0)

tzone tzone_local;

static const char *MONTHS[13] = {
	NULL, "january", "february", "march", "april", "may", "june",
	"july", "august", "september", "october", "november", "december"
};

#if 0
static const char *DAYS[8] = {
	NULL, "monday", "tuesday", "wednesday", "thursday",
	"friday", "saturday", "sunday"
};
static const char *COUNT1[7] = {
	NULL, "first", "second", "third", "fourth", "fifth", "last"
};
static const char *COUNT2[7] = {
	NULL, "1st", "2nd", "3rd", "4th", "5th", "last"
};
#endif

static int LEAPDAYS[13] = {
	0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
};
static int CUMDAYS[13] = {
	0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365
};
static int CUMLEAPDAYS[13] = {
	0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366
};

static date DATE_MAX, DATE_MIN;		/* often used dates; computed once */

#define YEAR_MAX		5867411
#define YEAR_MIN		(-YEAR_MAX)
#define MONTHDAYS(m,y)	((m) != 2 ? LEAPDAYS[m] : leapyear(y) ? 29 : 28)
#define YEARDAYS(y)		(leapyear(y) ? 366 : 365)
#define DATE(d,m,y)		((m) > 0 && (m) <= 12 && (d) > 0 && (y) != 0 && (y) >= YEAR_MIN && (y) <= YEAR_MAX && (d) <= MONTHDAYS(m, y))
#define TIME(h,m,s,x)	((h) >= 0 && (h) < 24 && (m) >= 0 && (m) < 60 && (s) >= 0 && (s) < 60 && (x) >= 0 && (x) < 1000)
#define LOWER(c)		((c) >= 'A' && (c) <= 'Z' ? (c) + 'a' - 'A' : (c))

/*
 * auxiliary functions
 */

/* 
 * In MonetDB itself, the nil timestamp is set when the mtime module
 * is (dynamically) loaded; we can just set it here at compile time. 
 */
const timestamp_nilval_t ts_nil =  { GDK_lng_min };
static const timestamp * const timestamp_nil = &ts_nil.ts;

static int synonyms = TRUE;

#define leapyear(y)		((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0))

static int
leapyears(int year)
{
	/* count the 4-fold years that passed since jan-1-0 */
	int y4 = year / 4;

	/* count the 100-fold years */
	int y100 = year / 100;

	/* count the 400-fold years */
	int y400 = year / 400;

	return y4 + y400 - y100 + (year >= 0);	/* may be negative */
}

static date
todate(int day, int month, int year)
{
	date n = date_nil;

	if (DATE(day, month, year)) {
		if (year < 0)
			year++;				/* HACK: hide year 0 */
		n = (date) (day - 1);
		if (month > 2 && leapyear(year))
			n++;
		n += CUMDAYS[month - 1];
		/* current year does not count as leapyear */
		n += 365 * year + leapyears(year >= 0 ? year - 1 : year);
	}
	return n;
}

static void
fromdate(date n, int *d, int *m, int *y)
{
	int day, month, year;

	if (n == date_nil) {
		if (d)
			*d = int_nil;
		if (m)
			*m = int_nil;
		if (y)
			*y = int_nil;
		return;
	}
	year = n / 365;
	day = (n - year * 365) - leapyears(year >= 0 ? year - 1 : year);
	if (n < 0) {
		year--;
		while (day >= 0) {
			year++;
			day -= YEARDAYS(year);
		}
		day = YEARDAYS(year) + day;
	} else {
		while (day < 0) {
			year--;
			day += YEARDAYS(year);
		}
	}
	if (d == 0 && m == 0) {
		if (y)
			*y = (year <= 0) ? year - 1 : year;	/* HACK: hide year 0 */
		return;
	}

	day++;
	if (leapyear(year)) {
		for (month = day / 31 == 0 ? 1 : day / 31; month <= 12; month++)
			if (day > CUMLEAPDAYS[month - 1] && day <= CUMLEAPDAYS[month]) {
				if (m)
					*m = month;
				if (d == 0)
					return;
				break;
			}
		day -= CUMLEAPDAYS[month - 1];
	} else {
		for (month = day / 31 == 0 ? 1 : day / 31; month <= 12; month++)
			if (day > CUMDAYS[month - 1] && day <= CUMDAYS[month]) {
				if (m)
					*m = month;
				if (d == 0)
					return;
				break;
			}
		day -= CUMDAYS[month - 1];
	}
	if (d)
		*d = day;
	if (m)
		*m = month;
	if (y)
		*y = (year <= 0) ? year - 1 : year;	/* HACK: hide year 0 */
}

static daytime
totime(int hour, int min, int sec, int msec)
{
	if (TIME(hour, min, sec, msec)) {
		return (daytime) (((((hour * 60) + min) * 60) + sec) * 1000 + msec);
	}
	return daytime_nil;
}

static void
fromtime(daytime n, int *hour, int *min, int *sec, int *msec)
{
	int h, m, s, ms;

	if (n != daytime_nil) {
		h = n / 3600000;
		n -= h * 3600000;
		m = n / 60000;
		n -= m * 60000;
		s = n / 1000;
		n -= s * 1000;
		ms = n;
	} else {
		h = m = s = ms = int_nil;
	}
	if (hour)
		*hour = h;
	if (min)
		*min = m;
	if (sec)
		*sec = s;
	if (msec)
		*msec = ms;
}

/* matches regardless of case and extra spaces */
static int
fleximatch(const char *s, const char *pat, int min)
{
	int hit, spacy = 0;

	if (min == 0) {
		min = (int) strlen(pat);	/* default mininum required hits */
	}
	for (hit = 0; *pat; s++, hit++) {
		if (LOWER(*s) != *pat) {
			if (GDKisspace(*s) && spacy) {
				min++;
				continue;		/* extra spaces */
			}
			break;
		}
		spacy = GDKisspace(*pat);
		pat++;
	}
	return (hit >= min) ? hit : 0;
}

static int
parse_substr(int *ret, const char *s, int min, const char *list[], int size)
{
	int j = 0, i = 0;

	*ret = int_nil;
	while (++i <= size) {
		if ((j = fleximatch(s, list[i], min)) > 0) {
			*ret = i;
			break;
		}
	}
	return j;
}

/* Monday = 1, Sunday = 7 */
static int
date_dayofweek(date v)
{
	/* note, v can be negative, so v%7 is in the range -6...6
	 * v==0 is Saturday, so should result in return value 6 */
	return (v % 7 + 12) % 7 + 1;
}

#define SKIP_DAYS(d, w, i)						\
	do {										\
		d += i;									\
		w = (w + i) % 7;						\
		if (w <= 0)								\
			w += 7;								\
	} while (0)

static date
compute_rule(const rule *val, int y)
{
	int m = val->s.month, cnt = abs(val->s.day - DAY_ZERO);
	date d = todate(1, m, y);
	int dayofweek = date_dayofweek(d);
	int w = abs(val->s.weekday - WEEKDAY_ZERO);

	if (val->s.weekday == WEEKDAY_ZERO || w == WEEKDAY_ZERO) {
		/* cnt-th of month */
		d += cnt - 1;
	} else if (val->s.day > DAY_ZERO) {
		if (val->s.weekday < WEEKDAY_ZERO) {
			/* first weekday on or after cnt-th of month */
			SKIP_DAYS(d, dayofweek, cnt - 1);
			cnt = 1;
		}						/* ELSE cnt-th weekday of month */
		while (dayofweek != w || --cnt > 0) {
			if (++dayofweek == WEEKDAY_ZERO)
				dayofweek = 1;
			d++;
		}
	} else {
		if (val->s.weekday > WEEKDAY_ZERO) {
			/* cnt-last weekday from end of month */
			SKIP_DAYS(d, dayofweek, MONTHDAYS(m, y) - 1);
		} else {
			/* first weekday on or before cnt-th of month */
			SKIP_DAYS(d, dayofweek, cnt - 1);
			cnt = 1;
		}
		while (dayofweek != w || --cnt > 0) {
			if (--dayofweek == 0)
				dayofweek = 7;
			d--;
		}
	}
	return d;
}

#define BEFORE(d1, m1, d2, m2) ((d1) < (d2) || ((d1) == (d2) && (m1) <= (m2)))

static int
timestamp_inside(timestamp *ret, const timestamp *t, const tzone *z, lng offset)
{
	/* starts with GMT time t, and returns whether it is in the DST for z */
	lng add = (offset != (lng) 0) ? offset : (get_offset(z)) * (lng) 60000;
	int start_days, start_msecs, end_days, end_msecs, year;
	rule start, end;

	MTIMEtimestamp_add(ret, t, &add);

	if (ts_isnil(*ret) || z->dst == 0) {
		return 0;
	}
	set_rule(start, z->dst_start);
	set_rule(end, z->dst_end);

	start_msecs = start.s.minutes * 60000;
	end_msecs = end.s.minutes * 60000;

	fromdate(ret->days, NULL, NULL, &year);
	start_days = compute_rule(&start, year);
	end_days = compute_rule(&end, year);

	return BEFORE(start_days, start_msecs, end_days, end_msecs) ?
		(BEFORE(start_days, start_msecs, ret->days, ret->msecs) &&
		 BEFORE(ret->days, ret->msecs, end_days, end_msecs)) :
		(BEFORE(start_days, start_msecs, ret->days, ret->msecs) ||
		 BEFORE(ret->days, ret->msecs, end_days, end_msecs));
}

/*
 * ADT implementations
 */
int
date_fromstr(const char *buf, int *len, date **d)
{
	int day = 0, month = int_nil;
	int year = 0, yearneg = (buf[0] == '-'), yearlast = 0;
	int pos = 0, sep;

	if (*len < (int) sizeof(date)) {
		if (*d)
			GDKfree(*d);
		*d = (date *) GDKmalloc(*len = sizeof(date));
		if( *d == NULL)
			return 0;
	}
	**d = date_nil;
	if (yearneg == 0 && !GDKisdigit(buf[0])) {
		if (synonyms == 0)
			return 0;
		yearlast = 1;
		sep = ' ';
	} else {
		for (pos = yearneg; GDKisdigit(buf[pos]); pos++) {
			year = (buf[pos] - '0') + year * 10;
			if (year > YEAR_MAX)
				break;
		}
		sep = buf[pos++];
		if (synonyms == 0 && sep != '-') {
			return 0;
		}
		sep = LOWER(sep);
		if (sep >= 'a' && sep <= 'z') {
			sep = 0;
		} else if (sep == ' ') {
			while (buf[pos] == ' ')
				pos++;
		} else if (sep != '-' && sep != '/' && sep != '\\') {
			return 0;			/* syntax error */
		}
	}
	if (GDKisdigit(buf[pos])) {
		month = buf[pos++] - '0';
		if (GDKisdigit(buf[pos])) {
			month = (buf[pos++] - '0') + month * 10;
		}
	} else if (synonyms == 0) {
		return 0;
	} else {
		pos += parse_substr(&month, buf + pos, 3, MONTHS, 12);
	}
	if (month == int_nil || (sep && buf[pos++] != sep)) {
		return 0;				/* syntax error */
	}
	if (sep == ' ') {
		while (buf[pos] == ' ')
			pos++;
	}
	if (!GDKisdigit(buf[pos])) {
		return 0;				/* syntax error */
	}
	while (GDKisdigit(buf[pos])) {
		day = (buf[pos++] - '0') + day * 10;
		if (day > 31)
			break;
	}
	if (yearlast && buf[pos] == ',') {
		while (buf[++pos] == ' ')
			;
		if (buf[pos] == '-') {
			yearneg = 1;
			pos++;
		}
		while (GDKisdigit(buf[pos])) {
			year = (buf[pos++] - '0') + year * 10;
			if (year > YEAR_MAX)
				break;
		}
	}
	/* handle semantic error here (returns nil in that case) */
	**d = todate(day, month, yearneg ? -year : year);
	if (**d == date_nil)
		return 0;
	return pos;
}

int
date_tostr(str *buf, int *len, const date *val)
{
	int day, month, year;

	fromdate(*val, &day, &month, &year);
	/* longest possible string: "-5867411-01-01" i.e. 14 chars
	   without NUL (see definition of YEAR_MIN/YEAR_MAX above) */
	if (*len < 15) {
		if (*buf)
			GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 15);
		if( *buf == NULL)
			return 0;
	}
	if (*val == date_nil || !DATE(day, month, year)) {
		strcpy(*buf, "nil");
		return 3;
	}
	sprintf(*buf, "%d-%02d-%02d", year, month, day);
	return (int) strlen(*buf);
}

/*
 * @- daytime
 */
int
daytime_fromstr(const char *buf, int *len, daytime **ret)
{
	int hour, min, sec = 0, msec = 0, pos = 0;

	if (*len < (int) sizeof(daytime)) {
		if (*ret)
			GDKfree(*ret);
		*ret = (daytime *) GDKmalloc(*len = sizeof(daytime));
		if (*ret == NULL)
			return 0;
	}
	**ret = daytime_nil;
	if (!GDKisdigit(buf[pos])) {
		return 0;				/* syntax error */
	}
	for (hour = 0; GDKisdigit(buf[pos]); pos++) {
		if (hour <= 24)
			hour = (buf[pos] - '0') + hour * 10;
	}
	if ((buf[pos++] != ':') || !GDKisdigit(buf[pos])) {
		return 0;				/* syntax error */
	}
	for (min = 0; GDKisdigit(buf[pos]); pos++) {
		if (min <= 60)
			min = (buf[pos] - '0') + min * 10;
	}
	if ((buf[pos] == ':') && GDKisdigit(buf[pos + 1])) {
		for (pos++, sec = 0; GDKisdigit(buf[pos]); pos++) {
			if (sec <= 60)
				sec = (buf[pos] - '0') + sec * 10;
		}
		if ((buf[pos] == '.' || (synonyms && buf[pos] == ':')) &&
			GDKisdigit(buf[pos + 1])) {
			int fac = 100;

			for (pos++, msec = 0; GDKisdigit(buf[pos]); pos++) {
				msec += (buf[pos] - '0') * fac;
				fac /= 10;
			}
		}
	}
	/* handle semantic error here (returns nil in that case) */
	**ret = totime(hour, min, sec, msec);
	return pos;
}

int
daytime_tz_fromstr(const char *buf, int *len, daytime **ret)
{
	const char *s = buf;
	int pos = daytime_fromstr(s, len, ret);
	lng val, offset = 0;
	daytime mtime = 24 * 60 * 60 * 1000;

	if (!*ret || **ret == daytime_nil)
		return pos;

	s = buf + pos;
	pos = 0;
	while (GDKisspace(*s))
		s++;
	/* incase of gmt we need to add the time zone */
	if (fleximatch(s, "gmt", 0) == 3) {
		s += 3;
	}
	if ((s[0] == '-' || s[0] == '+') &&
		GDKisdigit(s[1]) && GDKisdigit(s[2]) && GDKisdigit(s[pos = 4]) &&
		((s[3] == ':' && GDKisdigit(s[5])) || GDKisdigit(s[pos = 3]))) {
		offset = (((s[1] - '0') * (lng) 10 + (s[2] - '0')) * (lng) 60 + (s[pos] - '0') * (lng) 10 + (s[pos + 1] - '0')) * (lng) 60000;
		pos += 2;
		if (s[0] != '-')
			offset = -offset;
		s += pos;
	} else {
		/* if no tzone is specified; work with the local */
		offset = get_offset(&tzone_local) * (lng) -60000;
	}
	val = **ret + offset;
	if (val < 0)
		val = mtime + val;
	if (val >= mtime)
		val = val - mtime;
	**ret = (daytime) val;
	return (int) (s - buf);
}

int
daytime_tostr(str *buf, int *len, const daytime *val)
{
	int hour, min, sec, msec;

	fromtime(*val, &hour, &min, &sec, &msec);
	if (*len < 12) {
		if (*buf)
			GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 13);
		if( *buf == NULL)
			return 0;
	}
	if (*val == daytime_nil || !TIME(hour, min, sec, msec)) {
		strcpy(*buf, "nil");
		return 3;
	}
	sprintf(*buf, "%02d:%02d:%02d.%03d", hour, min, sec, msec);
	return 12;
}

/*
 * @- timestamp
 */
int
timestamp_fromstr(const char *buf, int *len, timestamp **ret)
{
	const char *s = buf;
	int pos;
	date *d;
	daytime *t;

	if (*len < (int) sizeof(timestamp)) {
		if (*ret)
			GDKfree(*ret);
		*ret = (timestamp *) GDKmalloc(*len = sizeof(timestamp));
		if( *ret == NULL)
			return 0;
	}
	d = &(*ret)->days;
	t = &(*ret)->msecs;
	(*ret)->msecs = 0;
	s += date_fromstr(buf, len, &d);
	if (s > buf && (*(s) == '@' || *s == ' ' || *s == '-' || *s == 'T')) {
		while (*(++s) == ' ')
			;
		pos = daytime_fromstr(s, len, &t);
		s = pos ? s + pos : buf;
	} else if (!s || *s) {
		(*ret)->msecs = daytime_nil;
	}
	if (s <= buf || (*ret)->days == date_nil || (*ret)->msecs == daytime_nil) {
		**ret = *timestamp_nil;
	} else {
		lng offset = 0;

		while (GDKisspace(*s))
			s++;
		/* incase of gmt we need to add the time zone */
		if (fleximatch(s, "gmt", 0) == 3) {
			s += 3;
		}
		if ((s[0] == '-' || s[0] == '+') &&
			GDKisdigit(s[1]) && GDKisdigit(s[2]) && GDKisdigit(s[pos = 4]) &&
			((s[3] == ':' && GDKisdigit(s[5])) || GDKisdigit(s[pos = 3]))) {
			offset = (((s[1] - '0') * (lng) 10 + (s[2] - '0')) * (lng) 60 + (s[pos] - '0') * (lng) 10 + (s[pos + 1] - '0')) * (lng) 60000;
			pos += 2;
			if (s[0] != '-')
				offset = -offset;
			s += pos;
		} else {
			/* if no tzone is specified; work with the local */
			timestamp tmp = **ret;

			offset = get_offset(&tzone_local) * (lng) -60000;
			if (timestamp_inside(&tmp, &tmp, &tzone_local, (lng) -3600000)) {
				**ret = tmp;
			}
		}
		MTIMEtimestamp_add(*ret, *ret, &offset);
	}
	return (int) (s - buf);
}

int
timestamp_tz_fromstr(const char *buf, int *len, timestamp **ret)
{
	const char *s = buf;
	int pos = timestamp_fromstr(s, len, ret);
	lng offset = 0;

	if (!*ret || *ret == timestamp_nil)
		return pos;

	s = buf + pos;
	pos = 0;
	while (GDKisspace(*s))
		s++;
	/* incase of gmt we need to add the time zone */
	if (fleximatch(s, "gmt", 0) == 3) {
		s += 3;
	}
	if ((s[0] == '-' || s[0] == '+') &&
		GDKisdigit(s[1]) && GDKisdigit(s[2]) && GDKisdigit(s[pos = 4]) &&
		((s[3] == ':' && GDKisdigit(s[5])) || GDKisdigit(s[pos = 3]))) {
		offset = (((s[1] - '0') * (lng) 10 + (s[2] - '0')) * (lng) 60 + (s[pos] - '0') * (lng) 10 + (s[pos + 1] - '0')) * (lng) 60000;
		pos += 2;
		if (s[0] != '-')
			offset = -offset;
		s += pos;
	} else {
		/* if no tzone is specified; work with the local */
		offset = get_offset(&tzone_local) * (lng) -60000;
	}
	MTIMEtimestamp_add(*ret, *ret, &offset);
	return (int) (s - buf);
}


int
timestamp_tz_tostr(str *buf, int *len, const timestamp *val, const tzone *timezone)
{
	int len1, len2, big = 128;
	char buf1[128], buf2[128], *s = *buf, *s1 = buf1, *s2 = buf2;
	if (timezone != NULL) {
		/* int off = get_offset(timezone); */
		timestamp tmp = *val;

		if (!ts_isnil(tmp) && timestamp_inside(&tmp, val, timezone, (lng) 0)) {
			lng add = (lng) 3600000;

			MTIMEtimestamp_add(&tmp, &tmp, &add);
			/* off += 60; */
		}
		len1 = date_tostr(&s1, &big, &tmp.days);
		len2 = daytime_tostr(&s2, &big, &tmp.msecs);

		if (*len < 2 + len1 + len2) {
			if (*buf)
				GDKfree(*buf);
			*buf = (str) GDKmalloc(*len = len1 + len2 + 2);
			if( *buf == NULL)
				return 0;
		}
		s = *buf;
		if (ts_isnil(tmp)) {
			strcpy(s, "nil");
			return 3;
		}
		strcpy(s, buf1);
		s += len1;
		*s++ = ' ';
		strcpy(s, buf2);
		s += len2;
		/* omit GMT distance in order not to confuse the confused user
		   strcpy(s, "GMT"); s += 3;
		   if (off) {
		   *s++ = (off>=0)?'+':'-';
		   sprintf(s, "%02d%02d", abs(off)/60, abs(off)%60);
		   s += 4;
		   }
		 */
	}
	return (int) (s - *buf);
}

int
timestamp_tostr(str *buf, int *len, const timestamp *val)
{
	return timestamp_tz_tostr(buf, len, val, &tzone_local);
}

/* returns the timestamp that comes 'milliseconds' after 'value'. */
str
MTIMEtimestamp_add(timestamp *ret, const timestamp *v, const lng *msec)
{
	if (!ts_isnil(*v) && *msec != lng_nil) {
		int day = (int) (*msec / (24 * 60 * 60 * 1000));

		ret->msecs = (int) (v->msecs + (*msec - ((lng) day) * (24 * 60 * 60 * 1000)));
		ret->days = v->days;
		if (ret->msecs >= (24 * 60 * 60 * 1000)) {
			day++;
			ret->msecs -= (24 * 60 * 60 * 1000);
		} else if (ret->msecs < 0) {
			day--;
			ret->msecs += (24 * 60 * 60 * 1000);
		}
		if (day) {
			MTIMEdate_adddays(&ret->days, &ret->days, &day);
			if (ret->days == int_nil) {
				*ret = *timestamp_nil;
			}
		}
	} else {
		*ret = *timestamp_nil;
	}
	return MAL_SUCCEED;
}

/* returns the date that comes a number of day after 'v' (or before
 * iff *delta < 0). */
str
MTIMEdate_adddays(date *ret, const date *v, const int *delta)
{
	lng min = DATE_MIN, max = DATE_MAX;
	lng cur = (lng) *v, inc = *delta;

	if (cur == int_nil || inc == int_nil || (inc > 0 && (max - cur) < inc) || (inc < 0 && (min - cur) > inc)) {
		*ret = date_nil;
	} else {
		*ret = *v + *delta;
	}
	return MAL_SUCCEED;
}
