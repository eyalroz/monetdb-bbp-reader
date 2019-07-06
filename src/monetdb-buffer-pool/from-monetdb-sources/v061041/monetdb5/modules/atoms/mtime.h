/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * @* Implementation
 *
 * @+ Atoms
 *
 * @- date
 * Internally, we store date as the (possibly negative) number of days
 * since the start of the calendar. Oddly, since I (later) learned
 * that the year 0 did no exist, this defines the start of the
 * calendar to be Jan 1 of the year -1 (in other words, a -- positive
 * -- year component of a date is equal to the number of years that
 * have passed since the start of the calendar).
 */
#ifndef _MONETTIME_H_
#define _MONETTIME_H_

#include "gdk/gdk.h"

/* Instead of "mal/mal.h" - begin */

#define mal_export extern

/*
 * This file was originally intended to be a MAL module, hence it
 * has many functions returning str error messages, with the NULL string
 * indicating success
 */

#ifndef MAL_SUCCEED
#define MAL_SUCCEED ((str) 0) /* no error */
#endif

/* Instead of "mal/mal.h" - end */


#include <time.h>

#ifdef HAVE_FTIME
#include <sys/timeb.h>		/* ftime */
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>		/* gettimeofday */
#endif

#define YEAR_MAX		5867411
#define YEAR_MIN		(-YEAR_MAX)

typedef int date;
#define date_nil		((date) int_nil)
#define is_date_nil(X)	((X) == date_nil)
#define date_max		GDK_int_max /* used for overflow checks */

/*
 * @- daytime
 * Daytime values are also stored as the number of milliseconds that
 * passed since the start of the day (i.e. midnight).
 */
typedef int daytime;
#define daytime_nil ((daytime) int_nil)
#define is_daytime_nil(X) ((X) == daytime_nil)
/* it should never overflow */

/*
 * @- timestamp
 * Timestamp is implemented as a record that contains a date and a time (GMT).
 */
typedef union {
	lng alignment;
	struct {
#ifndef WORDS_BIGENDIAN
		daytime p_msecs;
		date p_days;
#else
		date p_days;
		daytime p_msecs;
#endif
	} payload;
} timestamp;
#define msecs payload.p_msecs
#define days payload.p_days

typedef union {
	lng nilval;
	timestamp ts;
} timestamp_nilval_t;

/*
 * In the MonetDB sources, the following is an opaque pointer, 
 * initialized elsewhere (at run time probably)
 */
mal_export const timestamp_nilval_t ts_nil;

/*
 * @- rule
 * rules are used to define the start and end of DST. It uses the 25
 * lower bits of an int.
 */
typedef union {
	struct {
		unsigned int month:4,	/* values: [1..12] */
		 minutes:11,			/* values: [0:1439] */
		 day:6,					/* values: [-31..-1,1..31] */
		 weekday:4,				/* values: [-7..-1,1..7] */
		 empty:7;				/* rule uses just 32-7=25 bits */
	} s;
	int asint;					/* the same, seen as single value */
} rule;

/*
 * @- tzone
 * A tzone consists of an offset and two DST rules, all crammed into one lng.
 */
typedef struct {
	/* we had this as bit fields in one unsigned long long, but native
	 * sun CC does not eat that.  */
	unsigned int dst:1, off1:6, dst_start:25;
	unsigned int off2:7, dst_end:25;
} tzone;

mal_export tzone tzone_local;

#define is_tzone_nil(z)   (get_offset(&(z)) == get_offset(tzone_nil))
#define is_timestamp_nil(t)   ((t).days == timestamp_nil->days && (t).msecs == timestamp_nil->msecs)

mal_export str MTIMEdate_adddays(date *ret, const date *v, const int *delta);
mal_export str MTIMEtimestamp_add(timestamp *ret, const timestamp *v, const lng *msec);
mal_export ssize_t date_fromstr(const char *buf, size_t *len, date **d, bool external);
mal_export ssize_t date_tostr(str *buf, size_t *len, const date *val, bool external);
mal_export ssize_t daytime_fromstr(const char *buf, size_t *len, daytime **ret, bool external);
mal_export ssize_t daytime_tostr(str *buf, size_t *len, const daytime *val, bool external);
mal_export ssize_t timestamp_fromstr(const char *buf, size_t *len, timestamp **ret, bool external);
mal_export ssize_t timestamp_tostr(str *buf, size_t *len, const timestamp *val, bool external);

#endif /* _MONETTIME_H_ */
