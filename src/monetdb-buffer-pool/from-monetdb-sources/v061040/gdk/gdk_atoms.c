/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * @a M. L. Kersten, P. Boncz
 * @* Atomic types
 * The Binary Association Table library assumes efficient
 * implementation of the atoms making up the binary association.  This
 * section describes the preliminaries for handling both built-in and
 * user-defined atomic types.
 * New types, such as point and polygons, can be readily added to this
 * collection.
 */
/*
 * @- inline comparison routines
 * Return 0 on l==r, < 0 iff l < r, >0 iff l > r
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

int
ATOMindex(const char *nme)
{
	int t, j = GDKatomcnt;

	for (t = 0; t < GDKatomcnt; t++) {
		if (!BATatoms[t].name[0]) {
			if (j == GDKatomcnt)
				j = t;
		} else if (strcmp(nme, BATatoms[t].name) == 0) {
			return t;
		}

	}
	if (strcmp(nme, "bat") == 0) {
		return TYPE_bat;
	}
	return -j;
}

char *
ATOMname(int t)
{
	return t >= 0 && t < GDKatomcnt && *BATatoms[t].name ? BATatoms[t].name : "null";
}

bool
ATOMisdescendant(int tpe, int parent)
{
	int cur = -1;

	while (cur != tpe) {
		cur = tpe;
		if (cur == parent)
			return true;
		tpe = ATOMstorage(tpe);
	}
	return false;
}


const bte bte_nil = GDK_bte_min-1;
const sht sht_nil = GDK_sht_min-1;
const int int_nil = GDK_int_min-1;
#ifdef NAN_CANNOT_BE_USED_AS_INITIALIZER
/* Definition of NAN is seriously broken on Intel compiler (at least
 * in some versions), so we work around it. */
const union _flt_nil_t _flt_nil_ = {
	.l = UINT32_C(0x7FC00000)
};
const union _dbl_nil_t _dbl_nil_ = {
	.l = UINT64_C(0x7FF8000000000000)
};
#else
const flt flt_nil = NAN;
const dbl dbl_nil = NAN;
#endif
const lng lng_nil = GDK_lng_min-1;
#ifdef HAVE_HGE
const hge hge_nil = GDK_hge_min-1;
#endif
const oid oid_nil = (oid) 1 << (sizeof(oid) * 8 - 1);
const char str_nil[2] = { '\200', 0 };
const ptr ptr_nil = NULL;


/*
 * @* Builtin Atomic Operator Implementations
 *
 * @+ Atom-from-String Conversions
 * These routines convert from string to atom. They are used during
 * conversion and BAT import. In order to avoid unnecessary
 * malloc()/free() sequences, the conversion functions have a meta
 * 'dst' pointer to a destination region, and an integer* 'len'
 * parameter, that denotes the length of that region (a char region
 * for ToStr functions, an atom region from FromStr conversions). Only
 * if necessary will the conversion routine do a GDKfree()/GDKmalloc()
 * sequence, and increment the 'len'.  Passing a pointer to a nil-ptr
 * as 'dst' and/or a *len==0 is valid; the conversion function will
 * then alloc some region for you.
 */
#define atommem(size)					\
	do {						\
		if (*dst == NULL || *len < (size)) {	\
			GDKfree(*dst);			\
			*len = (size);			\
			*dst = GDKmalloc(*len);		\
			if (*dst == NULL) {		\
				*len = 0;		\
				return -1;		\
			}				\
		}					\
	} while (0)

#define is_ptr_nil(val)		((val) == ptr_nil)

#define atomtostr(TYPE, FMT, FMTCAST)			\
ssize_t							\
TYPE##ToStr(char **dst, size_t *len, const TYPE *src)	\
{							\
	atommem(TYPE##Strlen);				\
	if (is_##TYPE##_nil(*src)) {			\
		return snprintf(*dst, *len, "nil");	\
	}						\
	return snprintf(*dst, *len, FMT, FMTCAST *src);	\
}

#define num08(x)	((x) >= '0' && (x) <= '7')
#define num10(x)	GDKisdigit(x)
#define num16(x)	(GDKisdigit(x) || ((x)  >= 'a' && (x)  <= 'f') || ((x)  >= 'A' && (x)  <= 'F'))
#define base10(x)	((x) - '0')
#define base08(x)	((x) - '0')
#define base16(x)	(((x) >= 'a' && (x) <= 'f') ? ((x) - 'a' + 10) : ((x) >= 'A' && (x) <= 'F') ? ((x) - 'A' + 10) : (x) - '0')
#define mult08(x)	((x) << 3)
#define mult16(x)	((x) << 4)

/*
 * numFromStr parses the head of the string for a number, accepting an
 * optional sign. The code has been prepared to continue parsing by
 * returning the number of characters read.  Both overflow and
 * incorrect syntax (not a number) result in the function returning 0
 * and setting the destination to nil.
 */
struct maxdiv {
	/* if we want to multiply a value with scale, the value must
	 * be no larger than maxval for there to not be overflow */
#ifdef HAVE_HGE
	hge scale, maxval;
#else
	lng scale, maxval;
#endif
};
static const struct maxdiv maxdiv[] = {
#ifdef HAVE_HGE
	/* maximum hge value: 170141183460469231731687303715884105727 (2**127-1)
	 * GCC doesn't currently support integer constants that don't
	 * fit in 8 bytes, so we split large values up*/
	{(hge) LL_CONSTANT(1), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000000000000000U)+ (hge) LL_CONSTANT(1687303715884105727)},
	{(hge) LL_CONSTANT(10), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000000000000000) + (hge) LL_CONSTANT(168730371588410572)},
	{(hge) LL_CONSTANT(100), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000000000000000) + (hge) LL_CONSTANT(16873037158841057)},
	{(hge) LL_CONSTANT(1000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000000000000) + (hge) LL_CONSTANT(1687303715884105)},
	{(hge) LL_CONSTANT(10000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000000000000) + (hge) LL_CONSTANT(168730371588410)},
	{(hge) LL_CONSTANT(100000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000000000000) + (hge) LL_CONSTANT(16873037158841)},
	{(hge) LL_CONSTANT(1000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000000000) + (hge) LL_CONSTANT(1687303715884)},
	{(hge) LL_CONSTANT(10000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000000000) + (hge) LL_CONSTANT(168730371588)},
	{(hge) LL_CONSTANT(100000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000000000) + (hge) LL_CONSTANT(16873037158)},
	{(hge) LL_CONSTANT(1000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000000) + (hge) LL_CONSTANT(1687303715)},
	{(hge) LL_CONSTANT(10000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000000) + (hge) LL_CONSTANT(168730371)},
	{(hge) LL_CONSTANT(100000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000000) + (hge) LL_CONSTANT(16873037)},
	{(hge) LL_CONSTANT(1000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000) + (hge) LL_CONSTANT(1687303)},
	{(hge) LL_CONSTANT(10000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000) + (hge) LL_CONSTANT(168730)},
	{(hge) LL_CONSTANT(100000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000) + (hge) LL_CONSTANT(16873)},
	{(hge) LL_CONSTANT(1000000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000) + (hge) LL_CONSTANT(1687)},
	{(hge) LL_CONSTANT(10000000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000) + (hge) LL_CONSTANT(168)},
	{(hge) LL_CONSTANT(100000000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100) + (hge) LL_CONSTANT(16)},
	{(hge) LL_CONSTANT(1000000000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10) + (hge) LL_CONSTANT(1)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1), (hge) LL_CONSTANT(17014118346046923173U)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10), (hge) LL_CONSTANT(1701411834604692317)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100), (hge) LL_CONSTANT(170141183460469231)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000), (hge) LL_CONSTANT(17014118346046923)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000), (hge) LL_CONSTANT(1701411834604692)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000), (hge) LL_CONSTANT(170141183460469)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000), (hge) LL_CONSTANT(17014118346046)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000), (hge) LL_CONSTANT(1701411834604)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000), (hge) LL_CONSTANT(170141183460)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000), (hge) LL_CONSTANT(17014118346)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000), (hge) LL_CONSTANT(1701411834)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000), (hge) LL_CONSTANT(170141183)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000), (hge) LL_CONSTANT(17014118)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000), (hge) LL_CONSTANT(1701411)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000), (hge) LL_CONSTANT(170141)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000), (hge) LL_CONSTANT(17014)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000), (hge) LL_CONSTANT(1701)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000000), (hge) LL_CONSTANT(170)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000000), (hge) LL_CONSTANT(17)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000000U),(hge) LL_CONSTANT(1)},
#else
	/* maximum lng value: 9223372036854775807 (2**63-1) */
	{LL_CONSTANT(1), LL_CONSTANT(9223372036854775807)},
	{LL_CONSTANT(10), LL_CONSTANT(922337203685477580)},
	{LL_CONSTANT(100), LL_CONSTANT(92233720368547758)},
	{LL_CONSTANT(1000), LL_CONSTANT(9223372036854775)},
	{LL_CONSTANT(10000), LL_CONSTANT(922337203685477)},
	{LL_CONSTANT(100000), LL_CONSTANT(92233720368547)},
	{LL_CONSTANT(1000000), LL_CONSTANT(9223372036854)},
	{LL_CONSTANT(10000000), LL_CONSTANT(922337203685)},
	{LL_CONSTANT(100000000), LL_CONSTANT(92233720368)},
	{LL_CONSTANT(1000000000), LL_CONSTANT(9223372036)},
	{LL_CONSTANT(10000000000), LL_CONSTANT(922337203)},
	{LL_CONSTANT(100000000000), LL_CONSTANT(92233720)},
	{LL_CONSTANT(1000000000000), LL_CONSTANT(9223372)},
	{LL_CONSTANT(10000000000000), LL_CONSTANT(922337)},
	{LL_CONSTANT(100000000000000), LL_CONSTANT(92233)},
	{LL_CONSTANT(1000000000000000), LL_CONSTANT(9223)},
	{LL_CONSTANT(10000000000000000), LL_CONSTANT(922)},
	{LL_CONSTANT(100000000000000000), LL_CONSTANT(92)},
	{LL_CONSTANT(1000000000000000000), LL_CONSTANT(9)},
#endif
};
static const int maxmod10 = 7;	/* (int) (maxdiv[0].maxval % 10) */

static ssize_t
numFromStr(const char *src, size_t *len, void **dst, int tp)
{
	const char *p = src;
	size_t sz = ATOMsize(tp);
#ifdef HAVE_HGE
	hge base = 0;
#else
	lng base = 0;
#endif
	int sign = 1;

	/* a valid number has the following syntax:
	 * [-+]?[0-9]+([eE][0-9]+)?(LL)? -- PCRE syntax, or in other words
	 * optional sign, one or more digits, optional exponent, optional LL
	 * the exponent has the following syntax:
	 * lower or upper case letter E, one or more digits
	 * embedded spaces are not allowed
	 * the optional LL at the end are only allowed for lng and hge
	 * values */
	atommem(sz);

	if (GDK_STRNIL(src)) {
		memcpy(*dst, ATOMnilptr(tp), sz);
		return 1;
	}

	while (GDKisspace(*p))
		p++;
	if (!num10(*p)) {
		switch (*p) {
		case 'n':
			memcpy(*dst, ATOMnilptr(tp), sz);
			if (p[1] == 'i' && p[2] == 'l') {
				p += 3;
				return (ssize_t) (p - src);
			}
			GDKerror("not a number");
			goto bailout;
		case '-':
			sign = -1;
			p++;
			break;
		case '+':
			p++;
			break;
		}
		if (!num10(*p)) {
			GDKerror("not a number");
			goto bailout;
		}
	}
	do {
		int dig = base10(*p);
		if (base > maxdiv[1].maxval ||
		    (base == maxdiv[1].maxval && dig > maxmod10)) {
			/* overflow */
			goto overflow;
		}
		base = 10 * base + dig;
		p++;
	} while (num10(*p));
	if ((*p == 'e' || *p == 'E') && num10(p[1])) {
		p++;
		if (base == 0) {
			/* if base is 0, any exponent will do, the
			 * result is still 0 */
			while (num10(*p))
				p++;
		} else {
			int exp = 0;
			do {
				/* this calculation cannot overflow */
				exp = exp * 10 + base10(*p);
				if (exp >= (int) (sizeof(maxdiv) / sizeof(maxdiv[0]))) {
					/* overflow */
					goto overflow;
				}
				p++;
			} while (num10(*p));
			if (base > maxdiv[exp].maxval) {
				/* overflow */
				goto overflow;
			}
			base *= maxdiv[exp].scale;
		}
	}
	base *= sign;
	switch (sz) {
	case 1: {
		bte **dstbte = (bte **) dst;
		if (base < GDK_bte_min || base > GDK_bte_max) {
			goto overflow;
		}
		**dstbte = (bte) base;
		break;
	}
	case 2: {
		sht **dstsht = (sht **) dst;
		if (base < GDK_sht_min || base > GDK_sht_max) {
			goto overflow;
		}
		**dstsht = (sht) base;
		break;
	}
	case 4: {
		int **dstint = (int **) dst;
		if (base < GDK_int_min || base > GDK_int_max) {
			goto overflow;
		}
		**dstint = (int) base;
		break;
	}
	case 8: {
		lng **dstlng = (lng **) dst;
#ifdef HAVE_HGE
		if (base < GDK_lng_min || base > GDK_lng_max) {
			goto overflow;
		}
#endif
		**dstlng = (lng) base;
		if (p[0] == 'L' && p[1] == 'L')
			p += 2;
		break;
	}
#ifdef HAVE_HGE
	case 16: {
		hge **dsthge = (hge **) dst;
		**dsthge = (hge) base;
		if (p[0] == 'L' && p[1] == 'L')
			p += 2;
		break;
	}
#endif
	}
	while (GDKisspace(*p))
		p++;
	return (ssize_t) (p - src);

  overflow:
	while (num10(*p))
		p++;
	GDKerror("overflow: \"%.*s\" does not fit in %s\n",
		 (int) (p - src), src, ATOMname(tp));
  bailout:
	memcpy(*dst, ATOMnilptr(tp), sz);
	return -1;
}

ssize_t
bteFromStr(const char *src, size_t *len, bte **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_bte);
}

ssize_t
shtFromStr(const char *src, size_t *len, sht **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_sht);
}

ssize_t
intFromStr(const char *src, size_t *len, int **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_int);
}

ssize_t
lngFromStr(const char *src, size_t *len, lng **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_lng);
}

#ifdef HAVE_HGE
ssize_t
hgeFromStr(const char *src, size_t *len, hge **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_hge);
}
#endif

atomtostr(bte, "%hhd", )

atomtostr(sht, "%hd", )

atomtostr(int, "%d", )

atomtostr(lng, LLFMT, )

#ifdef HAVE_HGE
#define HGE_LL018FMT "%018" PRId64
#define HGE_LL18DIGITS LL_CONSTANT(1000000000000000000)
#define HGE_ABS(a) (((a) < 0) ? -(a) : (a))
ssize_t
hgeToStr(char **dst, size_t *len, const hge *src)
{
	atommem(hgeStrlen);
	if (is_hge_nil(*src)) {
		strncpy(*dst, "nil", *len);
		return 3;
	}
	if ((hge) GDK_lng_min <= *src && *src <= (hge) GDK_lng_max) {
		lng s = (lng) *src;
		return lngToStr(dst, len, &s);
	} else {
		hge s = *src / HGE_LL18DIGITS;
		ssize_t llen = hgeToStr(dst, len, &s);
		if (llen < 0)
			return llen;
		snprintf(*dst + llen, *len - llen, HGE_LL018FMT,
			 (lng) HGE_ABS(*src % HGE_LL18DIGITS));
		return strlen(*dst);
	}
}
#endif

ssize_t
ptrFromStr(const char *src, size_t *len, ptr **dst)
{
	size_t base = 0;
	const char *p = src;

	atommem(sizeof(ptr));

	**dst = ptr_nil;
	if (GDK_STRNIL(src))
		return 1;

	while (GDKisspace(*p))
		p++;
	if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		p += 3;
	} else {
		if (p[0] == '0' && (p[1] == 'x' || p[1] == 'X')) {
			p += 2;
		}
		if (!num16(*p)) {
			GDKerror("not a number\n");
			return -1;
		}
		while (num16(*p)) {
			if (base >= ((size_t) 1 << (8 * sizeof(size_t) - 4))) {
				GDKerror("overflow\n");
				return -1;
			}
			base = mult16(base) + base16(*p);
			p++;
		}
		**dst = (ptr) base;
	}
	while (GDKisspace(*p))
		p++;
	return (ssize_t) (p - src);
}

atomtostr(ptr, "%p", )

ssize_t
dblFromStr(const char *src, size_t *len, dbl **dst)
{
	const char *p = src;
	ssize_t n = 0;
	double d;

	/* alloc memory */
	atommem(sizeof(dbl));

	if (GDK_STRNIL(src)) {
		**dst = dbl_nil;
		return 1;
	}

	while (GDKisspace(*p))
		p++;
	if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		**dst = dbl_nil;
		p += 3;
		n = (ssize_t) (p - src);
	} else {
		/* on overflow, strtod returns HUGE_VAL and sets
		 * errno to ERANGE; on underflow, it returns a value
		 * whose magnitude is no greater than the smallest
		 * normalized double, and may or may not set errno to
		 * ERANGE.  We accept underflow, but not overflow. */
		char *pe;
		errno = 0;
		d = strtod(p, &pe);
		if (p == pe)
			p = src; /* nothing converted */
		else
			p = pe;
		n = (ssize_t) (p - src);
		if (n == 0 || (errno == ERANGE && (d < -1 || d > 1))
		    || !isfinite(d) /* no NaN or Infinte */
		    ) {
			GDKerror("overflow or not a number\n");
			return -1;
		} else {
			while (src[n] && GDKisspace(src[n]))
				n++;
			**dst = (dbl) d;
		}
	}
	return n;
}

ssize_t
dblToStr(char **dst, size_t *len, const dbl *src)
{
	int i;

	atommem(dblStrlen);
	if (is_dbl_nil(*src)) {
		return snprintf(*dst, *len, "nil");
	}
	for (i = 4; i < 18; i++) {
		snprintf(*dst, *len, "%.*g", i, *src);
		if (strtod(*dst, NULL) == *src)
			break;
	}
	return (ssize_t) strlen(*dst);
}

ssize_t
fltFromStr(const char *src, size_t *len, flt **dst)
{
	const char *p = src;
	ssize_t n = 0;
	float f;

	/* alloc memory */
	atommem(sizeof(flt));

	if (GDK_STRNIL(src)) {
		**dst = flt_nil;
		return 1;
	}

	while (GDKisspace(*p))
		p++;
	if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		**dst = flt_nil;
		p += 3;
		n = (ssize_t) (p - src);
	} else {
		/* on overflow, strtof returns HUGE_VALF and sets
		 * errno to ERANGE; on underflow, it returns a value
		 * whose magnitude is no greater than the smallest
		 * normalized float, and may or may not set errno to
		 * ERANGE.  We accept underflow, but not overflow. */
		char *pe;
		errno = 0;
		f = strtof(p, &pe);
		if (p == pe)
			p = src; /* nothing converted */
		else
			p = pe;
		n = (ssize_t) (p - src);
		if (n == 0 || (errno == ERANGE && (f < -1 || f > 1))
		    || !isfinite(f) /* no NaN or infinite */) {
			GDKerror("overflow or not a number\n");
			return -1;
		} else {
			while (src[n] && GDKisspace(src[n]))
				n++;
			**dst = (flt) f;
		}
	}
	return n;
}

ssize_t
fltToStr(char **dst, size_t *len, const flt *src)
{
	int i;

	atommem(fltStrlen);
	if (is_flt_nil(*src)) {
		return snprintf(*dst, *len, "nil");
	}
	for (i = 4; i < 10; i++) {
		snprintf(*dst, *len, "%.*g", i, *src);
		if (strtof(*dst, NULL) == *src)
			break;
	}
	return (ssize_t) strlen(*dst);
}

/* String Atom Implementation
 *
 * Strings are stored in two parts.  The first part is the normal tail
 * heap which contains a list of offsets.  The second part is the
 * theap which contains the actual strings.  The offsets in the tail
 * heap (a.k.a. offset heap) point into the theap (a.k.a. string
 * heap).  Strings are NULL-terminated and are stored without any
 * escape sequec=nces.  Strings are encoded using the UTF-8 encoding
 * of Unicode.  This means that individual "characters" (really,
 * Unicode code points) can be between one and four bytes long.
 *
 * Because in many typical situations there are lots of duplicated
 * string values that are being stored in a table, but also in many
 * (other) typical situations there are very few duplicated string
 * values stored, a scheme has been introduced to cater to both
 * situations.
 *
 * When the string heap is "small" (defined as less than 64KiB), the
 * string heap is fully duplicate eliminated.  When the string heap
 * grows beyond this size, the heap is not kept free of duplicate
 * strings, but there is then a heuristic that tries to limit the
 * number of duplicates.
 *
 * This is done by having a fixed sized hash table at the start of the
 * string heap, and allocating space for collision lists in the first
 * 64KiB of the string heap.  After the first 64KiB no extra space is
 * allocated for lists, so hash collisions cannot be resolved.
 */

int
strNil(const char *s)
{
	return GDK_STRNIL(s);
}

size_t
strLen(const char *s)
{
	return GDK_STRLEN(s);
}

/*
static int
strCmp(const char *l, const char *r)
{
	return GDK_STRCMP(l, r);
}
*/

int
strCmpNoNil(const unsigned char *l, const unsigned char *r)
{
	while (*l == *r) {
		if (*l == 0)
			return 0;
		l++;
		r++;
	}
	return (*l < *r) ? -1 : 1;
}

static void
strHeap(Heap *d, size_t cap)
{
	GDKfatal("This should not be triggered when we're just loading data");
}


BUN
strHash(const char *s)
{
	BUN res;

	GDK_STRHASH(s, res);
	return res;
}

/*
 * The strPut routine. The routine strLocate can be used to identify
 * the location of a string in the heap if it exists. Otherwise it
 * returns zero.
 */
var_t
strLocate(Heap *h, const char *v)
{
	stridx_t *ref, *next;
	const size_t extralen = h->hashash ? EXTRALEN : 0;

	/* search hash-table, if double-elimination is still in place */
	BUN off;
	GDK_STRHASH(v, off);
	off &= GDK_STRHASHMASK;

	/* should only use strLocate iff fully double eliminated */
	assert(GDK_ELIMBASE(h->free) == 0);

	/* search the linked list */
	for (ref = ((stridx_t *) h->base) + off; *ref; ref = next) {
		next = (stridx_t *) (h->base + *ref);
		if (GDK_STRCMP(v, (str) (next + 1) + extralen) == 0)
			return (var_t) ((sizeof(stridx_t) + *ref + extralen));	/* found */
	}
	return 0;
}

void
strCleanHash(Heap *h, int rebuild)
{
	stridx_t newhash[GDK_STRHASHTABLE];
	size_t pad, pos;
	const size_t extralen = h->hashash ? EXTRALEN : 0;
	BUN off, strhash;
	const char *s;

	(void) rebuild;
	if (!h->cleanhash)
		return;
	/* rebuild hash table for double elimination
	 *
	 * If appending strings to the BAT was aborted, if the heap
	 * was memory mapped, the hash in the string heap may well be
	 * incorrect.  Therefore we don't trust it when we read in a
	 * string heap and we rebuild the complete table (it is small,
	 * so this won't take any time at all). ... but since
	 * we're only loading data and not writing anything, we can't
	 * write back the computed hash into the heap h, since it might
	 * be memory-mapped to its on-disk file. We're thus only
	 * verifying that the loaded hash does actually turn out to
	 * be valid. */
	memset(newhash, 0, sizeof(newhash));
	pos = GDK_STRHASHSIZE;
	while (pos < h->free && pos < GDK_ELIMLIMIT) {
		pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
		if (pad < sizeof(stridx_t))
			pad += GDK_VARALIGN;
		pos += pad + extralen;
		s = h->base + pos;
		if (h->hashash)
			strhash = ((const BUN *) s)[-1];
		else
			GDK_STRHASH(s, strhash);
		off = strhash & GDK_STRHASHMASK;
		newhash[off] = (stridx_t) (pos - extralen - sizeof(stridx_t));
		pos += GDK_STRLEN(s);
	}
	/* only set dirty flag if the hash table actually changed */
	if (memcmp(newhash, h->base, sizeof(newhash)) != 0) {
		/*
		 * TODO: Make sure this doesn't actually write anything back to disk.
		 * although... it looks like it would, for non-private-mmap'ped columns :-(
		 */
		memcpy(h->base, newhash, sizeof(newhash));
	}
#ifndef NDEBUG
	if (GDK_ELIMDOUBLES(h)) {
		pos = GDK_STRHASHSIZE;
		while (pos < h->free) {
			pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
			if (pad < sizeof(stridx_t))
				pad += GDK_VARALIGN;
			pos += pad + extralen;
			s = h->base + pos;
			assert(strLocate(h, s) != 0);
			pos += GDK_STRLEN(s);
		}
	}
#endif
	h->cleanhash = 0;
}

static var_t
strPut(Heap *h, var_t *dst, const char *v)
{
	GDKfatal("should not be calling strPut when just loading data");
	return 0;
}

/*
 * Convert an "" separated string to a GDK string value, checking that
 * the input is correct UTF-8.
 */

/*
   UTF-8 encoding is as follows:
U-00000000 - U-0000007F: 0xxxxxxx
U-00000080 - U-000007FF: 110xxxxx 10xxxxxx
U-00000800 - U-0000FFFF: 1110xxxx 10xxxxxx 10xxxxxx
U-00010000 - U-001FFFFF: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
U-00200000 - U-03FFFFFF: 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
U-04000000 - U-7FFFFFFF: 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
*/
/* To be correctly coded UTF-8, the sequence should be the shortest
 * possible encoding of the value being encoded.  This means that for
 * an encoding of length n+1 (1 <= n <= 5), at least one of the bits
 * in utf8chkmsk[n] should be non-zero (else the encoding could be
 * shorter). */
static int utf8chkmsk[] = {
	0x0000007f,
	0x00000780,
	0x0000f800,
	0x001f0000,
	0x03e00000,
	0x7c000000,
};

ssize_t
GDKstrFromStr(unsigned char *restrict dst, const unsigned char *restrict src, ssize_t len)
{
	unsigned char *p = dst;
	const unsigned char *cur = src, *end = src + len;
	int escaped = FALSE, mask = 0, n, c, utf8char = 0;

	if (len >= 2 && strcmp((const char *) src, str_nil) == 0) {
		strcpy((char *) dst, str_nil);
		return 1;
	}

	/* copy it in, while performing the correct escapes */
	/* n is the number of follow-on bytes left in a multi-byte
	 * UTF-8 sequence */
	for (cur = src, n = 0; cur < end || escaped; cur++) {
		/* first convert any \ escapes and store value in c */
		if (escaped) {
			switch (*cur) {
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
				/* \ with up to three octal digits */
				c = base08(*cur);
				if (num08(cur[1])) {
					cur++;
					c = mult08(c) + base08(*cur);
					if (num08(cur[1])) {
						if (c > 037) {
							/* octal
							 * escape
							 * sequence
							 * out or
							 * range */
							GDKerror("not an octal number\n");
							return -1;
						}
						cur++;
						c = mult08(c) + base08(*cur);
						assert(c >= 0 && c <= 0377);
					}
				}
				break;
			case 'x':
				/* \x with one or two hexadecimal digits */
				if (num16(cur[1])) {
					cur++;
					c = base16(*cur);
					if (num16(cur[1])) {
						cur++;
						c = mult16(c) + base16(*cur);
					}
				} else
					c = 'x';
				break;
			case 'a':
				c = '\a';
				break;
			case 'b':
				c = '\b';
				break;
			case 'f':
				c = '\f';
				break;
			case 'n':
				c = '\n';
				break;
			case 'r':
				c = '\r';
				break;
			case 't':
				c = '\t';
				break;
			case '\0':
				c = '\\';
				break;
			case '\'':
			case '\\':
				/* \' and \\ can be handled by the
				 * default case */
			default:
				/* unrecognized \ escape, just copy
				 * the backslashed character */
				c = *cur;
				break;
			}
			escaped = FALSE;
		} else if ((c = *cur) == '\\') {
			escaped = TRUE;
			continue;
		}

		if (n > 0) {
			/* we're still expecting follow-up bytes in a
			 * UTF-8 sequence */
			if ((c & 0xC0) != 0x80) {
				/* incorrect UTF-8 sequence: byte is
				 * not 10xxxxxx */
				goto notutf8;
			}
			utf8char = (utf8char << 6) | (c & 0x3F);
			n--;
			if (n == 0) {
				/* this was the last byte in the sequence */
				if ((utf8char & mask) == 0) {
					/* incorrect UTF-8 sequence:
					 * not shortest possible */
					goto notutf8;
				}
				if (utf8char > 0x10FFFF) {
					/* incorrect UTF-8 sequence:
					 * value too large */
					goto notutf8;
				}
				if ((utf8char & 0x1FFF800) == 0xD800) {
					/* incorrect UTF-8 sequence:
					 * low or high surrogate
					 * encoded as UTF-8 */
					goto notutf8;
				}
			}
		} else if (c >= 0x80) {
			int m;

			/* start of multi-byte UTF-8 character */
			for (n = 0, m = 0x40; c & m; n++, m >>= 1)
				;
			/* n now is number of 10xxxxxx bytes that
			 * should follow */
			if (n == 0 || n >= 4) {
				/* incorrect UTF-8 sequence */
				/* n==0: c == 10xxxxxx */
				/* n>=4: c == 11111xxx */
				goto notutf8;
			}
			mask = utf8chkmsk[n];
			/* collect the Unicode code point in utf8char */
			utf8char = c & ~(0xFFC0 >> n);	/* remove non-x bits */
		}
		*p++ = c;
	}
	if (n > 0) {
		/* incomplete UTF-8 sequence */
		goto notutf8;
	}
	*p++ = 0;
	return len;
  notutf8:
	GDKerror("not a proper UTF-8 sequence\n");
	return -1;
}

ssize_t
strFromStr(const char *restrict src, size_t *restrict len, char **restrict dst)
{
	const char *cur = src, *start = NULL;
	size_t l = 1;
	int escaped = FALSE;

	if (GDK_STRNIL(src)) {
		atommem(2);
		strcpy(*dst, str_nil);
		return 1;
	}

	while (GDKisspace(*cur))
		cur++;
	if (*cur != '"') {
		if (strncmp(cur, "nil", 3) == 0) {
			atommem(2);
			strcpy(*dst, str_nil);
			return (ssize_t) (cur - src) + 3;
		}
		GDKerror("not a quoted string\n");
		return -1;
	}

	/* scout the string to find out its length and whether it was
	 * properly quoted */
	for (start = ++cur; *cur != '"' || escaped; cur++) {
		if (*cur == 0) {
			GDKerror("no closing quotes\n");
			return -1;
		} else if (*cur == '\\' && escaped == FALSE) {
			escaped = TRUE;
		} else {
			escaped = FALSE;
			l++;
		}
	}

	/* alloc new memory */
	if (*dst == NULL || *len < l) {
		GDKfree(*dst);
		*dst = GDKmalloc(*len = l);
		if (*dst == NULL) {
			*len = 0;
			return -1;
		}
	}

	return GDKstrFromStr((unsigned char *) *dst,
			     (const unsigned char *) start,
			     (ssize_t) (cur - start));
}

/*
 * Convert a GDK string value to something printable.
 */
/* all but control characters (in range 0 to 31) and DEL */
#ifdef ASCII_CHR
/* ASCII printable characters */
#define printable_chr(ch)	(' ' <= (ch) && (ch) <= '~')
#else
/* everything except ASCII control characters */
#define printable_chr(ch)	((' ' <= (ch) && (ch) <= '~') || ((ch) & 0x80) != 0)
#endif

size_t
escapedStrlen(const char *restrict src, const char *sep1, const char *sep2, int quote)
{
	size_t end, sz = 0;
	size_t sep1len, sep2len;

	sep1len = sep1 ? strlen(sep1) : 0;
	sep2len = sep2 ? strlen(sep2) : 0;
	for (end = 0; src[end]; end++)
		if (src[end] == '\\' ||
		    src[end] == quote ||
		    (sep1len && strncmp(src + end, sep1, sep1len) == 0) ||
		    (sep2len && strncmp(src + end, sep2, sep2len) == 0)) {
			sz += 2;
#ifndef ASCII_CHR
		} else if (src[end] == (char) '\302' &&
			   0200 <= ((int) src[end + 1] & 0377) &&
			   ((int) src[end + 1] & 0377) <= 0237) {
			/* Unicode control character (code point range
			 * U-00000080 through U-0000009F encoded in
			 * UTF-8 */
			/* for the first one of the two UTF-8 bytes we
			 * count a width of 7 and for the second one
			 * 1, together that's 8, i.e. the width of two
			 * backslash-escaped octal coded characters */
			sz += 7;
#endif
		} else if (!printable_chr(src[end])) {
			sz += 4;
		} else {
			sz++;
		}
	return sz;
}

size_t
escapedStr(char *restrict dst, const char *restrict src, size_t dstlen, const char *sep1, const char *sep2, int quote)
{
	size_t cur = 0, l = 0;
	size_t sep1len, sep2len;

	sep1len = sep1 ? strlen(sep1) : 0;
	sep2len = sep2 ? strlen(sep2) : 0;
	for (; src[cur] && l < dstlen; cur++)
		if (!printable_chr(src[cur])
#ifndef ASCII_CHR
		    || (src[cur] == '\302' &&
			0200 <= (src[cur + 1] & 0377) &&
			((int) src[cur + 1] & 0377) <= 0237)
		    || (cur > 0 &&
			src[cur - 1] == '\302' &&
			0200 <= (src[cur] & 0377) &&
			(src[cur] & 0377) <= 0237)
#endif
			) {
			dst[l++] = '\\';
			switch (src[cur]) {
			case '\t':
				dst[l++] = 't';
				break;
			case '\n':
				dst[l++] = 'n';
				break;
			case '\r':
				dst[l++] = 'r';
				break;
			case '\f':
				dst[l++] = 'f';
				break;
			default:
				snprintf(dst + l, dstlen - l, "%03o", (unsigned char) src[cur]);
				l += 3;
				break;
			}
		} else if (src[cur] == '\\' ||
			   src[cur] == quote ||
			   (sep1len && strncmp(src + cur, sep1, sep1len) == 0) ||
			   (sep2len && strncmp(src + cur, sep2, sep2len) == 0)) {
			dst[l++] = '\\';
			dst[l++] = src[cur];
		} else {
			dst[l++] = src[cur];
		}
	assert(l < dstlen);
	dst[l] = 0;
	return l;
}

/*
 * String conversion routines.
 */
ssize_t
OIDfromStr(const char *src, size_t *len, oid **dst)
{
#if SIZEOF_OID == SIZEOF_INT
	int ui = 0, *uip = &ui;
#else
	lng ui = 0, *uip = &ui;
#endif
	size_t l = sizeof(ui);
	ssize_t pos = 0;
	const char *p = src;

	atommem(sizeof(oid));

	**dst = oid_nil;
	if (GDK_STRNIL(src))
		return 1;

	while (GDKisspace(*p))
		p++;

	if (strncmp(p, "nil", 3) == 0)
		return (ssize_t) (p - src) + 3;

	if (GDKisdigit(*p)) {
#if SIZEOF_OID == SIZEOF_INT
		pos = intFromStr(p, &l, &uip);
#else
		pos = lngFromStr(p, &l, &uip);
#endif
		if (pos < 0)
			return pos;
		if (p[pos] == '@') {
			pos++;
			while (GDKisdigit(p[pos]))
				pos++;
		}
		if (ui >= 0) {
			**dst = ui;
		}
		p += pos;
	}
	while (GDKisspace(*p))
		p++;
	return (ssize_t) (p - src);
}

ssize_t
OIDtoStr(char **dst, size_t *len, const oid *src)
{
	atommem(oidStrlen);

	if (is_oid_nil(*src)) {
		return snprintf(*dst, *len, "nil");
	}
	return snprintf(*dst, *len, OIDFMT "@0", *src);
}

#include "monetdb5/modules/atoms/mtime.h"

atomDesc BATatoms[MAXATOMS] = {
	{"void",		/* name */
	 TYPE_void,		/* storage */
	 true,			/* linear */
	 0,			/* size */
#if SIZEOF_OID == SIZEOF_INT
	 (ptr) &int_nil,	/* atomNull */
#else
	 (ptr) &lng_nil,	/* atomNull */
#endif
	 (ssize_t (*)(const char *, size_t *, ptr *)) OIDfromStr,    /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) OIDtoStr,      /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,      /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
#if SIZEOF_OID == SIZEOF_INT
	 (int (*)(const void *, const void *)) NULL,	      /* atomCmp */
	 (BUN (*)(const void *)) NULL,		      /* atomHash */
#else
	 (int (*)(const void *, const void *)) NULL,	      /* atomCmp */
	 (BUN (*)(const void *)) NULL,		      /* atomHash */
#endif
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"bit",			/* name */
	 TYPE_bte,		/* storage */
	 true,			/* linear */
	 sizeof(bit),		/* size */
	 (ptr) &bte_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) NULL,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"bte",			/* name */
	 TYPE_bte,		/* storage */
	 true,			/* linear */
	 sizeof(bte),		/* size */
	 (ptr) &bte_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) bteFromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"sht",			/* name */
	 TYPE_sht,		/* storage */
	 true,			/* linear */
	 sizeof(sht),		/* size */
	 (ptr) &sht_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) shtFromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"BAT",			/* name */
	 TYPE_int,		/* storage */
	 true,			/* linear */
	 sizeof(bat),		/* size */
	 (ptr) &int_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) NULL,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 (int (*)(const void *)) NULL,		     /* atomFix */
	 (int (*)(const void *)) NULL,		     /* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"int",			/* name */
	 TYPE_int,		/* storage */
	 true,			/* linear */
	 sizeof(int),		/* size */
	 (ptr) &int_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) intFromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"oid",			/* name */
#if SIZEOF_OID == SIZEOF_INT
	 TYPE_int,		/* storage */
#else
	 TYPE_lng,		/* storage */
#endif
	 true,			/* linear */
	 sizeof(oid),		/* size */
#if SIZEOF_OID == SIZEOF_INT
	 (ptr) &int_nil,	/* atomNull */
#else
	 (ptr) &lng_nil,	/* atomNull */
#endif
	 (ssize_t (*)(const char *, size_t *, ptr *)) OIDfromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) OIDtoStr,     /* atomToStr */
#if SIZEOF_OID == SIZEOF_INT
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
#else
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
#endif
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"ptr",			/* name */
	 TYPE_ptr,		/* storage */
	 true,			/* linear */
	 sizeof(ptr),		/* size */
	 (ptr) &ptr_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) ptrFromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) ptrToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
#if SIZEOF_VOID_P == SIZEOF_INT
	 (int (*)(const void *, const void *)) NULL,       /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
#else /* SIZEOF_VOID_P == SIZEOF_LNG */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
#endif
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"flt",			/* name */
	 TYPE_flt,		/* storage */
	 true,			/* linear */
	 sizeof(flt),		/* size */
	 (ptr) &flt_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) fltFromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"dbl",			/* name */
	 TYPE_dbl,		/* storage */
	 true,			/* linear */
	 sizeof(dbl),		/* size */
	 (ptr) &dbl_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) dblFromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"lng",			/* name */
	 TYPE_lng,		/* storage */
	 true,			/* linear */
	 sizeof(lng),		/* size */
	 (ptr) &lng_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) lngFromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
#ifdef HAVE_HGE
	{"hge",			/* name */
	 TYPE_hge,		/* storage */
	 true,			/* linear */
	 sizeof(hge),		/* size */
	 (ptr) &hge_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) hgeFromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
#endif
	{"str",			/* name */
	 TYPE_str,		/* storage */
	 true,			/* linear */
	 sizeof(var_t),		/* size */
	 (ptr) str_nil,		/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) strFromStr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) NULL,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) strHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 (var_t (*)(Heap *, var_t *, const void *)) strPut,  /* atomPut */
	 0,			/* atomDel */
	 (size_t (*)(const void *)) strLen,		     /* atomLen */
	 strHeap,		/* atomHeap */
	},

	{"date",			/* name */
	 TYPE_int,		/* storage */
	 1,			/* linear */
	 sizeof(date),		/* size */
	 (ptr) &int_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) date_fromstr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) date_tostr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},

	{"daytime",			/* name */
	 TYPE_int,		/* storage */
	 1,			/* linear */
	 sizeof(daytime),		/* size */
	 (ptr) &int_nil,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) daytime_fromstr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) daytime_tostr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},

	{"timestamp",			/* name */
	 TYPE_timestamp,		/* storage */
	 1,			/* linear */
	 sizeof(timestamp),		/* size */
	 (ptr) &ts_nil.ts,	/* atomNull */
	 (ssize_t (*)(const char *, size_t *, ptr *)) timestamp_fromstr,   /* atomFromStr */
	 (ssize_t (*)(str *, size_t *, const void *)) timestamp_tostr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) NULL,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) NULL, /* atomWrite */
	 (int (*)(const void *, const void *)) NULL,	     /* atomCmp */
	 (BUN (*)(const void *)) NULL,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},

};

int GDKatomcnt = TYPE_last_builtin + 1;

/*
 * Sometimes a bat descriptor is loaded before the dynamic module
 * defining the atom is loaded. To support this an extra set of
 * unknown atoms is kept.  These can be accessed via the ATOMunknown
 * interface. Finding an (negative) atom index can be done via
 * ATOMunknown_find, which simply adds the atom if it's not in the
 * unknown set. The index van be used to find the name of an unknown
 * ATOM via ATOMunknown_name.
 */
static str unknown[MAXATOMS] = { NULL };

int
ATOMunknown_find(const char *nme)
{
	/* multithreaded-related instructions removed. */
	int i, j = 0;

	for (i = 1; i < MAXATOMS; i++) {
		if (unknown[i]) {
			if (strcmp(unknown[i], nme) == 0) {
				return -i;
			}
		} else if (j == 0)
			j = i;
	}
	if (j == 0) {
		/* no space for new atom (shouldn't happen) */
		return 0;
	}
	if ((unknown[j] = GDKstrdup(nme)) == NULL) {
		return 0;
	}
	return -j;
}

str
ATOMunknown_name(int i)
{
	assert(unknown[-i]);
	return unknown[-i];
}
/*
 Do we really need this one?

void
ATOMunknown_clean(void)
{
	int i;

	MT_lock_set(&GDKthreadLock);
	for (i = 1; i < MAXATOMS; i++) {
		if(unknown[i]) {
			GDKfree(unknown[i]);
			unknown[i] = NULL;
		} else {
			break;
		}
	}
	MT_lock_unset(&GDKthreadLock);
}*/
