/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* String Atom Implementation
 *
 * Strings are stored in two parts.  The first part is the normal tail
 * heap which contains a list of offsets.  The second part is the
 * theap which contains the actual strings.  The offsets in the tail
 * heap (a.k.a. offset heap) point into the theap (a.k.a. string
 * heap).  Strings are NULL-terminated and are stored without any
 * escape sequences.  Strings are encoded using the UTF-8 encoding
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

/* some of these macros are duplicates from gdk_atoms.c */
#define num08(x)	((x) >= '0' && (x) <= '7')
#define base08(x)	((x) - '0')
#define mult08(x)	((x) << 3)

#define num16(x)	isxdigit((unsigned char) (x))
#define base16(x)	(((x) >= 'a' && (x) <= 'f') ? ((x) - 'a' + 10) : ((x) >= 'A' && (x) <= 'F') ? ((x) - 'A' + 10) : (x) - '0')
#define mult16(x)	((x) << 4)

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

const char str_nil[2] = { '\200', 0 };

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

int
strCmp(const char *l, const char *r)
{
	return GDK_STRCMP(l, r);
}

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

BUN
strHash(const char *s)
{
	BUN res;

	GDK_STRHASH(s, res);
	return res;
}

void
strCleanHash(Heap *h, bool rebuild)
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
	 * so this won't take any time at all).
	 * Note that we will only do this the first time the heap is
	 * loaded, and only for heaps that existed when the server was
	 * started. */
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

/*
		if (h->storage == STORE_MMAP) {
			if (!(GDKdebug & NOSYNCMASK))
				(void) MT_msync(h->base, GDK_STRHASHSIZE);
		} else
			h->dirty = true;
*/
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
	h->cleanhash = false;
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

/* TODO: Probably unused, remove this from the atom table (from other GDK versions as well. */
var_t
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
	bool escaped = false;
	int mask = 0, n, c, utf8char = 0;

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
			case 'u':
			case 'U':
				/* \u with four hexadecimal digits or
				 * \U with eight hexadecimal digits */
				if (n > 0) {
					/* not when in the middle of a
					 * UTF-8 sequence */
					goto notutf8;
				}
				c = 0;
				for (n = *cur == 'U' ? 8 : 4; n > 0; n--) {
					cur++;
					if (!num16(*cur)) {
						GDKerror("not a Unicode code point escape\n");
						return -1;
					}
					c = c << 4 | base16(*cur);
				}
				/* n == 0 now */
				if (c == 0 || c > 0x10FFFF ||
				    (c & 0xFFF800) == 0xD800) {
					GDKerror("illegal Unicode code point\n");
					return -1;
				}
				if (c < 0x80) {
					*p++ = (unsigned char) c;
				} else {
					if (c < 0x800) {
						*p++ = 0xC0 | (c >> 6);
					} else {
						if (c < 0x10000) {
							*p++ = 0xE0 | (c >> 12);
						} else {
							*p++ = 0xF0 | (c >> 18);
							*p++ = 0x80 | ((c >> 12) & 0x3F);
						}
						*p++ = 0x80 | ((c >> 6) & 0x3F);
					}
					*p++ = 0x80 | (c & 0x3F);
				}
				escaped = false;
				continue;
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
			escaped = false;
		} else if ((c = *cur) == '\\') {
			escaped = true;
			continue;
#if 0
		} else if (c == quote && cur[1] == quote) {
			assert(c != 0);
			if (n > 0)
				goto notutf8;
			*p++ = quote;
			cur++;
			continue;
#endif
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
strFromStr(const char *restrict src, size_t *restrict len, char **restrict dst, bool external)
{
	const char *cur = src, *start = NULL;
	size_t l = 1;
	bool escaped = false;

	if (!external) {
		size_t sz = strLen(src);
		atommem(sz);
		strncpy(*dst, src, sz + 1);
		return (ssize_t) sz;
	}

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
		} else if (*cur == '\\' && !escaped) {
			escaped = true;
		} else {
			escaped = false;
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
		if (src[end] == '\\'
		    || src[end] == quote
		    || (sep1len && strncmp(src + end, sep1, sep1len) == 0)
		    || (sep2len && strncmp(src + end, sep2, sep2len) == 0)) {
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
		    || (src[cur] == '\302'
			&& 0200 <= (src[cur + 1] & 0377)
			&& ((int) src[cur + 1] & 0377) <= 0237)
		    || (cur > 0
			&& src[cur - 1] == '\302'
			&& 0200 <= (src[cur] & 0377)
			&& (src[cur] & 0377) <= 0237)
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
		} else if (src[cur] == '\\'
			   || src[cur] == quote
			   || (sep1len && strncmp(src + cur, sep1, sep1len) == 0)
			   || (sep2len && strncmp(src + cur, sep2, sep2len) == 0)) {
			dst[l++] = '\\';
			dst[l++] = src[cur];
		} else {
			dst[l++] = src[cur];
		}
	assert(l < dstlen);
	dst[l] = 0;
	return l;
}

ssize_t
strToStr(char **restrict dst, size_t *restrict len, const char *restrict src, bool external)
{
	size_t sz;

	if (!external) {
		sz = strLen(src);
		atommem(sz);
		strncpy(*dst, src, sz + 1);
		return (ssize_t) sz;
	}
	if (GDK_STRNIL(src)) {
		atommem(4);
		strcpy(*dst, "nil");
		return 3;
	} else {
		ssize_t l = 0;
		size_t sz = escapedStrlen(src, NULL, NULL, '"');

		atommem(sz + 3);
		l = (ssize_t) escapedStr((*dst) + 1, src, *len - 1, NULL, NULL, '"');
		l++;
		(*dst)[0] = (*dst)[l++] = '"';
		(*dst)[l] = 0;
		return l;
	}
}
