/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/* This file should not be included in any file outside of this directory */

enum heaptype {
	offheap,
	varheap,
	hashheap,
	imprintsheap,
	orderidxheap
};

__hidden int ATOMunknown_find(const char *nme)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
__hidden str ATOMunknown_name(int a)
	__attribute__((__visibility__("hidden")));
__hidden void BATdestroy(BAT *b)
	__attribute__((__visibility__("hidden")));
__hidden void BATfree(BAT *b)
	__attribute__((__visibility__("hidden")));
__hidden gdk_return BATgroup_internal(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *s, BAT *g, BAT *e, BAT *h, bool subsorted)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
__hidden void BATinit_idents(BAT *bn)
	__attribute__((__visibility__("hidden")));
__hidden BAT *BATload_intern(BBP* bbp, bat bid, bool lock)
	__attribute__((__visibility__("hidden")));
__hidden gdk_return BBPcacheit(BBP* bbp, BAT *bn, bool lock)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
__hidden void BBPexit(BBP* bbp)
	__attribute__((__visibility__("hidden")));
__hidden BAT *BBPgetdesc(BBP* bbp, bat i)
	__attribute__((__visibility__("hidden")));
/*__hidden void gdk_bbp_reset(void)
	__attribute__((__visibility__("hidden")));*/
__hidden gdk_return GDKextend(const char *fn, size_t size)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
__hidden int GDKfdlocate(const char* db_farm_dir, const char *nme, const char *mode, const char *ext)
	__attribute__((__visibility__("hidden")));
__hidden FILE *GDKfilelocate(const char* db_farm_dir, const char *nme, const char *mode, const char *ext)
	__attribute__((__visibility__("hidden")));
__hidden FILE *GDKfileopen(const char* db_farm_dir, const char *dir, const char *name, const char *extension, const char *mode)
	__attribute__((__visibility__("hidden")));
__hidden char *GDKload(const char* db_farm_dir, const char *nme, const char *ext, size_t size, size_t *maxsize, storage_t mode)
	__attribute__((__visibility__("hidden")));
__hidden void GDKlog(_In_z_ _Printf_format_string_ FILE * fl, const char *format, ...)
	__attribute__((__format__(__printf__, 2, 3)))
	__attribute__((__visibility__("hidden")));
__hidden gdk_return GDKmove(int farmid, const char *dir1, const char *nme1, const char *ext1, const char *dir2, const char *nme2, const char *ext2)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
__hidden void *GDKmremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
	__attribute__((__visibility__("hidden")));
__hidden gdk_return GDKmunmap(void *addr, size_t len)
	__attribute__((__visibility__("hidden")));
__hidden void HASHfree(BAT *b)
	__attribute__((__visibility__("hidden")));
__hidden void HEAPfree(Heap *h) /* never removing/deleting the heap file */
	__attribute__((__visibility__("hidden")));
__hidden gdk_return HEAPload(Heap *h, const char *nme, const char *ext, bool trunc)
	__attribute__((__visibility__("hidden")));
/* Do we need this?
__hidden void HEAP_recover(Heap *, const var_t *, BUN)
	__attribute__((__visibility__("hidden")));
*/
static inline char *
stpconcat(char *restrict dst, const char *src, ...)
{
	va_list ap;

	va_start(ap, src);
	while (src) {
		dst = stpcpy(dst, src);
		src = va_arg(ap, const char *);
	}
	va_end(ap);
	return dst;
}
__hidden void strCleanHash(Heap *hp, bool rebuild)
	__attribute__((__visibility__("hidden")));
__hidden int strCmp(const char *l, const char *r)
	__attribute__((__visibility__("hidden")));
__hidden int strCmpNoNil(const unsigned char *l, const unsigned char *r)
	__attribute__((__visibility__("hidden")));
__hidden void strHeap(Heap *d, size_t cap)
	__attribute__((__visibility__("hidden")));
__hidden var_t strLocate(Heap *h, const char *v)
	__attribute__((__visibility__("hidden")));
__hidden var_t strPut(Heap *h, var_t *dst, const char *v)
	__attribute__((__visibility__("hidden")));
__hidden str strRead(str a, stream *s, size_t cnt)
	__attribute__((__visibility__("hidden")));
__hidden ssize_t strToStr(char **restrict dst, size_t *restrict len, const char *restrict src, bool external)
	__attribute__((__visibility__("hidden")));
__hidden void gdk_system_reset(void)
	__attribute__((__visibility__("hidden")));

#define BBP_BATMASK	(128 * SIZEOF_SIZE_T - 1)
#define BBP_THREADMASK	63

struct PROPrec {
	enum prop_t id;
	ValRecord v;
	struct PROPrec *next;	/* simple chain of properties */
};

#define MAXFARMS       32

extern size_t GDK_mmap_minsize_persistent; /* size after which we use memory mapped files for persistent heaps */
extern size_t GDK_mmap_minsize_transient; /* size after which we use memory mapped files for transient heaps */
extern size_t GDK_mmap_pagesize; /* mmap granularity */

/* extra space in front of strings in string heaps when hashash is set
 * if at least (2*SIZEOF_BUN), also store length (heaps are then
 * incompatible) */
#define EXTRALEN ((SIZEOF_BUN + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1))

#if !defined(NDEBUG) && !defined(STATIC_CODE_ANALYSIS)
/* see comment in gdk.h */
#ifdef __GNUC__
#define GDKmunmap(p, l)						\
	({	void *_ptr = (p);				\
		size_t _len = (l);				\
		gdk_return _res = GDKmunmap(_ptr, _len);	\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#GDKmunmap(%p,%zu) -> %u"	\
				" %s[%s:%d]\n",			\
				_ptr, _len, _res,		\
				__func__, __FILE__, __LINE__);	\
		_res;						\
	})
#define GDKmremap(p, m, oa, os, ns)					\
	({								\
		const char *_path = (p);				\
		int _mode = (m);					\
		void *_oa = (oa);					\
		size_t _os = (os);					\
		size_t *_ns = (ns);					\
		size_t _ons = *_ns;					\
		void *_res = GDKmremap(_path, _mode, _oa, _os, _ns);	\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKmremap(%s,0x%x,%p,%zu,%zu > %zu) -> %p" \
				" %s[%s:%d]\n",				\
				_path ? _path : "NULL", (unsigned) _mode, \
				_oa, _os, _ons, *_ns,			\
				_res,					\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	 })
#else
static inline gdk_return
GDKmunmap_debug(void *ptr, size_t len, const char *filename, int lineno)
{
	gdk_return res = GDKmunmap(ptr, len);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKmunmap(%p,%zu) -> %d [%s:%d]\n",
			   ptr, len, (int) res, filename, lineno);
	return res;
}
#define GDKmunmap(p, l)		GDKmunmap_debug((p), (l), __FILE__, __LINE__)
static inline void *
GDKmremap_debug(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size, const char *filename, int lineno)
{
	size_t orig_new_size = *new_size;
	void *res = GDKmremap(path, mode, old_address, old_size, new_size);
	ALLOCDEBUG
		fprintf(stderr,
			"#GDKmremap(%s,0x%x,%p,%zu,%zu > %zu) -> %p"
			" [%s:%d]\n",
			path ? path : "NULL", mode,
			old_address, old_size, orig_new_size, *new_size,
			res,
			filename, lineno);
	return res;
}
#define GDKmremap(p, m, oa, os, ns)	GDKmremap_debug(p, m, oa, os, ns, __FILE__, __LINE__)

#endif
#endif
