/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a M. L. Kersten, P. Boncz, N. Nes
 *
 * @* Utilities
 * The utility section contains functions to initialize the Monet
 * database system, memory allocation details, and a basic system
 * logging scheme.
 */
#include "monetdb_config.h"

#include "gdk.h"
#include "gdk_private.h"

int GDKdebug = 0;

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#ifdef HAVE_PWD_H
# include <pwd.h>
#endif

#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>  /* prerequisite of sys/sysctl on OpenBSD */
#endif
#ifdef HAVE_SYS_SYSCTL_H
# include <sys/sysctl.h>
#endif

#undef malloc
#undef calloc
#undef realloc
#undef free

/* memory thresholds; these values some "sane" constants only, really
 * set in GDKinit() */
size_t GDK_mmap_minsize = (size_t) 1 << 18;
size_t GDK_mmap_pagesize = (size_t) 1 << 16; /* mmap granularity */

int GDK_vm_trim = 1;

#define SEG_SIZE(x,y)	((x)+(((x)&((1<<(y))-1))?(1<<(y))-((x)&((1<<(y))-1)):0))
#define MAX_BIT		((int) (sizeof(ssize_t)<<3))

/* Not using atomic intrinsics for these - since we
 * won't be working multi-threaded when just loading data
 */
static volatile size_t GDK_mallocedbytes_estimate = 0;
static volatile size_t GDK_vm_cursize = 0;


/*
 * @+ Session Initialization
 * The interface code to the operating system is highly dependent on
 * the processing environment. It can be filtered away with
 * compile-time flags.  Suicide is necessary due to some system
 * implementation errors.
 *
 * The kernel requires file descriptors for I/O with the user.  They
 * are thread specific and should be obtained by a function.
 *
 * The arguments relevant for the kernel are extracted from the list.
 * Their value is turned into a blanc space.
 */

#define GDKERRLEN	(1024+512)

void
GDKerror(const char *format, ...)
{
	char message[GDKERRLEN];
	size_t len = strlen(GDKERROR);
	va_list ap;

	if (!strncmp(format, GDKERROR, len)) {
		len = 0;
	} else {
		strcpy(message, GDKERROR);
	}
	va_start(ap, format);
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
	va_end(ap);

	// When only loading data, let's simplify error message printing
	fputs(message, stderr);
	fputc('\n', stderr);
	fflush(stderr);
}

void
GDKsyserror(const char *format, ...)
{
	char message[GDKERRLEN];
	size_t len = strlen(GDKERROR);

	int err = errno;
	va_list ap;

	if (strncmp(format, GDKERROR, len) == 0) {
		len = 0;
	} else {
		strncpy(message, GDKERROR, sizeof(message));
	}
	va_start(ap, format);
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
	va_end(ap);
#ifndef NATIVE_WIN32
	if (err > 0 && err < 1024)
#endif
	{
		size_t len1;
		size_t len2;
		size_t len3;
		char *osmsg;
#ifdef NATIVE_WIN32
		char osmsgbuf[256];
		osmsg = osmsgbuf;
		FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, err,
			      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
			      (LPTSTR) osmsgbuf, sizeof(osmsgbuf), NULL);
#else
		osmsg = strerror(err);
#endif
		len1 = strlen(message);
		len2 = len1 + strlen(GDKMESSAGE);
		len3 = len2 + strlen(osmsg);

		if (len3 + 2 < sizeof(message)) {
			strcpy(message + len1, GDKMESSAGE);
			strcpy(message + len2, osmsg);
			if (len3 > 0 && message[len3 - 1] != '\n') {
				message[len3] = '\n';
				message[len3 + 1] = 0;
			}
		}
	}
	/* 
	 * When only loading data, let's simplify error message printing -
	 * by not using GDKaddbuf() and related functions.
	 */
	fprintf(stderr, "%s", message);

	errno = 0;
}

jmp_buf GDKfataljump;
str GDKfatalmsg;
bit GDKfataljumpenable = 0;

/* coverity[+kill] */
void
GDKfatal(const char *format, ...)
{
	char message[GDKERRLEN];
	size_t len = strlen(GDKFATAL);
	va_list ap;

	GDKdebug |= IOMASK;
	if (!strncmp(format, GDKFATAL, len)) {
		len = 0;
	} else {
		strcpy(message, GDKFATAL);
	}
	va_start(ap, format);
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
	va_end(ap);

	if (!GDKfataljumpenable) {
		fputs(message, stderr);
		fputs("\n", stderr);
		fflush(stderr);

		/* 
		 * We're never GDKexit'ing , since we never GDKinit when only
		 * loading data; and we can't (?) use GDKfatal here either.
		 */
		abort();
	} else { // in embedded mode, we really don't want to kill our host
		GDKfatalmsg = GDKstrdup(message);
		longjmp(GDKfataljump, 42);
	}
}

size_t
GDKmem_cursize(void)
{
	/* RAM/swapmem that Monet is really using now */
	return (size_t) GDK_mallocedbytes_estimate;
}

size_t
GDKvm_cursize(void)
{
	/* current Monet VM address space usage */
	return (size_t) GDK_vm_cursize + GDKmem_cursize();
}

#define heapinc(_memdelta)						\
	GDK_mallocedbytes_estimate += _memdelta
#define heapdec(_memdelta)						\
	GDK_mallocedbytes_estimate -= _memdelta

#define meminc(vmdelta)							\
	GDK_vm_cursize += (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG)
#define memdec(vmdelta)							\
	GDK_vm_cursize -= (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG)

/*
 * @+ Malloc
 * Malloc normally maps through directly to the OS provided
 * malloc/free/realloc calls. Where possible, we want to use the
 * -lmalloc library on Unix systems, because it allows to influence
 * the memory allocation strategy. This can prevent fragmentation and
 * greatly help enhance performance.
 *
 * The "added-value" of the GDKmalloc/GDKfree/GDKrealloc over the
 * standard OS primitives is that the GDK versions try to do recovery
 * from failure to malloc by initiating a BBPtrim. Also, big requests
 * are redirected to anonymous virtual memory. Finally, additional
 * information on block sizes is kept (helping efficient
 * reallocations) as well as some debugging that guards against
 * duplicate frees.
 *
 * A number of different strategies are available using different
 * switches, however:
 *
 * - zero sized blocks
 *   Normally, GDK gives fatal errors on illegal block sizes.
 *   This can be overridden with  GDK_MEM_NULLALLOWED.
 *
 * - resource tracking
 *   Many malloc interfaces lack a routine that tells the size of a
 *   block by the pointer. We need this information for correct malloc
 *   statistics.
 *
 * - outstanding block histograms
 *   In order to solve the problem, we allocate extra memory in front
 *   of the returned block. With the resource tracking in place, we
 *   keep a total of allocated bytes.  Also, if GDK_MEM_KEEPHISTO is
 *   defined, we keep a histogram of the outstanding blocks on the
 *   log2 of the block size (similarly for virtual.  memory blocks;
 *   define GDK_VM_KEEPHISTO).
 *
 * 64-bits update: Some 64-bit implementations (Linux) of mallinfo is
 * severely broken, as they use int-s for memory sizes!!  This causes
 * corruption of mallinfo stats. As we depend on those, we should keep
 * the malloc arena small. Thus, VM redirection is now quickly
 * applied: for all mallocs > 1MB.
 */
static void
GDKmemfail(const char *s, size_t len)
{
	int bak = GDKdebug;
	fprintf(stderr, "#%s(" SZFMT ") fails, try to free up space [memory in use=" SZFMT ",virtual memory in use=" SZFMT "]\n", s, len, GDKmem_cursize(), GDKvm_cursize());
	GDKfatal("Can't free up memory in data-loading-only mode - we don't have BBPtrim available");
	GDKdebug = MIN(GDKdebug, bak);
	fprintf(stderr, "#%s(" SZFMT ") result [mem=" SZFMT ",vm=" SZFMT "]\n", s, len, GDKmem_cursize(), GDKvm_cursize());
}

/* the blocksize is stored in the ssize_t before it. Negative size <=>
 * VM memory */
#define GDK_MEM_BLKSIZE(p) ((ssize_t*) (p))[-1]
#ifdef __GLIBC__
#define GLIBC_BUG 8
#else
#define GLIBC_BUG 0
#endif

/* we allocate extra space and return a pointer offset by this amount */
#define MALLOC_EXTRA_SPACE	(2 * SIZEOF_VOID_P)

/* allocate 8 bytes extra (so it stays 8-bytes aligned) and put
 * realsize in front */
static inline void *
GDKmalloc_prefixsize(size_t size)
{
	ssize_t *s;

	if ((s = malloc(size + MALLOC_EXTRA_SPACE + GLIBC_BUG)) != NULL) {
		assert((((uintptr_t) s) & 7) == 0); /* no MISALIGN */
		s = (ssize_t*) ((char*) s + MALLOC_EXTRA_SPACE);
		s[-1] = (ssize_t) (size + MALLOC_EXTRA_SPACE);
	}
	return s;
}

/*
 * The emergency flag can be set to force a fatal error if needed.
 * Otherwise, the caller is able to deal with the lack of memory.
 */
#undef GDKmallocmax
void *
GDKmallocmax(size_t size, size_t *maxsize, int emergency)
{
	void *s;

	if (size == 0) {
#ifdef GDK_MEM_NULLALLOWED
		return NULL;
#else
		GDKfatal("GDKmallocmax: called with size " SZFMT "", size);
#endif
	}
	size = (size + 7) & ~7;	/* round up to a multiple of eight */
	s = GDKmalloc_prefixsize(size);
	if (s == NULL) {
		GDKmemfail("GDKmalloc", size);
		s = GDKmalloc_prefixsize(size);
		if (s == NULL) {
			if (emergency == 0) {
				GDKerror("GDKmallocmax: failed for " SZFMT " bytes", size);
				return NULL;
			}
			GDKfatal("GDKmallocmax: failed for " SZFMT " bytes", size);
		} else {
			fprintf(stderr, "#GDKmallocmax: recovery ok. Continuing..\n");
		}
	}
	*maxsize = size;
	heapinc(size + MALLOC_EXTRA_SPACE);
	return s;
}

#undef GDKmalloc
void *
GDKmalloc(size_t size)
{
	void *p = GDKmallocmax(size, &size, 0);
#ifndef NDEBUG
	DEADBEEFCHK if (p)
		memset(p, 0xBD, size);
#endif
	return p;
}

#undef GDKzalloc
void *
GDKzalloc(size_t size)
{
	size_t maxsize = size;
	void *p = GDKmallocmax(size, &maxsize, 0);
	if (p) {
		memset(p, 0, size);
#ifndef NDEBUG
		/* DeadBeef allocated area beyond what was requested */
		DEADBEEFCHK if (maxsize > size)
			memset((char *) p + size, 0xBD, maxsize - size);
#endif
	}
	return p;
}

#undef GDKfree
void
GDKfree(void *blk)
{
	ssize_t size = 0, *s = (ssize_t *) blk;

	if (s == NULL)
		return;

	size = GDK_MEM_BLKSIZE(s);

	/* check against duplicate free */
	assert((size & 2) == 0);

	assert(size != 0);

#ifndef NDEBUG
	/* The check above detects obvious duplicate free's, but fails
	 * in case the "check-bit" is cleared between two free's
	 * (e.g., as the respective memory has been re-allocated and
	 * initialized.
	 * To simplify detection & debugging of duplicate free's, we
	 * now overwrite the to be freed memory, which will trigger a
	 * segfault in case the memory had already been freed and/or
	 * trigger some error in case the memory is accessed after is
	 * has been freed.
	 * To avoid performance penalty in the "production version",
	 * we only do this in debugging/development mode (i.e., when
	 * configured with --enable-assert).
	 * Disable at command line using --debug=33554432
	 */
	DEADBEEFCHK memset(s, 0xDB, size - (MALLOC_EXTRA_SPACE + (size & 1)));	/* 0xDeadBeef */
#endif
	free(((char *) s) - MALLOC_EXTRA_SPACE);
	heapdec(size);
}

#undef GDKreallocmax
ptr
GDKreallocmax(void *blk, size_t size, size_t *maxsize, int emergency)
{
	void *oldblk = blk;
	ssize_t oldsize = 0;
	size_t newsize;

	if (blk == NULL) {
		return GDKmallocmax(size, maxsize, emergency);
	}
	if (size == 0) {
#ifdef GDK_MEM_NULLALLOWED
		GDKfree(blk);
		*maxsize = 0;
		return NULL;
#else
		GDKfatal("GDKreallocmax: called with size 0");
#endif
	}
	size = (size + 7) & ~7;	/* round up to a multiple of eight */
	oldsize = GDK_MEM_BLKSIZE(blk);

	/* check against duplicate free */
	assert((oldsize & 2) == 0);

	newsize = size + MALLOC_EXTRA_SPACE;

	blk = realloc(((char *) blk) - MALLOC_EXTRA_SPACE,
		      newsize + GLIBC_BUG);
	if (blk == NULL) {
		GDKmemfail("GDKrealloc", newsize);
		blk = realloc(((char *) oldblk) - MALLOC_EXTRA_SPACE,
			      newsize);
		if (blk == NULL) {
			if (emergency == 0) {
				GDKerror("GDKreallocmax: failed for "
					 SZFMT " bytes", newsize);
				return NULL;
			}
			GDKfatal("GDKreallocmax: failed for "
				 SZFMT " bytes", newsize);
		} else {
			fprintf(stderr, "#GDKremallocmax: "
				"recovery ok. Continuing..\n");
		}
	}
	/* place MALLOC_EXTRA_SPACE bytes before it */
	assert((((uintptr_t) blk) & 4) == 0);
	blk = ((char *) blk) + MALLOC_EXTRA_SPACE;
	((ssize_t *) blk)[-1] = (ssize_t) newsize;

	/* adapt statistics */
	heapinc(newsize);
	heapdec(oldsize);
	*maxsize = size;
	return blk;
}

#undef GDKrealloc
ptr
GDKrealloc(void *blk, size_t size)
{
	size_t sz = size;

	return GDKreallocmax(blk, sz, &size, 0);
}

#undef GDKstrdup
char *
GDKstrdup(const char *s)
{
	int l = strLen(s);
	char *n = GDKmalloc(l);

	if (n)
		memcpy(n, s, l);
	return n;
}


/*
 * @- virtual memory
 * allocations affect only the logical VM resources.
 */
#undef GDKmmap
void *
GDKmmap(const char *path, int mode, size_t len)
{
	void *ret = MT_mmap(path, mode, len);

	if (ret == NULL) {
		GDKmemfail("GDKmmap", len);
		ret = MT_mmap(path, mode, len);
		if (ret != NULL) {
			fprintf(stderr, "#GDKmmap: recovery ok. Continuing..\n");
		}
	}
	if (ret != NULL) {
		meminc(len);
	}
	return ret;
}

#undef GDKmunmap
gdk_return
GDKmunmap(void *addr, size_t size)
{
	int ret;

	ret = MT_munmap(addr, size);
	if (ret == 0)
		memdec(size);
	return ret == 0 ? GDK_SUCCEED : GDK_FAIL;
}
