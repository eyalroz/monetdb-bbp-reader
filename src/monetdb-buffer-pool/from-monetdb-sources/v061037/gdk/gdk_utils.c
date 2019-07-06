/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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

#ifdef NATIVE_WIN32
#define chdir _chdir
#endif

#undef malloc
#undef calloc
#undef realloc
#undef free

/* memory thresholds; these values some "sane" constants only, really
 * set in GDKinit() */
#define MMAP_MINSIZE_PERSISTENT	((size_t) 1 << 18)
#if SIZEOF_SIZE_T == 4
#define MMAP_MINSIZE_TRANSIENT	((size_t) 1 << 20)
#else
#define MMAP_MINSIZE_TRANSIENT	((size_t) 1 << 32)
#endif
#define MMAP_PAGESIZE		((size_t) 1 << 16)
size_t GDK_mmap_minsize_persistent = MMAP_MINSIZE_PERSISTENT;
size_t GDK_mmap_minsize_transient = MMAP_MINSIZE_TRANSIENT;
size_t GDK_mmap_pagesize = MMAP_PAGESIZE; /* mmap granularity */
size_t GDK_mem_maxsize = GDK_VM_MAXSIZE;
size_t GDK_vm_maxsize = GDK_VM_MAXSIZE;

int GDK_vm_trim = 1;

#define SEG_SIZE(x,y)	((x)+(((x)&((1<<(y))-1))?(1<<(y))-((x)&((1<<(y))-1)):0))

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
	if (vsnprintf(message + len, sizeof(message) - (len + 2), format, ap) < 0)
		strcpy(message, GDKERROR "an error occurred within GDKerror.\n");
	va_end(ap);

	/* When only loading data, let's simplify error message printing */
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
	fputs(message, stderr);
	fputc('\n', stderr);
	fflush(stderr);

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
	/* TODO: Do we need the conditional BATSIGinit() here? */
/*
#ifndef NATIVE_WIN32
	BATSIGinit();
#endif
*/
	if (!strncmp(format, GDKFATAL, len)) {
		len = 0;
	} else {
		strcpy(message, GDKFATAL);
	}
	va_start(ap, format);
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
	va_end(ap);

 	/* When only using GDK for reading data - you can't jump anywhere, nor GDKexit, no GDKfatal */
	{
		fputs(message, stderr);
		fputs("\n", stderr);
		fflush(stderr);
		abort();
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

static void
GDKmemfail(const char *s, size_t len)
{
	fprintf(stderr, "#%s(" SZFMT ") fails, try to free up space [memory in use=" SZFMT ",virtual memory in use=" SZFMT "]\n", s, len, GDKmem_cursize(), GDKvm_cursize());
	GDKfatal("Can't free up memory in data-loading-only mode - we don't have BBPtrim available");
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

/*
 * We don't have a GDKsetmemorylimit(lng nbytes) - that's only in use
 * for debugging the GDK code.
 */

#ifdef NDEBUG
#define DEBUG_SPACE	0
#else
#define DEBUG_SPACE	16
#endif


static void *
GDKmalloc_internal(size_t size)
{
	void *s;
	size_t nsize;

	assert(size != 0);

	/* pad to multiple of eight bytes and add some extra space to
	 * write real size in front; when debugging, also allocate
	 * extra space for check bytes */
	nsize = (size + 7) & ~7;
	if ((s = malloc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE)) == NULL) {
		GDKmemfail("GDKmalloc", size);
		GDKerror("GDKmalloc_internal: failed for " SZFMT " bytes", size);
		return NULL;
	}
	s = (void *) ((char *) s + MALLOC_EXTRA_SPACE);

	heapinc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE);

	/* just before the pointer that we return, write how much we
	 * asked of malloc */
	((size_t *) s)[-1] = nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE;
#ifndef NDEBUG
	/* just before that, write how much was asked of us */
	((size_t *) s)[-2] = size;
	/* write pattern to help find out-of-bounds writes */
	memset((char *) s + size, '\xBD', nsize + DEBUG_SPACE - size);
#endif
	return s;
}

#undef GDKmalloc
void *
GDKmalloc(size_t size)
{
	void *s;

	if ((s = GDKmalloc_internal(size)) == NULL)
		return NULL;
#ifndef NDEBUG
	/* write a pattern to help make sure all data is properly
	 * initialized by the caller */
	DEADBEEFCHK memset(s, '\xBD', size);
#endif
	return s;
}

#undef GDKzalloc
void *
GDKzalloc(size_t size)
{
	void *s;

	if ((s = GDKmalloc_internal(size)) == NULL)
		return NULL;
	memset(s, 0, size);
	return s;
}

#undef GDKstrdup
char *
GDKstrdup(const char *s)
{
	size_t size;
	char *p;

	if (s == NULL)
		return NULL;
	size = strlen(s) + 1;

	if ((p = GDKmalloc_internal(size)) == NULL)
		return NULL;
	memcpy(p, s, size);	/* including terminating NULL byte */
	return p;
}

#undef GDKstrndup
char *
GDKstrndup(const char *s, size_t size)
{
	char *p;

	if (s == NULL || size == 0)
		return NULL;
	if ((p = GDKmalloc_internal(size + 1)) == NULL)
		return NULL;
	memcpy(p, s, size);
	p[size] = '\0';		/* make sure it's NULL terminated */
	return p;
}

#undef GDKfree
void
GDKfree(void *s)
{
	size_t asize;

	if (s == NULL)
		return;

	asize = ((size_t *) s)[-1]; /* how much allocated last */

#ifndef NDEBUG
	assert((asize & 2) == 0);   /* check against duplicate free */
	/* check for out-of-bounds writes */
	{
		size_t i = ((size_t *) s)[-2]; /* how much asked for last */
		for (; i < asize - MALLOC_EXTRA_SPACE; i++)
			assert(((char *) s)[i] == '\xBD');
	}
	((size_t *) s)[-1] |= 2; /* indicate area is freed */
#endif

#ifndef NDEBUG
	/* overwrite memory that is to be freed with a pattern that
	 * will help us recognize access to already freed memory in
	 * the debugger */
	DEADBEEFCHK memset(s, '\xDB', asize - MALLOC_EXTRA_SPACE);
#endif

	free((char *) s - MALLOC_EXTRA_SPACE);
	heapdec((ssize_t) asize);
}

#undef GDKrealloc
void *
GDKrealloc(void *s, size_t size)
{
	size_t nsize, asize;
#ifndef NDEBUG
	size_t osize;
	size_t *os;
#endif

	assert(size != 0);

	if (s == NULL)
		return GDKmalloc(size);

	nsize = (size + 7) & ~7;
	asize = ((size_t *) s)[-1]; /* how much allocated last */

#ifndef NDEBUG
	assert((asize & 2) == 0);   /* check against duplicate free */
	/* check for out-of-bounds writes */
	osize = ((size_t *) s)[-2]; /* how much asked for last */
	{
		size_t i;
		for (i = osize; i < asize - MALLOC_EXTRA_SPACE; i++)
			assert(((char *) s)[i] == '\xBD');
	}
	/* if shrinking, write debug pattern into to-be-freed memory */
	DEADBEEFCHK if (size < osize)
		memset((char *) s + size, '\xDB', osize - size);
	os = s;
	os[-1] |= 2;		/* indicate area is freed */
#endif
	s = realloc((char *) s - MALLOC_EXTRA_SPACE,
		    nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE);
	if (s == NULL) {
#ifndef NDEBUG
		os[-1] &= ~2;	/* not freed after all */
#endif
		GDKmemfail("GDKrealloc", size);
		GDKerror("GDKrealloc: failed for " SZFMT " bytes", size);
		return NULL;
	}
	s = (void *) ((char *) s + MALLOC_EXTRA_SPACE);
	/* just before the pointer that we return, write how much we
	 * asked of malloc */
	((size_t *) s)[-1] = nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE;
#ifndef NDEBUG
	/* just before that, write how much was asked of us */
	((size_t *) s)[-2] = size;
	/* if growing, initialize new memory with debug pattern */
	DEADBEEFCHK if (size > osize)
 		memset((char *) s + osize, '\xBD', size - osize);
	/* write pattern to help find out-of-bounds writes */
	memset((char *) s + size, '\xBD', nsize + DEBUG_SPACE - size);
#endif

	heapinc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE);
	heapdec((ssize_t) asize);

	return s;
}

void
GDKsetmallocsuccesscount(lng count)
{
	(void) count;
}

/*
 * @- virtual memory
 * allocations affect only the logical VM resources.
 */
#undef GDKmmap
void *
GDKmmap(const char *path, int mode, size_t len)
{
	void *ret;

	if (GDKvm_cursize() + len >= GDK_vm_maxsize) {
		GDKerror("allocating too much virtual address space\n");
		return NULL;
	}
	ret = MT_mmap(path, mode, len);
	if (ret == NULL) {
		GDKmemfail("GDKmmap", len);
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

