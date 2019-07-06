/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @a Niels Nes, Peter Boncz
 * @* System Independent Layer
 *
 * GDK is built on Posix. Exceptions are made for memory mapped files
 * and anonymous virtual memory, for which somewhat higher-level
 * functions are defined here.  Most of this file concerns itself with
 * emulation of Posix functionality on the WIN32 native platform.
 */
#include "monetdb_config.h"
#include "gdk.h"		/* includes gdk_posix.h */
#include "gdk_private.h"
#include <stdio.h>
#include <unistd.h>
#include <string.h>     /* strncpy */

#ifdef HAVE_FCNTL_H
# include <fcntl.h>
#endif
#ifdef HAVE_PROCFS_H
# include <procfs.h>
#endif
#ifdef HAVE_MACH_TASK_H
# include <mach/task.h>
#endif
#ifdef HAVE_MACH_MACH_INIT_H
# include <mach/mach_init.h>
#endif
#if defined(HAVE_KVM_H) && defined(HAVE_SYS_SYSCTL_H)
# include <kvm.h>
# include <sys/param.h>
# include <sys/sysctl.h>
# include <sys/user.h>
#endif

#ifdef NDEBUG
#ifndef NVALGRIND
#define NVALGRIND NDEBUG
#endif
#endif

#if defined(__GNUC__) && defined(HAVE_VALGRIND)
#include <valgrind.h>
#else
#define VALGRIND_MALLOCLIKE_BLOCK(addr, sizeB, rzB, is_zeroed)
#define VALGRIND_FREELIKE_BLOCK(addr, rzB)
#define VALGRIND_RESIZEINPLACE_BLOCK(addr, oldSizeB, newSizeB, rzB)
#endif

#ifndef MAP_NORESERVE
# define MAP_NORESERVE		MAP_PRIVATE
#endif
#if defined(MAP_ANON) && !defined(MAP_ANONYMOUS)
#define MAP_ANONYMOUS		MAP_ANON
#endif

#define MMAP_ADVISE		7
#define MMAP_WRITABLE		(MMAP_WRITE|MMAP_COPY)

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif

/* DDALERT: AIX4.X 64bits needs HAVE_SETENV==0 due to a AIX bug, but
 * it probably isn't detected so by configure */

#ifndef HAVE_SETENV
int
setenv(const char *name, const char *value, int overwrite)
{
	int ret = 0;

	if (overwrite || getenv(name) == NULL) {
		char *p = (char *) GDKmalloc(2 + strlen(name) + strlen(value));

		if (p == NULL)
			return -1;
		strcpy(p, name);
		strcat(p, "=");
		strcat(p, value);
		ret = putenv(p);
		/* GDKfree(p); LEAK INSERTED DUE TO SOME WEIRD CRASHES */
	}
	return ret;
}
#endif

/* Crude VM buffer management that keep a list of all memory mapped
 * regions.
 *
 * a.k.a. "helping stupid VM implementations that ignore VM advice"
 *
 * The main goal is to be able to tell the OS to please stop buffering
 * all memory mapped pages when under pressure. A major problem is
 * materialization of large results in newly created memory mapped
 * files. Operating systems tend to cache all dirty pages, such that
 * when memory is out, all pages are dirty and cannot be unloaded
 * quickly. The VM panic occurs and comatose OS states may be
 * observed.  This is in spite of our use of
 * madvise(MADV_SEQUENTIAL). That is; we would want that the OS drops
 * pages after we've passed them. That does not happen; pages are
 * retained and pollute the buffer cache.
 *
 * Regrettably, at this level, we don't know anything about how Monet
 * is using the mmapped regions. Monet code is totally oblivious of
 * any I/O; that's why it is so easy to create CPU efficient code in
 * Monet.
 *
 * The current solution focuses on large writable maps. These often
 * represent newly created BATs, that are the result of some (running)
 * operator. We assume two things here:
 * - the BAT is created in sequential fashion (always almost true)
 * - afterwards, this BAT is used in sequential fashion (often true)
 *
 * A VMtrim thread keeps an eye on the RSS (memory pressure) and large
 * writable memory maps. If RSS approaches mem_maxsize(), it starts to
 * *worry*, and starts to write dirty data from these writable maps to
 * disk in 128MB tiles. So, if memory pressure rises further in the
 * near future, the OS has some option to release memory pages cheaply
 * (i.e. without needing I/O). This is also done explicitly by the
 * VM-thread: when RSS exceeds mem_maxsize() is explicitly asks the OS
 * to release pages.  The reason is that Linux is not smart enough to
 * do even this. Anyway..
 *
 * The way to free pages explicitly in Linux is to call
 * posix_fadvise(..,MADV_DONTNEED).  Particularly,
 * posix_madvise(..,POSIX_MADV_DONTNEED) which is supported and
 * documented doesn't work on Linux. But we do both posix_madvise and
 * posix_fadvise, so on other unix systems that don't support
 * posix_fadvise, posix_madvise still might work.  On Windows, to our
 * knowledge, there is no way to tell it stop buffering a memory
 * mapped region. msync (FlushViewOfFile) does work, though. So let's
 * hope the VM paging algorithm behaves better than Linux which just
 * runs off the cliff and if MonetDB does not prevent RSS from being
 * too high, enters coma.
 *
 * We will only be able to sensibly test this on Windows64. On
 * Windows32, mmap sizes do not significantly exceed RAM sizes so
 * MonetDB swapping actually will not happen (of course, you've got
 * this nasty problem of VM fragemntation and failing mmaps instead).
 *
 * In principle, page tiles are saved sequentially, and behind it, but
 * never overtaking it, is an "unload-cursor" that frees the pages if
 * that is needed to keep RSS down.  There is a tweak in the
 * algorithm, that re-sets the unload-cursor if it seems that all
 * tiles to the end have been saved (whether a tile is actually saved
 * is determined by timing the sync action). This means that the
 * producing operator is ready creating the BAT, and we assume it is
 * going to be used sequentially afterwards.  In that case, we should
 * start unloading right after the 'read-cursor', that is, from the
 * start.
 *
 * EXAMPLE
 * D = dirty tile
 * s = saved tile (i.e. clean)
 * u = unloaded tile
 * L = tile that is being loaded
 *
 *           +--> operator produces  BAT
 * (1) DDDDDD|......................................| end of reserved mmap
 *                      ____|RSS
 *                     |
 *                     | at 3/4 of RSS consumed we start to worry
 *                     +--> operator produces BAT
 * (2) DDDDDDDDDDDDDDDD|............................|
 *                    s<----------------------------- VM backwards save thread
 *                    |
 *                    + first tile of which saving costs anything
 *
 *                        +--> operator produces BAT
 * (3) DDDDDDDDDDDDDDDss|D|.........................|
 *     VM-thread save ->|
 *
 * When the RSS target is exceeded, we start unloading tiles..
 *
 *                     +-->  VM-thread unload starts at *second* 's'
 *                     |
 *                     |    +--> operator produces BAT
 * (4) DDDDDDDDDDDDDDDsus|DD|........................|
 *     VM-thread save -->|  | RSS = Full!
 *
 *                                  +-- 0 => save costs nothing!!
 *     VM-thread save ------------->|        assume bat complete
 * (5) DDDDDDDDDDDDDDDsuuuuuuuuussss0................|
 *                    |<-------- re-set unload cursor
 *                    +--- first tile was not unloaded.
 *
 * later.. some other operator sequentially reads the bat
 * first part is 'D', that is, nicely cached.
 *
 *     ---read------->|
 * (6) DDDDDDDDDDDDDDDsuuuuuuuuussss0................|
 *
 * now we're hitting the unloaded region. the query becomes
 * I/O read bound here (typically 20% CPU utilization).
 *
 *     ---read-------->|
 * (7) DDDDDDDDDDDDDDDuLuuuuuuuussss0................|
 *                   /  \
 *      unload cursor    load cursor
 *
 *     ---read---------------->|
 * (8) DDDDDDDDDDDDDDDuuuuuuuuuLssss0................|
 *                           /  \
 *              unload cursor    load cursor
 *
 *     ---read--------------------->| done
 * (9) DDDDDDDDDDDDDDDuuuuuuuuuLssss0................|
 *                              ****
 *                              last part still cached
 *
 * note: if we would not have re-setted the unload cursor (5)
 *       the last part would have been lost due to continuing
 *       RSS pressure from the 'L' read-cursor.
 *
 * If multiple write-mmaps exist, we do unload-tile and save-tile
 * selection on a round-robin basis among them.
 *
 * Of course, this is a simple solution for simple cases only.
 * (a) if the bat is produced too fast, (or your disk is too slow)
 *     RSS will exceeds its limit and Linux will go into swapping.
 * (b) if your data is not produced and read sequentially.
 *     Examples are sorting or clustering on huge datasets.
 * (c) if RSS pressure is due to large read-maps, rather than
 *     intermediate results.
 *
 * Two crude suggestions:
 * - If we are under RSS pressure without unloadable tiles and with
 *   savable tiles, we should consider suspending *all* other threads
 *   until we manage to unload a tile.
 * - if there are no savable tiles (or in case of read-only maps)
 *   we could resort to saving and unloading random tiles.
 *
 * To do better, our BAT algorithms should provide even more detailed
 * advice on their access patterns, which may even consist of pointers
 * to the cursors (i.e. pointers to b->batBuns->free or the cursors
 * in radix-cluster), which an enhanced version of this thread might
 * take into account.
 *
 * [Kersten] The memory map table should be aligned to the number of
 * mapped files. In more recent applications, such as the SkyServer
 * this may be around 2000 BATs easily.
 */

#ifdef HAVE_PTHREAD_H
/* pthread.h on Windows includes config.h if HAVE_CONFIG_H is set */
#undef HAVE_CONFIG_H
#include <sched.h>
#include <pthread.h>
#endif
#ifdef HAVE_SEMAPHORE_H
#include <semaphore.h>
#endif

#ifndef NATIVE_WIN32
#ifdef HAVE_POSIX_FADVISE
#ifdef HAVE_UNAME
#include <sys/utsname.h>
#endif
#endif

void *
MT_mmap(const char *path, int mode, size_t len)
{
	int fd;
	void *ret;

	fd = open(path, O_CREAT | ((mode & MMAP_WRITE) ? O_RDWR : O_RDONLY) | O_CLOEXEC, MONETDB_MODE);
	if (fd < 0) {
		GDKsyserror("MT_mmap: open %s failed\n", path);
		return MAP_FAILED;
	}
	ret = mmap(NULL,
		   len,
		   ((mode & MMAP_WRITABLE) ? PROT_WRITE : 0) | PROT_READ,
		   (mode & MMAP_COPY) ? (MAP_PRIVATE | MAP_NORESERVE) : MAP_SHARED,
		   fd,
		   0);
	if (ret == MAP_FAILED) {
		GDKsyserror("MT_mmap: mmap(%s,"SZFMT") failed\n", path, len);
		ret = NULL;
	}
	close(fd);
	VALGRIND_MALLOCLIKE_BLOCK(ret, len, 0, 1);
	return ret;
}

int
MT_munmap(void *p, size_t len)
{
	int ret = munmap(p, len);

	if (ret < 0)
		GDKsyserror("MT_munmap: munmap(" PTRFMT "," SZFMT ") failed\n",
			    PTRFMTCAST p, len);
	VALGRIND_FREELIKE_BLOCK(p, 0);
#ifdef MMAP_DEBUG
	fprintf(stderr, "#munmap(" PTRFMT "," SZFMT ") = %d\n", PTRFMTCAST p, len, ret);
#endif
	return ret;
}


int
MT_path_absolute(const char *pathname)
{
	return (*pathname == DIR_SEP);
}

#ifdef HAVE_DLFCN_H
# include <dlfcn.h>
#endif

#else /* WIN32 native */

#ifndef BUFSIZ
#define BUFSIZ 1024
#endif

#undef _errno
#undef stat
#undef rmdir
#undef mkdir

#include <windows.h>

#ifdef _MSC_VER
#include <io.h>
#endif /* _MSC_VER */
#include <Psapi.h>

#define MT_SMALLBLOCK 256

/* Windows mmap keeps a global list of base addresses for complex
 * (remapped) memory maps the reason is that each remapped segment
 * needs to be unmapped separately in the end. */

void *
MT_mmap(const char *path, int mode, size_t len)
{
	DWORD mode0 = FILE_READ_ATTRIBUTES | FILE_READ_DATA;
	DWORD mode1 = FILE_SHARE_READ | FILE_SHARE_WRITE;
	DWORD mode2 = mode & MMAP_ADVISE;
	DWORD mode3 = PAGE_READONLY;
	int mode4 = FILE_MAP_READ;
	SECURITY_ATTRIBUTES sa;
	HANDLE h1, h2;
	void *ret;

	if (mode & MMAP_WRITE) {
		mode0 |= FILE_APPEND_DATA | FILE_WRITE_ATTRIBUTES | FILE_WRITE_DATA;
	}
	if (mode2 == MMAP_RANDOM || mode2 == MMAP_DONTNEED) {
		mode2 = FILE_FLAG_RANDOM_ACCESS;
	} else if (mode2 == MMAP_SEQUENTIAL || mode2 == MMAP_WILLNEED) {
		mode2 = FILE_FLAG_SEQUENTIAL_SCAN;
	} else {
		mode2 = FILE_FLAG_NO_BUFFERING;
	}
	if (mode & MMAP_SYNC) {
		mode2 |= FILE_FLAG_WRITE_THROUGH;
	}
	if (mode & MMAP_COPY) {
		mode3 = PAGE_WRITECOPY;
		mode4 = FILE_MAP_COPY;
	} else if (mode & MMAP_WRITE) {
		mode3 = PAGE_READWRITE;
		mode4 = FILE_MAP_WRITE;
	}
	sa.nLength = sizeof(SECURITY_ATTRIBUTES);
	sa.bInheritHandle = TRUE;
	sa.lpSecurityDescriptor = 0;

	h1 = CreateFile(path, mode0, mode1, &sa, OPEN_ALWAYS, mode2, NULL);
	if (h1 == INVALID_HANDLE_VALUE) {
		(void) SetFileAttributes(path, FILE_ATTRIBUTE_NORMAL);
		h1 = CreateFile(path, mode0, mode1, &sa, OPEN_ALWAYS, mode2, NULL);
		if (h1 == INVALID_HANDLE_VALUE) {
			errno = winerror(GetLastError());
			GDKsyserror("MT_mmap: CreateFile('%s', %lu, %lu, &sa, %lu, %lu, NULL) failed\n",
				    path, mode0, mode1, (DWORD) OPEN_ALWAYS, mode2);
			return NULL;
		}
	}

	h2 = CreateFileMapping(h1, &sa, mode3, (DWORD) (((__int64) len >> 32) & LL_CONSTANT(0xFFFFFFFF)), (DWORD) (len & LL_CONSTANT(0xFFFFFFFF)), NULL);
	if (h2 == NULL) {
		errno = winerror(GetLastError());
		GDKsyserror("MT_mmap: CreateFileMapping(" PTRFMT ", &sa, %lu, %lu, %lu, NULL) failed\n",
			    PTRFMTCAST h1, mode3,
			    (DWORD) (((__int64) len >> 32) & LL_CONSTANT(0xFFFFFFFF)),
			    (DWORD) (len & LL_CONSTANT(0xFFFFFFFF)));
		CloseHandle(h1);
		return NULL;
	}
	CloseHandle(h1);

	ret = MapViewOfFileEx(h2, mode4, (DWORD) 0, (DWORD) 0, len, NULL);
	if (ret == NULL)
		errno = winerror(GetLastError());
	CloseHandle(h2);

	return ret;
}

int
MT_munmap(void *p, size_t dummy)
{
	int ret;

	(void) dummy;
	/*       Windows' UnmapViewOfFile returns success!=0, error== 0,
	 * while Unix's   munmap          returns success==0, error==-1. */
	ret = UnmapViewOfFile(p);
	if (ret == 0) {
		errno = winerror(GetLastError());
		GDKsyserror("MT_munmap failed\n");
		return -1;
	}
	return 0;
}

void *
MT_mremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
{
	void *p;

	/* doesn't make sense for us to extend read-only memory map */
	assert(mode & MMAP_WRITABLE);

	/* round up to multiple of page size */
	*new_size = (*new_size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);

	if (old_size >= *new_size) {
		*new_size = old_size;
		return old_address;	/* don't bother shrinking */
	}
	if (GDKextend(path, *new_size) != GDK_SUCCEED) {
		fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): GDKextend() failed\n", __FILE__, __LINE__, path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size);
		return NULL;
	}
	if (path && !(mode & MMAP_COPY))
		MT_munmap(old_address, old_size);
	p = MT_mmap(path, mode, *new_size);
	if (p != NULL && (path == NULL || (mode & MMAP_COPY))) {
		memcpy(p, old_address, old_size);
		MT_munmap(old_address, old_size);
	}
#ifdef MMAP_DEBUG
	fprintf(stderr, "MT_mremap(%s,"PTRFMT","SZFMT","SZFMT") -> "PTRFMT"\n", path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size, PTRFMTCAST p);
#endif
	if (p == NULL)
		fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): p == NULL\n", __FILE__, __LINE__, path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size);
	return p;
}

int
MT_path_absolute(const char *pathname)
{
	/* drive letter, colon, directory separator */
	return (((('a' <= pathname[0] && pathname[0] <= 'z') ||
		  ('A' <= pathname[0] && pathname[0] <= 'Z')) &&
		 pathname[1] == ':' &&
		 (pathname[2] == '/' || pathname[2] == '\\')) ||
		(pathname[0] == '\\' && pathname[1] == '\\'));
}


#undef _stat64
int
win_stat(const char *pathname, struct _stat64 *st)
{
	char buf[128], *p = reduce_dir_name(pathname, buf, sizeof(buf));
	int ret;

	if (p == NULL)
		return -1;
	ret = _stat64(p, st);
	if (p != buf)
		free(p);
	return ret;
}

int
win_rmdir(const char *pathname)
{
	char buf[128], *p = reduce_dir_name(pathname, buf, sizeof(buf));
	int ret;

	if (p == NULL)
		return -1;
	ret = _rmdir(p);
	if (ret < 0 && errno != ENOENT) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		IODEBUG fprintf(stderr, "retry rmdir %s\n", pathname);
		MT_sleep_ms(100);	/* wait a little */
		ret = _rmdir(p);
	}
	if (p != buf)
		free(p);
	return ret;
}

int
win_unlink(const char *pathname)
{
	int ret = _unlink(pathname);
	if (ret < 0) {
		/* Vista is paranoid: we cannot delete read-only files
		 * owned by ourselves. Vista somehow also sets these
		 * files to read-only.
		 */
		(void) SetFileAttributes(pathname, FILE_ATTRIBUTE_NORMAL);
		ret = _unlink(pathname);
	}
	if (ret < 0 && errno != ENOENT) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		IODEBUG fprintf(stderr, "retry unlink %s\n", pathname);
		MT_sleep_ms(100);	/* wait a little */
		ret = _unlink(pathname);
	}
	return ret;
}

#endif
