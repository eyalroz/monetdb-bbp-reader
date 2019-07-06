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
 * @* Database Storage Management
 * Contains routines for writing and reading GDK data to and from
 * disk.  This section contains the primitives to manage the
 * disk-based images of the BATs. It relies on the existence of a UNIX
 * file system, including memory mapped files. Solaris and IRIX have
 * different implementations of madvise().
 *
 * The current version assumes that all BATs are stored on a single
 * disk partition. This simplistic assumption should be replaced in
 * the near future by a multi-volume version. The intention is to use
 * several BAT home locations.  The files should be owned by the
 * database server. Otherwise, IO operations are likely to fail. This
 * is accomplished by setting the GID and UID upon system start.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include <stdlib.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

/* Moved here from gdk_storage.h */
static void DESCclean(BAT *);


/* GDKfilepath returns a newly allocated string containing the path
 * name of a database farm.
 * The arguments are the farmID or -1, the name of a subdirectory
 * within the farm (i.e., something like BATDIR or BAKDIR -- see
 * gdk.h) or NULL, the name of a BAT (i.e. the name that is stored in
 * BBP.dir -- something like 07/714), and finally the file extension.
 *
 * If farmid is >= 0, GDKfilepath returns the complete path to the
 * specified farm concatenated with the other arguments with
 * appropriate separators.  If farmid is -1, it returns the
 * concatenation of its other arguments (in this case, the result
 * cannot be used to access a file directly -- the farm needs to be
 * prepended in some other place). */
char *
GDKfilepath(const char* farm_dir, const char *dir, const char *name, const char *ext)
{
	char sep[2];
	size_t pathlen;
	char *path;

	assert(dir == NULL || *dir != DIR_SEP);
	assert(farm_dir != NULL);
	if (MT_path_absolute(name)) {
		GDKerror("GDKfilepath: name should not be absolute\n");
		return NULL;
	}
	if (dir && *dir == DIR_SEP)
		dir++;
	if (dir == NULL || dir[0] == 0 || dir[strlen(dir) - 1] == DIR_SEP) {
		sep[0] = 0;
	} else {
		sep[0] = DIR_SEP;
		sep[1] = 0;
	}
	pathlen = (strlen(farm_dir) + 1) +
		(dir ? strlen(dir) : 0) + strlen(sep) + strlen(name) +
		(ext ? strlen(ext) + 1 : 0) + 1;
	path = GDKmalloc(pathlen);
	if (path == NULL)
		return NULL;
	snprintf(path, pathlen, "%s%c%s%s%s%s%s",
		farm_dir, DIR_SEP,
		 dir ? dir : "", sep, name,
		 ext ? "." : "", ext ? ext : "");
	return path;
}

#define _FUNBUF		0x040000
#define _FWRTHR		0x080000
#define _FRDSEQ		0x100000

/* open a file and return its file descriptor; the file is specified
 * using farmid, name and extension; if opening for writing, we create
 * the parent directory if necessary */
int
GDKfdlocate(const char* bbp_farm_dir, const char *nme, const char *mode, const char *extension)
{
	char *path;
	int fd, flags = 0;

	if (nme == NULL || *nme == 0)
		return -1;

	path = GDKfilepath(bbp_farm_dir, BATDIR, nme, extension);
	if (path == NULL)
		return -1;

	if (*mode == 'm') {	/* file open for mmap? */
		mode++;
#ifdef _CYGNUS_H_
	} else {
		flags = _FRDSEQ;	/* WIN32 CreateFile(FILE_FLAG_SEQUENTIAL_SCAN) */
#endif
	}

	if (strchr(mode, 'w')) {
		flags |= O_WRONLY | O_CREAT;
		GDKfatal("When only loading data, we shouldn't be trying to open files for writing");
		GDKfree(path);
		return -1;
	} else if (!strchr(mode, '+')) {
		flags |= O_RDONLY;
	} else {
		flags |= O_RDWR;
		GDKfatal("When only loading data, we shouldn't be trying to open files for writing");
		GDKfree(path);
		return -1;
	}
#ifdef WIN32
	flags |= strchr(mode, 'b') ? O_BINARY : O_TEXT;
#endif
	/*
	 * At this point we are (?) sure the file is only being opened
	 * in read-only mode.
	 */
	fd = open(path, flags, MONETDB_MODE);
	/* don't generate error if we can't open a file for reading */
	GDKfree(path);
	return fd;
}

/* like GDKfdlocate, except return a FILE pointer */
FILE *
GDKfilelocate(const char* bbp_farm_dir, const char *nme, const char *mode, const char *extension)
{
	int fd;
	FILE *f;

	if ((fd = GDKfdlocate(bbp_farm_dir, nme, mode, extension)) < 0)
		return NULL;
	if (*mode == 'm')
		mode++;
	if ((f = fdopen(fd, mode)) == NULL) {
		GDKsyserror("GDKfilelocate: cannot fdopen file\n");
		close(fd);
		return NULL;
	}
	return f;
}

/* We won't really extend the file - just ensure it's the right size */
gdk_return
GDKextendf(int fd, size_t size, const char *fn)
{
	struct stat stb;
	int rt = 0;

	if (fstat(fd, &stb) < 0) {
		/* shouldn't happen */
		GDKsyserror("GDKextendf: fstat unexpectedly failed\n");
		return GDK_FAIL;
	}
	/* even if necessary, do _not_ extend the underlying file */
	if (stb.st_size < (off_t) size) {
		GDKfatal(
			"It's necessary to extend a (heap?) file, probably "
			"for mmap'ing - but we only want to load data and not "
			"write anything. Please run MonetDB to fix this.");
		rt = -1;
	}
	/* posix_fallocate returns != 0 on failure, fallocate and
	 * ftruncate return -1 on failure, but all three return 0 on
	 * success */
	return rt != 0 ? GDK_FAIL : GDK_SUCCEED;
}

/* We won't really extend the file - just ensure it's the right size */
gdk_return
GDKextend(const char *fn, size_t size)
{
	int fd, flags = O_RDWR;
	gdk_return rt = GDK_FAIL;

#ifdef O_BINARY
	/* On Windows, open() fails if the file is bigger than 2^32
	 * bytes without O_BINARY. */
	flags |= O_BINARY;
#endif
	if ((fd = open(fn, flags)) >= 0) {
		rt = GDKextendf(fd, size, fn);
		close(fd);
	} else {
		GDKsyserror("GDKextend: cannot open file %s\n", fn);
	}
	return rt;
}

/*
 * Loads a single heap (or a vheap) for a single BAT, from a single file
 * in the DB directory. Typically, the name would be something like "12/1265"
 * and the extension ".tail", so you the file would be
 * "/path/to/db_farm/my_specific_db/bat/12/1265.tail".
 *
 * Space for the load is directly allocated and the heaps are mapped.
 * Further initialization of the atom heaps require a separate action
 * defined in their implementation.
 *
 * size -- how much to read
 * *maxsize -- (in/out) how much to allocate / how much was allocated
 */
char *
GDKload(const char* farm_dir, const char *nme, const char *ext, size_t size, size_t *maxsize, storage_t mode)
{
	char *ret = NULL;

	assert(size <= *maxsize);
	IODEBUG {
		fprintf(stderr, "#GDKload: name=%s, ext=%s, mode %d\n", nme, ext ? ext : "", (int) mode);
	}
	if (mode == STORE_MEM) {
		int fd = GDKfdlocate(farm_dir, nme, "rb", ext);

		if (fd >= 0) {
			char *dst = ret = GDKmalloc(*maxsize);
			ssize_t n_expected, n = 0;

			if (ret) {
				/* read in chunks, some OSs do not
				 * give you all at once and Windows
				 * only accepts int */
				for (n_expected = (ssize_t) size; n_expected > 0; n_expected -= n) {
					n = read(fd, dst, (unsigned) MIN(1 << 30, n_expected));
					if (n < 0)
						GDKsyserror("GDKload: cannot read: name=%s, ext=%s, " SZFMT " bytes missing.\n", nme, ext ? ext : "", (size_t) n_expected);
#ifndef STATIC_CODE_ANALYSIS
					/* Coverity doesn't seem to
					 * recognize that we're just
					 * printing the value of ptr,
					 * not its contents */
					IODEBUG fprintf(stderr, "#read(dst " PTRFMT ", n_expected " SSZFMT ", fd %d) = " SSZFMT "\n", PTRFMTCAST(void *)dst, n_expected, fd, n);
#endif

					if (n <= 0)
						break;
					dst += n;
				}
				if (n_expected > 0) {
					/* we couldn't read all, error
					 * already generated */
					GDKfree(ret);
					ret = NULL;
				}
#ifndef NDEBUG
				/* just to make valgrind happy, we
				 * initialize the whole thing */
				if (ret && *maxsize > size)
					memset(ret + size, 0, *maxsize - size);
#endif
			}
			close(fd);
		} else {
			GDKerror("GDKload: cannot open: name=%s, ext=%s\n", nme, ext ? ext : "");
		}
	} else {
		char *path;

		/* round up to multiple of GDK_mmap_pagesize with a
		 * minimum of one */
		size = (*maxsize + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
		if (size == 0)
			size = GDK_mmap_pagesize;
		path = GDKfilepath(farm_dir, BATDIR, nme, ext);
		if (path != NULL && GDKextend(path, size) == GDK_SUCCEED) {
			int mod = MMAP_READ | MMAP_WRITE | MMAP_SEQUENTIAL | MMAP_SYNC;

			if (mode == STORE_PRIV)
				mod |= MMAP_COPY;
			ret = GDKmmap(path, mod, size);
			if (ret != NULL) {
				/* success: update allocated size */
				*maxsize = size;
			}
			IODEBUG fprintf(stderr, "#mmap(NULL, 0, maxsize " SZFMT ", mod %d, path %s, 0) = " PTRFMT "\n", size, mod, path, PTRFMTCAST(void *)ret);
		}
		GDKfree(path);
	}
	return ret;
}

/*
 * @+ BAT disk storage
 *
 * Between sessions the BATs comprising the database are saved on
 * disk.  To simplify code, we assume a UNIX directory called its
 * physical @%home@ where they are to be located.  The subdirectories
 * BAT and PRG contain what its name says.
 *
 * A BAT created by @%BATnew@ is considered temporary until one calls
 * the routine @%BATsave@. This routine reserves disk space and checks
 * for name clashes.
 *
 * Saving and restoring BATs is left to the upper layers. The library
 * merely copies the data into place.  Failure to read or write the
 * BAT results in a NULL, otherwise it returns the BAT pointer.
 */
static BATstore *
DESCload(BBP* bbp, int i)
{
	str s, nme = BBP_physical(bbp, i);
	BATstore *bs;
	BAT *b = NULL;
	int ht, tt;

	IODEBUG {
		fprintf(stderr, "#DESCload %s\n", nme ? nme : "<noname>");
	}
	bs = BBP_desc(bbp, i);

	if (bs == NULL)
		return 0;
	b = &bs->B;

	ht = b->htype;
	tt = b->ttype;
	if ((ht < 0 && (ht = ATOMindex(s = ATOMunknown_name(ht))) < 0) ||
	    (tt < 0 && (tt = ATOMindex(s = ATOMunknown_name(tt))) < 0)) {
		GDKerror("DESCload: atom '%s' unknown, in BAT '%s'.\n", s, nme);
		return NULL;
	}
	b->htype = ht;
	b->ttype = tt;
	b->H->hash = b->T->hash = NULL;
	/* mil shouldn't mess with just loaded bats */
	if (b->batStamp > 0)
		b->batStamp = -b->batStamp;

	/* reconstruct mode from BBP status (BATmode doesn't flush
	 * descriptor, so loaded mode may be stale) */
	b->batPersistence = (BBP_status(bbp, b->batCacheid) & BBPPERSISTENT) ? PERSISTENT : TRANSIENT;
	b->batCopiedtodisk = 1;
	DESCclean(b);
	return bs;
}

static void
DESCclean(BAT *b)
{
	/* We're just loading data, BATs don't get dirtied by transactions or otherwise */
	b->batDirtyflushed = FALSE;
	b->batDirty = 0;
	b->batDirtydesc = 0;
	/* The heaps don't have a 'dirty' member */
}

BAT *
BATload_intern(BBP* bbp, bat i, int lock)
{
	bat bid = abs(i);
	str nme = BBP_physical(bbp, bid);
	BATstore *bs = DESCload(bbp, bid);
	BAT *b;

	if (bs == NULL) {
		return NULL;
	}
	b = &bs->B;

	/* LOAD bun heap */
	if (b->htype != TYPE_void) {
		if (HEAPload(&b->H->heap, nme, "head", b->batRestricted == BAT_READ) != GDK_SUCCEED) {
			return NULL;
		}
		assert(b->H->heap.size >> b->H->shift <= BUN_MAX);
		b->batCapacity = (BUN) (b->H->heap.size >> b->H->shift);
	} else {
		b->H->heap.base = NULL;
	}
	if (b->ttype != TYPE_void) {
		if (HEAPload(&b->T->heap, nme, "tail", b->batRestricted == BAT_READ) != GDK_SUCCEED) {
			HEAPfree(&b->H->heap, 0);
			return NULL;
		}
		if (b->htype == TYPE_void) {
			assert(b->T->heap.size >> b->T->shift <= BUN_MAX);
			b->batCapacity = (BUN) (b->T->heap.size >> b->T->shift);
		}
		if (b->batCapacity != (b->T->heap.size >> b->T->shift)) {
			GDKfatal("The actual tail (heap) size of a BAT differs from the size described in the BBP.dir file; this should not happen - run MonetDB to (maybe) fix it.");
		}
	} else {
		b->T->heap.base = NULL;
	}

	/* LOAD head heap */
	if (ATOMvarsized(b->htype)) {
		if (HEAPload(b->H->vheap, nme, "hheap", b->batRestricted == BAT_READ) != GDK_SUCCEED) {
			HEAPfree(&b->H->heap, 0);
			HEAPfree(&b->T->heap, 0);
			return NULL;
		}
		if (ATOMstorage(b->htype) == TYPE_str) {
			strCleanHash(b->H->vheap, FALSE);	/* ensure consistency */
		}
	}

	/* LOAD tail heap */
	if (ATOMvarsized(b->ttype)) {
		if (HEAPload(b->T->vheap, nme, "theap", b->batRestricted == BAT_READ) != GDK_SUCCEED) {
			if (b->H->vheap)
				HEAPfree(b->H->vheap, 0);
			HEAPfree(&b->H->heap, 0);
			HEAPfree(&b->T->heap, 0);
			return NULL;
		}
		if (ATOMstorage(b->ttype) == TYPE_str) {
			strCleanHash(b->T->vheap, FALSE);	/* ensure consistency */
		}
	}

	/* initialize descriptor */
	b->batDirtydesc = FALSE;
	b->H->heap.parentid = b->T->heap.parentid = 0;

	/* load succeeded; register it in BBP */
	BBPcacheit(bbp, bs, lock);

	/* We are only loading data, so BATs cannot be dirty */


	if ((b->batRestricted == BAT_WRITE && (GDKdebug & CHECKMASK)) ||
	    (GDKdebug & PROPMASK)) {
		++b->batSharecnt;
		--b->batSharecnt;
	}
	return (i < 0) ? BATmirror(bbp, b) : b;
}
