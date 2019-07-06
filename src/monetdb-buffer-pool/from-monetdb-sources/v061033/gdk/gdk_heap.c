/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @f gdk_heap
 * @a Peter Boncz, Wilko Quak
 * @+ Atom Heaps
 * Heaps are the basic mass storage structure of Monet. A heap is a
 * handle to a large, possibly huge, contiguous area of main memory,
 * that can be allocated in various ways (discriminated by the
 * heap->storage field):
 *
 * @table @code
 * @item STORE_MEM: malloc-ed memory
 * small (or rather: not huge) heaps are allocated with GDKmalloc.
 * Notice that GDKmalloc may redirect big requests to anonymous
 * virtual memory to prevent @emph{memory fragmentation} in the malloc
 * library (see gdk_utils.mx).
 *
 * @item STORE_MMAP: read-only mapped region
 * this is a file on disk that is mapped into virtual memory.  This is
 * normally done MAP_SHARED, so we can use msync() to commit dirty
 * data using the OS virtual memory management.
 *
 * @item STORE_PRIV: read-write mapped region
 * in order to preserve ACID properties, we use a different memory
 * mapping on virtual memory that is writable. This is because in case
 * of a crash on a dirty STORE_MMAP heap, the OS may have written some
 * of the dirty pages to disk and other not (but it is impossible to
 * determine which).  The OS MAP_PRIVATE mode does not modify the file
 * on which is being mapped, rather creates substitute pages
 * dynamically taken from the swap file when modifications occur. This
 * is the only way to make writing to mmap()-ed regions safe.  To save
 * changes, we created a new file X.new; as some OS-es do not allow to
 * write into a file that has a mmap open on it (e.g. Windows).  Such
 * X.new files take preference over X files when opening them.
 * @end table
 * Read also the discussion in BATsetaccess (gdk_bat.mx).
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

static gdk_return HEAPload_intern(Heap *h, const char *nme, const char *ext, const char *suffix, int trunc);

/* Free the memory associated with the heap H.
 * Unlinks (removes) the associated file if the remove flag is set. */
void
HEAPfree(Heap *h, int remove)
{
	if (h->base) {
		if (h->storage == STORE_MEM) {	/* plain memory */
			HEAPDEBUG fprintf(stderr, "#HEAPfree " SZFMT
					  " " PTRFMT "\n",
					  h->size, PTRFMTCAST h->base);
			GDKfree(h->base);
		} else {	/* mapped file, or STORE_PRIV */
			gdk_return ret = GDKmunmap(h->base, h->size);

			if (ret != GDK_SUCCEED) {
				GDKsyserror("HEAPfree: %s was not mapped\n",
					    h->filename);
				assert(0);
			}
			HEAPDEBUG fprintf(stderr, "#munmap(base=" PTRFMT ", "
					  "size=" SZFMT ") = %d\n",
					  PTRFMTCAST(void *)h->base,
					  h->size, (int) ret);
		}
	}
	h->base = NULL;
	if (h->filename) {
		if (remove) {
			char *path = GDKfilepath(h->farm_dir, BATDIR, h->filename, NULL);
			if (path && unlink(path) < 0 && errno != ENOENT)
				perror(path);
			GDKfree(path);
			path = GDKfilepath(h->farm_dir, BATDIR, h->filename, "new");
			if (path && unlink(path) < 0 && errno != ENOENT)
				perror(path);
			GDKfree(path);
		}
		GDKfree(h->filename);
		h->filename = NULL;
	}
}

/*
 * @- HEAPload
 *
 * If we find file X.new, we move it over X (if present) and open it.
 *
 * This routine initializes the h->filename without deallocating its
 * previous contents.
 */
static gdk_return
HEAPload_intern(Heap *h, const char *nme, const char *ext, const char *suffix, int trunc)
{
	size_t minsize;
	int ret = 0;
	char *srcpath, *dstpath;

	h->storage = h->newstorage = h->size < GDK_mmap_minsize ? STORE_MEM : STORE_MMAP;
	if (h->filename == NULL)
		h->filename = (char *) GDKmalloc(strlen(nme) + strlen(ext) + 2);
	if (h->filename == NULL)
		return GDK_FAIL;
	sprintf(h->filename, "%s.%s", nme, ext);

	minsize = (h->size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
	if (h->storage != STORE_MEM && minsize != h->size)
		h->size = minsize;

	/* when a bat is made read-only, we can truncate any unused
	 * space at the end of the heap */

	/* ... but we won't since we don't want to try to write
	 * anything when only loading data */
	if (trunc) {
		/* round up mmap heap sizes to GDK_mmap_pagesize
		 * segments, also add some slack */
		size_t truncsize = ((size_t) (h->free * 1.05) + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
		int fd;

		if (truncsize == 0)
			truncsize = GDK_mmap_pagesize; /* minimum of one page */
		(void) ret;
		(void) fd;
		HEAPDEBUG fprintf(stderr,
				  "# NOT calling ftruncate(file=%s.%s, size=" SZFMT
				  ")\n", nme, ext, truncsize);
	}

	HEAPDEBUG fprintf(stderr, "#HEAPload(%s.%s,storage=%d,free=" SZFMT
			  ",size=" SZFMT ")\n", nme, ext,
			  (int) h->storage, h->free, h->size);

	/* On some OSs (WIN32,Solaris), it is prohibited to write to a
	 * file that is open in MAP_PRIVATE (FILE_MAP_COPY) solution:
	 * we write to a file named .ext.new.  This file, if present,
	 * takes precedence. */
	srcpath = GDKfilepath(h->farm_dir, BATDIR, nme, ext);
	dstpath = GDKfilepath(h->farm_dir, BATDIR, nme, ext);
	srcpath = GDKrealloc(srcpath, strlen(srcpath) + strlen(suffix) + 1);
	strcat(srcpath, suffix);

	struct stat st;
	int result = stat(srcpath, &st);
	if (result == 0) {	
		GDKfatal("Can't fine file %s for a move",srcpath);
	}
	GDKfree(srcpath);
	GDKfree(dstpath);

	h->base = GDKload(h->farm_dir, nme, ext, h->free, &h->size, h->newstorage);
	if (h->base == NULL)
		return GDK_FAIL; /* file could  not be read satisfactorily */

	return GDK_SUCCEED;
}

gdk_return
HEAPload(Heap *h, const char *nme, const char *ext, int trunc)
{
	return HEAPload_intern(h, nme, ext, ".new", trunc);
}

/*
 * @+ Standard Heap Library
 * This library contains some routines which implement a @emph{
 * malloc} and @emph{ free} function on the Monet @emph{Heap}
 * structure. They are useful when implementing a new @emph{
 * variable-size} atomic data type, or for implementing new search
 * accelerators.  All functions start with the prefix @emph{HEAP_}. T
 *
 * Due to non-careful design, the HEADER field was found to be
 * 32/64-bit dependent.  As we do not (yet) want to change the BAT
 * image on disk, This is now fixed by switching on-the-fly between
 * two representations. We ensure that the 64-bit memory
 * representation is just as long as the 32-bits version (20 bytes) so
 * the rest of the heap never needs to shift. The function
 * HEAP_checkformat converts at load time dynamically between the
 * layout found on disk and the memory format.  Recognition of the
 * header mode is done by looking at the first two ints: alignment
 * must be 4 or 8, and head can never be 4 or eight.
 *
 * TODO: user HEADER64 for both 32 and 64 bits (requires BAT format
 * change)
 */
/* #define DEBUG */
/* #define TRACE */

#define HEAPVERSION	20030408

typedef struct heapheader {
	size_t head;		/* index to first free block */
	int alignment;		/* alignment of objects on heap */
	size_t firstblock;	/* first block in heap */
	int version;
	int (*sizefcn)(const void *);	/* ADT function to ask length */
} HEADER32;

typedef struct {
	int version;
	int alignment;
	size_t head;
	size_t firstblock;
	int (*sizefcn)(const void *);
} HEADER64;

#if SIZEOF_SIZE_T==8
typedef HEADER64 HEADER;
typedef HEADER32 HEADER_OTHER;
#else
typedef HEADER32 HEADER;
typedef HEADER64 HEADER_OTHER;
#endif
typedef struct hfblock {
	size_t size;		/* Size of this block in freelist */
	size_t next;		/* index of next block */
} CHUNK;

#define roundup_8(x)	(((x)+7)&~7)
#define roundup_4(x)	(((x)+3)&~3)
#define blocksize(h,p)	((p)->size)

#define HEAP_index(HEAP,INDEX,TYPE)	((TYPE *)((char *) (HEAP)->base + (INDEX)))

#ifdef TRACE
static void
HEAP_printstatus(Heap *heap)
{
	HEADER *hheader = HEAP_index(heap, 0, HEADER);
	size_t block, cur_free = hheader->head;
	CHUNK *blockp;

	fprintf(stderr,
		"#HEAP has head " SZFMT " and alignment %d and size " SZFMT "\n",
		hheader->head, hheader->alignment, heap->free);

	/* Walk the blocklist */
	block = hheader->firstblock;

	while (block < heap->free) {
		blockp = HEAP_index(heap, block, CHUNK);

		if (block == cur_free) {
			fprintf(stderr,
				"#   free block at " PTRFMT " has size " SZFMT " and next " SZFMT "\n",
				PTRFMTCAST(void *)block,
				blockp->size, blockp->next);

			cur_free = blockp->next;
			block += blockp->size;
		} else {
			size_t size = blocksize(hheader, blockp);

			fprintf(stderr,
				"#   block at " SZFMT " with size " SZFMT "\n",
				block, size);
			block += size;
		}
	}
}
#endif /* TRACE */
