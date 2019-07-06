/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a M. L. Kersten, P. Boncz, N. J. Nes
 * @* BAT Buffer Pool (BBP)
 * The BATs created and loaded are collected in a BAT buffer pool.
 * The Bat Buffer Pool has a number of functions:
 * @table @code
 *
 * @item administration and lookup
 * The BBP is a directory which contains status information about all
 * known BATs.  This interface may be used very heavily, by
 * data-intensive applications.  To eliminate all overhead, read-only
 * access to the BBP may be done by table-lookups. The integer index
 * type for these lookups is @emph{bat}, as retrieved by
 * @emph{BBPcacheid(b)}. The @emph{bat} zero is reserved for the nil
 * bat.
 *
 * @item persistence
 * The BBP is made persistent by saving it to the dictionary file
 * called @emph{BBP.dir} in the database.
 *
 * When the number of BATs rises, having all files in one directory
 * becomes a bottleneck.  The BBP therefore implements a scheme that
 * distributes all BATs in a growing directory tree with at most 64
 * BATs stored in one node.
 *
 * @item buffer management
 * The BBP is responsible for loading and saving of BATs to disk. It
 * also contains routines to unload BATs from memory when memory
 * resources get scarce. For this purpose, it administers BAT memory
 * reference counts (to know which BATs can be unloaded) and BAT usage
 * statistics (it unloads the least recently used BATs).
 *
 * @item recovery
 * When the database is closed or during a run-time syncpoint, the
 * system tables must be written to disk in a safe way, that is immune
 * for system failures (like disk full). To do so, the BBP implements
 * an atomic commit and recovery protocol: first all files to be
 * overwritten are moved to a BACKUP/ dir. If that succeeds, the
 * writes are done. If that also fully succeeds the BACKUP/ dir is
 * renamed to DELETE_ME/ and subsequently deleted.  If not, all files
 * in BACKUP/ are moved back to their original location.
 *
 * @item unloading
 * Bats which have a logical reference (ie. a lrefs > 0) but no memory
 * reference (refcnt == 0) can be unloaded. Unloading dirty bats
 * means, moving the original (committed version) to the BACKUP/ dir
 * and saving the bat. This complicates the commit and recovery/abort
 * issues.  The commit has to check if the bat is already moved. And
 * The recovery has to always move back the files from the BACKUP/
 * dir.
 *
 * @item reference counting
 * Bats use have two kinds of references: logical and physical
 * (pointer) ones.  Both are administered with the BBPincref/BBPdecref
 * routines. For backward compatibility, we maintain BBPfix/BBPunfix
 * as shorthands for the adjusting the pointer references.
 *
 * @item share counting
 * Views use the heaps of there parent bats. To save guard this, the
 * parent has a shared counter, which is incremented and decremented
 * using BBPshare and BBPunshare. These functions make sure the parent
 * is memory resident as required because of the 'pointer' sharing.
 * @end table
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#ifndef F_OK
#define F_OK 0
#endif
#ifdef _MSC_VER
#define access(f, m)	_access(f, m)
#endif

/* 
 * MonetDB itself has a global BAT Buffer Pool - BBP,
 * with a anonymous-virtual-memory mechanism to ensure
 * its address is always fixed and it can be extended
 * in-place. Inside, there's hashing of records, and
 * masking for hash buckets and other stuff which we
 * dispense with.
 */

/* 
 * The following 3 macros were moved here from gdk_bbh.h -
 * as we don't need them elsewhere.
 */
#define BBP_status_set(bbp, bid, mode, nme)		\
	do {					\
		BBP_status(bbp, bid) = mode;		\
	} while (0)

#define BBP_status_on(bbp, bid, flags, nme)					\
		BBP_status_set(bbp, bid, BBP_status(bbp, bid) | flags, nme)

#define BBP_status_off(bbp, bid, flags, nme)					\
		BBP_status_set(bbp, bid, BBP_status(bbp, bid) & ~(flags), nme)

#define KITTENNAP 4 	/* used to suspend processing */
#define BBPNONAME "."		/* filler for no name in BBP.dir */

static void BBPuncacheit(BBP* bbp, bat bid, int unloaddesc);
static BAT *getBBPdescriptor(BBP* bbp, bat i, int lock);

#ifdef HAVE_HGE
/* start out by saying we have no hge, but as soon as we've seen one,
 * we'll always say we do have it */
static int havehge = 0;
#endif

#define BBPnamecheck(s) (BBPtmpcheck(s) ? ((s)[3] == '_' ? strtol((s) + 4, NULL, 8) : -strtol((s) + 5, NULL, 8)) : 0)

bat BBPsize(BBP* bbp)
{
	return (bat) bbp->size;
}

/*
 * Unlike in MonetDB, BBPextend doesn't take a lock; we assume
 * single-threaded access.
 */
static void
BBPextend(BBP* bbp, int idx, int buildhash)
{
	if ((bat) bbp->size >= N_BBPINIT * BBPINIT)
		GDKfatal("BBPextend: trying to extend BAT pool beyond the "
			 "limit (%d)\n", N_BBPINIT * BBPINIT);

	/* make sure the new size is at least bbp->size large */
	while (bbp->limit < (bat) bbp->size) {
		assert(bbp->pool[bbp->limit >> BBPINITLOG] == NULL);
		bbp->pool[bbp->limit >> BBPINITLOG] = GDKzalloc(BBPINIT * sizeof(BBPrec));
		if (bbp->pool[bbp->limit >> BBPINITLOG] == NULL)
			GDKfatal("BBPextend: failed to extend BAT pool\n");
		bbp->limit += BBPINIT;
	}

	if (buildhash) {
		GDKfatal("Should not be building the hash here - we are only loading data");
		return;
	}
}

static inline str
BBPtmpname(str s, int len, bat i)
{
	int reverse = i < 0;

	if (reverse)
		i = -i;
	s[--len] = 0;
	while (i > 0) {
		s[--len] = '0' + (i & 7);
		i >>= 3;
	}
	s[--len] = '_';
	if (reverse)
		s[--len] = 'r';
	s[--len] = 'p';
	s[--len] = 'm';
	s[--len] = 't';
	return s + len;
}

/*
 * A read only BAT can be shared in a file system by reading its
 * descriptor separately.  The default src=0 is to read the full
 * BBPdir file.
 */
static int
heapinit(BBP* bbp, COLrec *col, const char *buf, int *hashash, const char *HT, int oidsize, int bbpversion, bat bid)
{
	int t;
	char type[11];
	unsigned short width;
	unsigned short var;
	unsigned short properties;
	lng nokey0;
	lng nokey1;
	lng nosorted;
	lng norevsorted;
	lng base;
	lng align;
	lng free;
	lng size;
	unsigned short storage;
	int n;

	(void) oidsize;		/* only used when SIZEOF_OID==8 */
	(void) bbpversion;	/* could be used to implement compatibility */

	norevsorted = 0; /* default for first case */
	if (sscanf(buf,
		   " %10s %hu %hu %hu %lld %lld %lld %lld %lld %lld %lld %lld %hu"
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &align, &free, &size, &storage,
		   &n) < 13)
		GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);

	if (properties & ~0x0F81)
		GDKfatal("BBPinit: unknown properties are set: incompatible database\n");
	*hashash = var & 2;
	var &= ~2;
	/* silently convert chr columns to bte */
	if (strcmp(type, "chr") == 0)
		strcpy(type, "bte");
#ifdef HAVE_HGE
	else if (strcmp(type, "hge") == 0)
		havehge = 1;
#endif
	if ((t = ATOMindex(type)) < 0)
		t = ATOMunknown_find(type);
	else if (var != (t == TYPE_void || BATatoms[t].atomPut != NULL))
		GDKfatal("BBPinit: inconsistent entry in BBP.dir: %s.varsized mismatch for BAT %d\n", HT, (int) bid);
	else if (var && t != 0 ?
		 ATOMsize(t) < width ||
		 (width != 1 && width != 2 && width != 4
#if SIZEOF_VAR_T == 8
		  && width != 8
#endif
			 ) :
		 ATOMsize(t) != width
#if SIZEOF_SIZE_T == 8 && SIZEOF_OID == 8
		 && (t != TYPE_oid || oidsize == 0 || width != oidsize)
#endif
		)
		GDKfatal("BBPinit: inconsistent entry in BBP.dir: %s.size mismatch for BAT %d\n", HT, (int) bid);
	col->type = t;
	col->width = width;
	col->varsized = var != 0;
	col->shift = ATOMelmshift(width);
	assert_shift_width(col->shift,col->width);
	col->nokey[0] = (BUN) nokey0;
	col->nokey[1] = (BUN) nokey1;
	col->sorted = (bit) ((properties & 0x0001) != 0);
	col->revsorted = (bit) ((properties & 0x0080) != 0);
	col->key = (properties & 0x0100) != 0;
	col->dense = (properties & 0x0200) != 0;
	col->nonil = (properties & 0x0400) != 0;
	col->nil = (properties & 0x0800) != 0;
	col->nosorted = (BUN) nosorted;
	col->norevsorted = (BUN) norevsorted;
	col->seq = base < 0 ? oid_nil : (oid) base;
	col->align = (oid) align;
	col->heap.free = (size_t) free;
	col->heap.size = (size_t) size;
	col->heap.base = NULL;
	col->heap.filename = NULL;
	col->heap.storage = (storage_t) storage;
	col->heap.copied = 0;
	col->heap.newstorage = (storage_t) storage;
	col->heap.farm_dir = bbp->directory;
	if (bbpversion <= GDKLIBRARY_INET_COMPARE &&
	    strcmp(type, "inet") == 0) {
		/* don't trust ordering information on inet columns */
		col->sorted = 0;
		col->revsorted = 0;
		col->nosorted = col->norevsorted = 0;
	}
	if (col->heap.free > col->heap.size)
		GDKfatal("BBPinit: \"free\" value larger than \"size\" in heap of bat %d\n", (int) bid);
	return n;
}

static int
vheapinit(BBP* bbp, COLrec *col, const char *buf, int hashash, bat bid)
{
	int n = 0;
	lng free, size;
	unsigned short storage;

	if (col->varsized && col->type != TYPE_void) {
		col->vheap = GDKzalloc(sizeof(Heap));
		if (col->vheap == NULL)
			GDKfatal("BBPinit: cannot allocate memory for heap.");
		if (sscanf(buf,
			   " %lld %lld %hu"
			   "%n",
			   &free, &size, &storage, &n) < 3)
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);
		col->vheap->free = (size_t) free;
		col->vheap->size = (size_t) size;
		col->vheap->base = NULL;
		col->vheap->filename = NULL;
		col->vheap->storage = (storage_t) storage;
		col->vheap->copied = 0;
		col->vheap->hashash = hashash != 0;
		col->vheap->newstorage = (storage_t) storage;
		col->vheap->parentid = bid;
		col->vheap->farm_dir = bbp->directory;
		if (col->vheap->free > col->vheap->size)
			GDKfatal("BBPinit: \"free\" value larger than \"size\" in var heap of bat %d\n", (int) bid);
	}
	return n;
}

static int
BBPreadEntries(BBP* bbp, FILE *fp, int oidsize, int bbpversion)
{
	bat bid = 0;
	char buf[4096];
	BATstore *bs;
	int needcommit = 0;

	/* read the BBP.dir and insert the BATs into the BBP */
	while (fgets(buf, sizeof(buf), fp) != NULL) {
		lng batid;
		unsigned short status;
		char headname[129];
		char tailname[129];
		char filename[129];
		unsigned int properties;
		int lastused;
		int nread;
		char *s, *options = NULL;
		char logical[1024];
		lng inserted = 0, deleted = 0, first, count, capacity;
		unsigned short map_head, map_tail, map_hheap, map_theap;
		int Hhashash, Thashash;

		if ((s = strchr(buf, '\r')) != NULL) {
			/* convert \r\n into just \n */
			if (s[1] != '\n')
				GDKfatal("BBPinit: invalid format for BBP.dir");
			*s++ = '\n';
			*s = 0;
		}

		if (bbpversion <= GDKLIBRARY_INSERTED ?
		    sscanf(buf,
			   "%lld %hu %128s %128s %128s %d %u %lld %lld %lld %lld %lld %hu %hu %hu %hu"
			   "%n",
			   &batid, &status, headname, tailname, filename,
			   &lastused, &properties, &inserted, &deleted, &first,
			   &count, &capacity, &map_head, &map_tail, &map_hheap,
			   &map_theap,
			   &nread) < 16 :
			sscanf(buf,
			   "%lld %hu %128s %128s %128s %d %u %lld %lld %lld %hu %hu %hu %hu"
			   "%n",
			   &batid, &status, headname, tailname, filename,
			   &lastused, &properties, &first,
			   &count, &capacity, &map_head, &map_tail, &map_hheap,
			   &map_theap,
			   &nread) < 14)
			GDKfatal("BBPinit: invalid format for BBP.dir%s", buf);

		if (first != 0) {
			GDKfatal("For simplicity, we only support BATs in which the first BUN (first element) is 0.");
		}

		/* convert both / and \ path separators to our own DIR_SEP */
#if DIR_SEP != '/'
		s = filename;
		while ((s = strchr(s, '/')) != NULL)
			*s++ = DIR_SEP;
#endif
#if DIR_SEP != '\\'
		s = filename;
		while ((s = strchr(s, '\\')) != NULL)
			*s++ = DIR_SEP;
#endif

		bid = (bat) batid;
		if (batid >= (lng) bbp->size) {
			bbp->size = (batid + 1);
			if ((bat) bbp->size >= bbp->limit)
				BBPextend(bbp, 0, FALSE);
		}
		if (BBP_desc(bbp, bid) != NULL)
			GDKfatal("BBPinit: duplicate entry in BBP.dir.");
		bs = GDKzalloc(sizeof(BATstore));
		if (bs == NULL)
			GDKfatal("BBPinit: cannot allocate memory for BATstore.");
		bs->B.H = &bs->H;
		bs->B.T = &bs->T;
		bs->B.S = &bs->S;
		bs->B.batCacheid = bid;
		bs->BM.H = &bs->T;
		bs->BM.T = &bs->H;
		bs->BM.S = &bs->S;
		bs->BM.batCacheid = -bid;
		BATroles(&bs->B, NULL, NULL);
		bs->S.persistence = PERSISTENT;
		bs->S.copiedtodisk = 1;
		bs->S.restricted = (properties & 0x06) >> 1;
		bs->S.first = (BUN) first;
		bs->S.count = (BUN) count;
		bs->S.inserted = bs->S.first + bs->S.count;
		bs->S.deleted = bs->S.first;
		bs->S.capacity = (BUN) capacity;

		nread += heapinit(bbp, &bs->H, buf + nread, &Hhashash, "H", oidsize, bbpversion, bid);
		nread += heapinit(bbp, &bs->T, buf + nread, &Thashash, "T", oidsize, bbpversion, bid);
		nread += vheapinit(bbp, &bs->H, buf + nread, Hhashash, bid);
		nread += vheapinit(bbp, &bs->T, buf + nread, Thashash, bid);

		if (bs->S.count > 1) {
			/* fix result of bug in BATappend not clearing
			 * revsorted property */
			if (bs->H.type == TYPE_void && bs->H.seq != oid_nil && bs->H.revsorted) {
				bs->H.revsorted = 0;
				bs->S.descdirty = 1;
				needcommit = 1;
			}
			if (bs->T.type == TYPE_void && bs->T.seq != oid_nil && bs->T.revsorted) {
				bs->T.revsorted = 0;
				bs->S.descdirty = 1;
				needcommit = 1;
			}
		}

		if (buf[nread] != '\n' && buf[nread] != ' ')
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);
		if (buf[nread] == ' ')
			options = buf + nread + 1;

		BBP_desc(bbp, bid) = bs;
		BBP_status(bbp, bid) = BBPEXISTING;	/* do we need other status bits? */
		if ((s = strchr(headname, '~')) != NULL && s == headname) {
			s = BBPtmpname(logical, sizeof(logical), bid);
		} else {
			if (s)
				*s = 0;
			strncpy(logical, headname, sizeof(logical));
			s = logical;
		}
		BBP_logical(bbp, bid) = GDKstrdup(s);
		if (strcmp(tailname, BBPNONAME) != 0)
			BBP_logical(bbp, -bid) = GDKstrdup(tailname);
		else
			BBP_logical(bbp, -bid) = GDKstrdup(BBPtmpname(tailname, sizeof(tailname), -bid));
		BBP_physical(bbp, bid) = GDKstrdup(filename);
		BBP_options(bbp, bid) = NULL;
		if (options)
			BBP_options(bbp, bid) = GDKstrdup(options);
		BBP_lastused(bbp, bid) = lastused;
		BBP_refs(bbp, bid) = 0;
		BBP_lrefs(bbp, bid) = 1;	/* any BAT we encounter here is persistent, so has a logical reference */
	}
	return needcommit;
}

#ifdef HAVE_HGE
#define SIZEOF_MAX_INT SIZEOF_HGE
#else
#define SIZEOF_MAX_INT SIZEOF_LNG
#endif

static int
BBPheader(BBP* bbp, FILE *fp, oid *BBPoid, int *OIDsize)
{
	char buf[BUFSIZ];
	int sz, bbpversion, ptrsize, oidsize, intsize;
	char *s;

	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: BBP.dir is empty");
	}
	if (sscanf(buf, "BBP.dir, GDKversion %d\n", &bbpversion) != 1) {
		GDKerror("BBPinit: old BBP without version number");
		GDKerror("dump the database using a compatible version,");
		GDKerror("then restore into new database using this version.\n");
		exit(1);
	}
	if (bbpversion != GDKLIBRARY &&
	    bbpversion != GDKLIBRARY_SORTEDPOS &&
	    bbpversion != GDKLIBRARY_64_BIT_INT &&
	    bbpversion != GDKLIBRARY_OLDWKB &&
	    bbpversion != GDKLIBRARY_INSERTED) {
		GDKfatal("BBPinit: incompatible BBP version: expected 0%o, got 0%o.", GDKLIBRARY, bbpversion);
	}
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: short BBP");
	}
	if (bbpversion <= GDKLIBRARY_64_BIT_INT) {
		if (sscanf(buf, "%d %d", &ptrsize, &oidsize) != 2) {
			GDKfatal("BBPinit: BBP.dir has incompatible format: pointer and OID sizes are missing");
		}
		intsize = SIZEOF_LNG;
	} else {
		if (sscanf(buf, "%d %d %d", &ptrsize, &oidsize, &intsize) != 3) {
			GDKfatal("BBPinit: BBP.dir has incompatible format: pointer, OID, and max. integer sizes are missing");
		}
	}
	if (ptrsize != SIZEOF_SIZE_T || oidsize != SIZEOF_OID) {
#if SIZEOF_SIZE_T == 8 && SIZEOF_OID == 8
		if (ptrsize != SIZEOF_SIZE_T || oidsize != SIZEOF_INT)
#endif
		GDKfatal("BBPinit: database created with incompatible server:\n"
			 "expected pointer size %d, got %d, expected OID size %d, got %d.",
			 SIZEOF_SIZE_T, ptrsize, SIZEOF_OID, oidsize);
	}
	if (intsize > SIZEOF_MAX_INT) {
		GDKfatal("BBPinit: database created with incompatible server:\n"
			 "expected max. integer size %d, got %d.",
			 SIZEOF_MAX_INT, intsize);
	}
	if (OIDsize)
		*OIDsize = oidsize;
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: short BBP");
	}
	*BBPoid = OIDread(buf);
	if ((s = strstr(buf, "BBPsize")) != NULL) {
		sscanf(s, "BBPsize=%d", &sz);
		sz = (int) (sz * BATMARGIN);
		if (sz > (bat) bbp->size)
			bbp->size = sz;
	}
	bbp->version = bbpversion;
	return bbpversion;
}

void
BBPaddfarm(BBP* bbp, const char *dirname, int rolemask)
{
	if (bbp->directory &&
	    dirname != bbp->directory) { 
		/* what about the role mask? */
		GDKfatal("BBPaddfarm: Only one single farm is supported when only reading data\n");
	}
	if (dirname == NULL) {
		GDKfatal("BBPinit: Invalid directory specified");
	}
	if (bbp->directory == NULL) {
		bbp->directory = GDKstrdup(dirname);
	}
}

BBP*
BBPinit(const char* farm_dir)
{
	FILE *fp = NULL;
	struct stat st;
	int bbpversion;
	int oidsize;
	oid BBPoid;
	str bbpdirstr;
	str backupbbpdirstr;
	int needcommit;
	BBP* bbp;

	bbp = GDKmalloc(sizeof(BBP));
	bbp->directory = NULL;

	BBPaddfarm(bbp, farm_dir, (1 << PERSISTENT) | (1 << TRANSIENT));

	bbpdirstr = GDKfilepath(bbp->directory, BATDIR, "BBP", "dir");
	backupbbpdirstr = GDKfilepath(bbp->directory, BAKDIR, "BBP", "dir");

	/* try to obtain a BBP.dir from bakdir */
	if (stat(backupbbpdirstr, &st) == 0) {
		fp = GDKfilelocate(
			bbp->directory, BAK_SUBDIR_ONLY DIR_SEP_STR  "BBP", "r", "dir");
		if (fp == NULL) {
			/* now it is time for real panic */
			GDKfree(bbpdirstr);
			GDKfree(backupbbpdirstr);
			GDKfree((void *) bbp->directory); /* we need to overlook the const'ness here */
			GDKfree(bbp); bbp = NULL;
			return NULL;
		}
	}
	else {
		GDKfatal("Could not find " BAK_SUBDIR_ONLY DIR_SEP_STR "BBP.dir; "
			"cowardly refusing to try other possible locations.");
	}
	assert(fp != NULL);

	/* scan the BBP.dir to obtain current size */
	bbp->limit = 0;
	memset(bbp->pool, 0, sizeof(bbp->pool));
	bbp->size = 1;

	bbpversion = BBPheader(bbp, fp, &BBPoid, &oidsize);
	bbp->version = bbpversion;

	BBPextend(bbp, 0, FALSE);		/* allocate BBP records */
	bbp->size = 1;

	needcommit = BBPreadEntries(bbp, fp, oidsize, bbpversion);
	fclose(fp);

	/* 
	 * Historically, oid values were 32-bit rather than 64-bits; MonetDB
	 * itself is willing to "fix" DBs with these small older oids; but
	 * we avoid writing anything.
	 */
#if SIZEOF_SIZE_T == 8 && SIZEOF_OID == 8
	if (oidsize == SIZEOF_INT) {
		GDKfatal("Will not accept or 'fix' small oid sizes - run MonetDB on this DB first");
		GDKfree((void*) bbp->directory) /* we can safely ignore the const'ness here */;
		GDKfree(bbp); bbp = NULL;
	}
#else
	(void) oidsize;
#endif
#ifdef GDKLIBRARY_SORTEDPOS
	if (bbpversion <= GDKLIBRARY_SORTEDPOS) {
		GDKfatal("Will not fix sortedness indication - run MonetDB on this DB first");
		GDKfree((void*) bbp->directory) /* we can safely ignore the const'ness here */;
		GDKfree(bbp); bbp = NULL;
	}
#endif
#ifdef GDKLIBRARY_OLDWKB
	if (bbpversion <= GDKLIBRARY_OLDWKB) {
		GDKfatal("Will not accept accept the old version of 'Well-Known Binary' - run MonetDB on this DB first");
		GDKfree((void*) bbp->directory) /* we can safely ignore the const'ness here */;
		GDKfree(bbp); bbp = NULL;
	}
#endif
	if (bbpversion < GDKLIBRARY) {
		GDKfatal("The database was persisted by a newer version of MonetDB GDK (%o)"
			"than the one supported by this library (%o).\n", GDKLIBRARY, bbpversion);
	}
	if (needcommit) {
		GDKfatal("Some fix/change to the persisted data is required before it can be loaded; "
			"run MonetDB on this database first.");
	}
	GDKfree(bbpdirstr);
	GDKfree(backupbbpdirstr);
	return bbp;
}

void
BBPexit(BBP* bbp)
{
	bat i;
	int skipped;

	// No locking - there are no other threads touching the BBP

	/* free all memory (just for leak-checking in Purify) */
	do {
		skipped = 0;
		for (i = 0; i < (bat) bbp->size; i++) {
			/*
			 * ... lots of BBP entries will not be valid, even when we're just
			 * loading data rather than processing any queries
			 */
			if (BBPvalid(bbp, i)) {
				BAT *b = BBP_cache(bbp, i);

				if (b) {
					if (b->batSharecnt > 0) {
						/* We'll probably not get here when just loading data */
						skipped = 1;
						continue;
					}
					/* TODO: Do we really not need to do anything with views here? */
					{
						BATfree(bbp, b);
					}
				}
				BBPuncacheit(bbp, i, TRUE);
				GDKfree(BBP_logical(bbp, i));
				BBP_logical(bbp, i) = NULL;
				GDKfree(BBP_logical(bbp, -i));
				BBP_logical(bbp, -i) = NULL;
			}
			if (BBP_physical(bbp, i)) {
				GDKfree(BBP_physical(bbp, i));
				BBP_physical(bbp, i) = NULL;
			}
		}
	} while (skipped);
	GDKfree((void *) bbp->directory); // discarding const qualifier since GDKfree is naughty
	GDKfree(bbp);

}

BATstore *
BBPgetdesc(BBP* bbp, bat i)
{
	if (i == bat_nil)
		return NULL;
	if (i < 0)
		i = -i;
	if (i != 0 && i < (bat) bbp->size && i && BBP_logical(bbp, i)) {
		return BBP_desc(bbp, i);
	}
	return NULL;
}


void
BBPcacheit(BBP* bbp, BATstore *bs, int lock)
{
	bat i = bs->B.batCacheid;
	int mode;

	if (i) {
		assert(i > 0);
	} else {
		/* Can we even get here at all? */
		GDKfatal("i is 0 - not previously entered into the BBP");
		return;
	}
	assert(bs->B.batCacheid > 0);
	assert(bs->BM.batCacheid < 0);
	assert(bs->B.batCacheid == -bs->BM.batCacheid);

	mode = (BBP_status(bbp, i) | BBPLOADED) & ~(BBPLOADING | BBPDELETING);
	BBP_status_set(bbp, i, mode, "BBPcacheit");
	BBP_desc(bbp, i) = bs;

	/* cache it! */
	BBP_cache(bbp, i) = &bs->B;
	BBP_cache(bbp, -i) = &bs->BM;

}

/*
 * BBPuncacheit changes the BBP status to swapped out.  Currently only
 * used in BBPfree (bat swapped out) and BBPclear (bat destroyed
 * forever).
 */

static void
BBPuncacheit(BBP* bbp, bat i, int unloaddesc)
{
	if (i < 0)
		i = -i;
	if (BBPcheck(bbp, i, "BBPuncacheit")) {
		BATstore *bs = BBP_desc(bbp, i);

		if (bs) {
			if (BBP_cache(bbp, i)) {
				BATDEBUG fprintf(stderr, "#uncache %d (%s)\n", (int) i, BBPname(bbp, i));

				BBP_cache(bbp, i) = BBP_cache(bbp, -i) = NULL;

				/* clearing bits can be done without the lock */
				BBP_status_off(bbp, i, BBPLOADED, "BBPuncacheit");
			}
			if (unloaddesc) {
				BBP_desc(bbp, i) = NULL;
				BATdestroy(bs);
			}
		}
	}
}

static inline int
incref(BBP* bbp, bat i, int logical, int lock)
{
	int refs;
	bat hp, tp, hvp, tvp;
	BATstore *bs;
	BAT *b;
	int load = 0;

	if (i == bat_nil) {
		/* Stefan: May this happen? Or should we better call
		 * GDKerror(), here? */
		/* GDKerror("BBPincref(bbp, ) called with bat_nil!\n"); */
		return 0;
	}
	if (i < 0)
		i = -i;

	if (!BBPcheck(bbp, i, "BBPincref"))
		return 0;

	bs = BBP_desc(bbp, i);
	if ( bs == 0) {
		/* should not have happened */
		return 0;
	}

	assert(BBP_refs(bbp, i) + BBP_lrefs(bbp, i) ||
	       BBP_status(bbp, i) & (BBPDELETED | BBPSWAPPED));
	if (logical) {
		/* parent BATs are not relevant for logical refs */
		hp = tp = hvp = tvp = 0;
		refs = ++BBP_lrefs(bbp, i);
	} else {
		hp = bs->B.H->heap.parentid;
		tp = bs->B.T->heap.parentid;
		hvp = bs->B.H->vheap == 0 || bs->B.H->vheap->parentid == i ? 0 : bs->B.H->vheap->parentid;
		tvp = bs->B.T->vheap == 0 || bs->B.T->vheap->parentid == i ? 0 : bs->B.T->vheap->parentid;
		refs = ++BBP_refs(bbp, i);
		if (refs == 1 && (hp || tp || hvp || tvp)) {
			/* If this is a view, we must load the parent
			 * BATs, but we must do that outside of the
			 * lock.  Set the BBPLOADING flag so that
			 * other threads will wait until we're
			 * done. */
			BBP_status_on(bbp, i, BBPLOADING, "BBPincref");
			load = 1;
		}
	}
	if (load) {
		/* load the parent BATs and set the heap base pointers
		 * to the correct values */
		assert(!logical);
		if (hp) {
			incref(bbp, hp, 0, lock);
			b = getBBPdescriptor(bbp, hp, lock);
			bs->B.H->heap.base = b->H->heap.base + (size_t) bs->B.H->heap.base;
			/* if we shared the hash before, share
			 * it again note that if the parent's
			 * hash is destroyed, we also don't
			 * have a hash anymore */
			if (bs->B.H->hash == (Hash *) -1)
				bs->B.H->hash = b->H->hash;
		}
		if (tp) {
			incref(bbp, tp, 0, lock);
			b = getBBPdescriptor(bbp, tp, lock);
			if (bs->B.H != bs->B.T) {  /* mirror? */
				bs->B.T->heap.base = b->H->heap.base + (size_t) bs->B.T->heap.base;
				/* if we shared the hash before, share
				 * it again note that if the parent's
				 * hash is destroyed, we also don't
				 * have a hash anymore */
				if (bs->B.T->hash == (Hash *) -1)
					bs->B.T->hash = b->H->hash;
			}
		}
		if (hvp) {
			incref(bbp, hvp, 0, lock);
			(void) getBBPdescriptor(bbp, hvp, lock);
		}
		if (tvp) {
			incref(bbp, tvp, 0, lock);
			(void) getBBPdescriptor(bbp, tvp, lock);
		}
		/* done loading, release descriptor */
		BBP_status_off(bbp, i, BBPLOADING, "BBPincref");
	}
	return refs;
}

int
BBPincref(BBP* bbp, bat i, int logical)
{
	/* No-need for locking the BBP */
	return incref(bbp, i, logical, 1); 
}

/*
 * BBPdescriptor checks whether BAT needs loading and does so if
 * necessary. You must have at least one fix on the BAT before calling
 * this.
 */
static BAT *
getBBPdescriptor(BBP* bbp, bat i, int lock)
{
	int load = FALSE;
	bat j = abs(i);
	BAT *b = NULL;

	if (!BBPcheck(bbp, i, "BBPdescriptor")) {
		return NULL;
	}
	/* Not using the following assertion, since when only reading data, the refs value is 0 for all columns
	assert(BBP_refs(bbp, i));
	*/
	if ((b = BBP_cache(bbp, i)) == NULL) {

		if  (BBP_status(bbp, j) & BBPWAITING) {
			GDKfatal("There should not be any other threads to wait for when we're just loading data");
			return NULL;
		}
		if (BBPvalid(bbp, j)) {
			b = BBP_cache(bbp, i);
			if (b == NULL) {
				load = TRUE;
				BATDEBUG {
					fprintf(stderr, "#BBPdescriptor set to unloading BAT %d\n", j);
				}
				BBP_status_on(bbp, j, BBPLOADING, "BBPdescriptor");
			}
		}
	}
	if (load) {
		IODEBUG fprintf(stderr, "#load %s\n", BBPname(bbp, i));

		b = BATload_intern(bbp, j, lock);
		bbp->swapped_in++;

		/* clearing bits can be done without the lock */
		BBP_status_off(bbp, j, BBPLOADING, "BBPdescriptor");
		if (i < 0)
			b = BATmirror(bbp, b);
	}
	return b;
}

BAT *
BBPdescriptor(BBP* bbp, bat i)
{
	int lock = 1;

	return getBBPdescriptor(bbp, i, lock);
}


// BBPdestroy'ing a BAT means deleting all of its files,
// which shouldn't happen when we're only loading data


/*
 * @- Storage trimming
 * BBPtrim unloads the least recently used BATs to free memory
 * resources.  It gets passed targets in bytes of physical memory and
 * logical virtual memory resources to free. Overhead costs are
 * reduced by making just one scan, analyzing the first BBPMAXTRIM
 * bats and keeping the result in a list for later use (the oldest bat
 * now is going to be the oldest bat in the future as well).  This
 * list is sorted on last-used timestamp. BBPtrim keeps unloading BATs
 * till the targets are met or there are no more BATs to unload.
 *
 * In determining whether a BAT will be unloaded, first it has to be
 * BBPswappable, and second its resources occupied must be of the
 * requested type. The algorithm actually makes two passes, in the
 * first only clean bats are unloaded (in order of their stamp).
 *
 * In order to keep this under control with multiple threads all
 * running out of memory at the same time, we make sure that
 * @itemize
 * @item
 * just one thread does a BBPtrim at a time (by having a BBPtrimLock
 * set).
 * @item
 * while decisions are made as to which bats to unload (1) the BBP is
 * scanned, and (2) unload decisions are made. Due to these
 * properties, the search&decide phase of BBPtrim acquires both
 * GDKcacheLock (due to (1)) and all GDKswapLocks (due to (2)). They
 * must be released during the actual unloading.  (as otherwise
 * deadlock occurs => unloading a bat may e.g. kill an accelerator
 * that is a BAT, which in turn requires BBP lock acquisition).
 * @item
 * to avoid further deadlock, the update functions in BBP that hold
 * either GDKcacheLock or a GDKswapLock may never cause a BBPtrim
 * (notice that BBPtrim could theoretically be set off just by
 * allocating a little piece of memory, e.g.  GDKstrdup()). If these
 * routines must alloc memory, they must set the BBP_notrim variable,
 * acquiring the addition GDKtrimLock, in order to prevent such
 * deadlock.
 * @item
 * the BBPtrim is atomic; only releases its locks when all BAT unload
 * work is done. This ensures that if all memory requests that
 * triggered BBPtrim could possible be satisfied by unloading BATs,
 * this will succeed.
 * @end itemize
 *
 * The scan phase was optimized further in order to stop early when it
 * is a priori known that the targets are met (which is the case if
 * the BBPtrim is not due to memory shortage but due to the ndesc
 * quota).  Note that scans may always stop before bbp->size as the
 * BBPMAXTRIM is a fixed number which may be smaller. As such, a
 * mechanism was added to resume a broken off scan at the point where
 * scanning was broken off rather than always starting at bbp->pool[1] (this
 * does more justice to the lower numbered bats and will more quickly
 * find fresh unload candidates).
 *
 * We also refined the swap criterion. If the BBPtrim was initiated
 * due to:
 * - too much descriptors: small bats are unloaded first (from LRU
 *   cold to hot)
 * - too little memory: big bats are unloaded first (from LRU cold to
 *   hot).
 * Unloading-first is enforced by subtracting @math{2^31} from the
 * stamp in the field where the candidates are sorted on.
 *
 * BBPtrim is abandoned when the application has indicated that it
 * does not need it anymore.
 */

/*
 * BBPquickdesc loads a BAT descriptor without loading the entire BAT,
 * of which the result be used only for a *limited* number of
 * purposes. Specifically, during the global sync/commit, we do not
 * want to load any BATs that are not already loaded, both because
 * this costs performance, and because getting into memory shortage
 * during a commit is extremely dangerous, as the global sync has all
 * the BBPlocks, so no BBPtrim() can be done to free memory when
 * needed. Loading a BAT tends not to be required, since the commit
 * actions mostly involve moving some pointers in the BAT
 * descriptor. However, some column types do require loading the full
 * bat. This is tested by the complexatom() routine. Such columns are
 * those of which the type has a fix/unfix method, or those that have
 * HeapDelete methods. The HeapDelete actions are not always required
 * and therefore the BBPquickdesc is parametrized.
 */
static int
complexatom(int t, int delaccess)
{
	if (t >= 0 && (BATatoms[t].atomFix || (delaccess && BATatoms[t].atomDel))) {
		return TRUE;
	}
	return FALSE;
}

BAT *
BBPquickdesc(BBP* bbp, bat bid, int delaccess)
{
	BAT *b;

	if (bid == bat_nil || bid == 0)
		return NULL;
	if (bid < 0) {
		GDKerror("BBPquickdesc: called with negative batid.\n");
		assert(0);
		return NULL;
	}
	if ((b = BBP_cache(bbp, bid)) != NULL)
		return b;	/* already cached */
	b = (BAT *) BBPgetdesc(bbp, bid);
	if (b == NULL ||
	    complexatom(b->htype, delaccess) ||
	    complexatom(b->ttype, delaccess)) {
		b = BATload_intern(bbp, bid, 1);
		bbp->swapped_in++;
	}
	return b;
}

