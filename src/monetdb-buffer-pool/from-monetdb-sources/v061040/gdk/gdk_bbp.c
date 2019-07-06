/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
 * (pointer) ones.  The logical references are administered by
 * BBPretain/BBPrelease, the physical ones by BBPfix/BBPunfix.
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

static void BBPuncacheit(BBP* bbp, bat bid, bool unloaddesc);
static BAT *getBBPdescriptor(BBP* bbp, bat i, bool lock);

#ifdef HAVE_HGE
/* start out by saying we have no hge, but as soon as we've seen one,
 * we'll always say we do have it */
static bool havehge = false;
#endif

#define BBPnamecheck(s) (BBPtmpcheck(s) ? strtol((s) + 4, NULL, 8) : 0)

bat
getBBPsize(BBP* bbp)
{
	return (bat) bbp->size;
}

/*
 * Unlike in MonetDB, BBPextend doesn't take a lock; we assume
 * single-threaded access.
 */
static gdk_return
BBPextend(BBP* bbp, int idx, bool buildhash)
{
	if ((bat) bbp->size >= N_BBPINIT * BBPINIT) {
		GDKerror("BBPextend: trying to extend BAT pool beyond the "
			 "limit (%d)\n", N_BBPINIT * BBPINIT);
		return GDK_FAIL;
	}

	/* make sure the new size is at least bbp->size large */
	while (bbp->limit < (bat) bbp->size) {
		assert(bbp->pool[bbp->limit >> BBPINITLOG] == NULL);
		bbp->pool[bbp->limit >> BBPINITLOG] = GDKzalloc(BBPINIT * sizeof(BBPrec));
		if (bbp->pool[bbp->limit >> BBPINITLOG] == NULL) {
			GDKfatal("BBPextend: failed to extend BAT pool\n");
			return GDK_FAIL;
		}
		bbp->limit += BBPINIT;
	}

	if (buildhash) {
		GDKfatal("Should not be building the hash here - we are only loading data");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* Do we  need this? */
/*
static gdk_return
recover_dir(int farmid, bool direxists)
{
	if (direxists) {
		* just try; don't care about these non-vital files *
		if (GDKunlink(farmid, BATDIR, "BBP", "bak") != GDK_SUCCEED)
			fprintf(stderr, "#recover_dir: unlink of BBP.bak failed\n");
		if (GDKmove(farmid, BATDIR, "BBP", "dir", BATDIR, "BBP", "bak") != GDK_SUCCEED)
			fprintf(stderr, "#recover_dir: rename of BBP.dir to BBP.bak failed\n");
	}
	return GDKmove(farmid, BAKDIR, "BBP", "dir", BATDIR, "BBP", "dir");
}
*/


/*
 * A read only BAT can be shared in a file system by reading its
 * descriptor separately.  The default src=0 is to read the full
 * BBPdir file.
 */
static int
headheapinit(BBP* bbp, oid *hseq, const char *buf, bat bid)
{
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

	if (sscanf(buf,
		   " %10s %hu %hu %hu "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" %hu"
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &align, &free, &size, &storage,
		   &n) < 13)
		GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);

	if (strcmp(type, "void") != 0)
		GDKfatal("BBPinit: head column must be VOID (ID = %d).", (int) bid);
	if (base < 0
#if SIZEOF_OID < SIZEOF_LNG
	    || base > (lng) GDK_oid_max
#endif
		)
		GDKfatal("BBPinit: head seqbase out of range (ID = %d, seq = "LLFMT").", (int) bid, base);
	*hseq = (oid) base;
	return n;
}

static int
heapinit(BBP* bbp, BAT *b, const char *buf, int *hashash, const char *HT, unsigned bbpversion, bat bid, const char *filename)
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

	(void) bbpversion;	/* could be used to implement compatibility */

	norevsorted = 0; /* default for first case */
	if (bbpversion <= GDKLIBRARY_TALIGN ?
	    sscanf(buf,
		   " %10s %hu %hu %hu "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" %hu"
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &align, &free, &size, &storage,
		   &n) < 13 :
		sscanf(buf,
		   " %10s %hu %hu %hu "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" %hu"
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &free, &size, &storage,
		   &n) < 12)
		GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);

	if (properties & ~0x0F81)
		GDKfatal("BBPinit: unknown properties are set: incompatible database\n");
	*hashash = var & 2;
	var &= ~2;
#ifdef HAVE_HGE
	if (strcmp(type, "hge") == 0)
		havehge = true;
#endif
	if ((t = ATOMindex(type)) < 0) {
		if ((t = ATOMunknown_find(type)) == 0)
			GDKfatal("BBPinit: no space for atom %s", type);
	} else if (var != (t == TYPE_void || BATatoms[t].atomPut != NULL))
		GDKfatal("BBPinit: inconsistent entry in BBP.dir: %s.varsized mismatch for BAT %d\n", HT, (int) bid);
	else if (var && t != 0 ?
		 ATOMsize(t) < width ||
		 (width != 1 && width != 2 && width != 4
#if SIZEOF_VAR_T == 8
		  && width != 8
#endif
			 ) :
		 ATOMsize(t) != width)
		GDKfatal("BBPinit: inconsistent entry in BBP.dir: %s.size mismatch for BAT %d\n", HT, (int) bid);
	b->ttype = t;
	b->twidth = width;
	b->tvarsized = var != 0;
	b->tshift = ATOMelmshift(width);
	assert_shift_width(b->tshift,b->twidth);
	b->tnokey[0] = (BUN) nokey0;
	b->tnokey[1] = (BUN) nokey1;
	b->tsorted = (bit) ((properties & 0x0001) != 0);
	b->trevsorted = (bit) ((properties & 0x0080) != 0);
	b->tkey = (properties & 0x0100) != 0;
	b->tdense = (properties & 0x0200) != 0;
	b->tnonil = (properties & 0x0400) != 0;
	b->tnil = (properties & 0x0800) != 0;
	b->tnosorted = (BUN) nosorted;
	b->tnorevsorted = (BUN) norevsorted;
	b->tseqbase = base < 0 ? oid_nil : (oid) base;
	b->theap.free = (size_t) free;
	b->theap.size = (size_t) size;
	b->theap.base = NULL;
	snprintf(b->theap.filename, sizeof(b->theap.filename),
		 "%s.tail", filename);
	b->theap.storage = (storage_t) storage;
	b->theap.copied = 0;
	b->theap.newstorage = (storage_t) storage;
	b->theap.farm_dir = bbp->directory;
		/* Unlike MonetDB's GDK, this library only supports reading from a single farm */
	/* heaps cannot become diry when we only read data */
/*	b->theap.dirty = 0;*/
	if (b->theap.free > b->theap.size)
		GDKfatal("BBPinit: \"free\" value larger than \"size\" in heap of bat %d\n", (int) bid);
	return n;
}

static int
vheapinit(BBP* bbp, BAT *b, const char *buf, int hashash, bat bid, const char *filename)
{
	int n = 0;
	lng free, size;
	unsigned short storage;

	if (b->tvarsized && b->ttype != TYPE_void) {
		b->tvheap = GDKzalloc(sizeof(Heap));
		if (b->tvheap == NULL)
			GDKfatal("BBPinit: cannot allocate memory for heap.");
		if (sscanf(buf,
			   " "LLFMT" "LLFMT" %hu"
			   "%n",
			   &free, &size, &storage, &n) < 3)
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);
		b->tvheap->free = (size_t) free;
		b->tvheap->size = (size_t) size;
		b->tvheap->base = NULL;
		snprintf(b->tvheap->filename, sizeof(b->tvheap->filename),
			 "%s.theap", filename);
		b->tvheap->storage = (storage_t) storage;
		b->tvheap->copied = 0;
		b->tvheap->hashash = hashash != 0;
		b->tvheap->cleanhash = 1;
		b->tvheap->newstorage = (storage_t) storage;
		/* v/heaps can't become dirty when we only read data */
/*		b->tvheap->dirty = 0; */
		b->tvheap->parentid = bid;
		b->tvheap->farm_dir = bbp->directory;
			/* Unlike MonetDB's GDK, this library only supports reading from a single farm */
		if (b->tvheap->free > b->tvheap->size)
			GDKfatal("BBPinit: \"free\" value larger than \"size\" in var heap of bat %d\n", (int) bid);
	}
	return n;
}

static int
BBPreadEntries(BBP* bbp, FILE *fp, unsigned bbpversion)
{
	bat bid = 0;
	char buf[4096];
	BAT *bn;
	/* 
	 * we're keeping track here of whether we've detected some write/commit is
	 * necessary. If it is - we're going to chicken out (not in this function)
	 * and refuse to proceed with loading the data.
	 */
	int needcommit = 0;

	/* read the BBP.dir and insert the BATs into the BBP */
	while (fgets(buf, sizeof(buf), fp) != NULL) {
		lng batid;
		unsigned short status;
		char headname[129];
		char filename[24];
		unsigned int properties;
		int lastused;
		int nread;
		char *s, *options = NULL;
		char logical[1024];
		lng inserted = 0, deleted = 0, first = 0, count, capacity, base = 0;
#ifdef GDKLIBRARY_HEADED
		/* these variables are not used in later versions */
		char tailname[129];
		unsigned short map_head = 0, map_tail = 0, map_hheap = 0, map_theap = 0;
#endif
		int Thashash;

		if ((s = strchr(buf, '\r')) != NULL) {
			/* convert \r\n into just \n */
			if (s[1] != '\n')
				GDKfatal("BBPinit: invalid format for BBP.dir");
			*s++ = '\n';
			*s = 0;
		}

		if (bbpversion <= GDKLIBRARY_INSERTED ?
		    sscanf(buf,
			   LLFMT" %hu %128s %128s %23s %d %u "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" %hu %hu %hu %hu"
			   "%n",
			   &batid, &status, headname, tailname, filename,
			   &lastused, &properties, &inserted, &deleted, &first,
			   &count, &capacity, &map_head, &map_tail, &map_hheap,
			   &map_theap,
			   &nread) < 16 :
		    bbpversion <= GDKLIBRARY_HEADED ?
		    sscanf(buf,
			   LLFMT" %hu %128s %128s %23s %d %u "LLFMT" "LLFMT" "LLFMT" %hu %hu %hu %hu"
			   "%n",
			   &batid, &status, headname, tailname, filename,
			   &lastused, &properties, &first,
			   &count, &capacity, &map_head, &map_tail, &map_hheap,
			   &map_theap,
			   &nread) < 14 :
		    sscanf(buf,
			   LLFMT" %hu %128s %23s %u "LLFMT" "LLFMT" "LLFMT
			   "%n",
			   &batid, &status, headname, filename,
			   &properties,
			   &count, &capacity, &base,
			   &nread) < 8)
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);

		if (batid >= N_BBPINIT * BBPINIT)
			GDKfatal("BBPinit: bat ID (" LLFMT ") too large to accomodate (max %d).", batid, N_BBPINIT * BBPINIT - 1);

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

		if (first != 0)
			GDKfatal("BBPinit: first != 0 (ID = "LLFMT").", batid);

		bid = (bat) batid;
		if (batid >= (lng) bbp->size) {
			bbp->size = (batid + 1);
			if ((bat) bbp->size >= bbp->limit)
				BBPextend(bbp, 0, false);
		}
		if (BBP_desc(bbp, bid) != NULL)
			GDKfatal("BBPinit: duplicate entry in BBP.dir (ID = "LLFMT").", batid);
		bn = GDKzalloc(sizeof(BAT));
		if (bn == NULL)
			GDKfatal("BBPinit: cannot allocate memory for BAT.");
		bn->batCacheid = bid;
		if (BATroles(bn, NULL) != GDK_SUCCEED)
			GDKfatal("BBPinit: BATroles failed.");
		bn->batPersistence = PERSISTENT;
		bn->batCopiedtodisk = 1;
		bn->batRestricted = (properties & 0x06) >> 1;
		bn->batCount = (BUN) count;
		bn->batInserted = bn->batCount;
		bn->batCapacity = (BUN) capacity;

		if (bbpversion <= GDKLIBRARY_HEADED) {
			nread += headheapinit(bbp, &bn->hseqbase, buf + nread, bid);
		} else {
			if (base < 0
#if SIZEOF_OID < SIZEOF_LNG
			    || base > (lng) GDK_oid_max
#endif
				)
				GDKfatal("BBPinit: head seqbase out of range (ID = "LLFMT", seq = "LLFMT").", batid, base);
			bn->hseqbase = (oid) base;
		}
		nread += heapinit(bbp, bn, buf + nread, &Thashash, "T", bbpversion, bid, filename);
		nread += vheapinit(bbp, bn, buf + nread, Thashash, bid, filename);

		if (bbpversion <= GDKLIBRARY_NOKEY &&
		    (bn->tnokey[0] != 0 || bn->tnokey[1] != 0)) {
			/* we don't trust the nokey values */
			bn->tnokey[0] = bn->tnokey[1] = 0;
			bn->batDirtydesc = 1;
			needcommit = 1;
		}

		if (buf[nread] != '\n' && buf[nread] != ' ')
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);
		if (buf[nread] == ' ')
			options = buf + nread + 1;

		BBP_desc(bbp, bid) = bn;
		BBP_status(bbp, bid) = BBPEXISTING;	/* do we need other status bits? */
		if ((s = strchr(headname, '~')) != NULL && s == headname) {
			snprintf(logical, sizeof(logical), "tmp_%o", (unsigned) bid);
		} else {
			if (s)
				*s = 0;
			strncpy(logical, headname, sizeof(logical));
		}
		s = logical;
		BBP_logical(bbp, bid) = GDKstrdup(s);
		/* tailname is ignored */
		if (strlen(filename) >= sizeof(BBP_physical(bbp, bid)))
			GDKfatal("BBPinit: physical name for BAT (%s) is too long (%zu bytes).", filename, sizeof(BBP_physical(bbp, bid)) - 1);
		strncpy(BBP_physical(bbp, bid), filename, sizeof(BBP_physical(bbp, bid)));
#ifdef STATIC_CODE_ANALYSIS
		/* help coverity */
		BBP_physical(bbp, bid)[sizeof(BBP_physical(bbp, bid)) - 1] = 0;
#endif
		BBP_options(bbp, bid) = NULL;
		if (options)
			BBP_options(bbp, bid) = GDKstrdup(options);
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

static unsigned
BBPheader(BBP* bbp, FILE *fp)
{
	char buf[BUFSIZ];
	int sz, ptrsize, oidsize, intsize;
	unsigned bbpversion;
	char *s;

	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: BBP.dir is empty");
	}
	if (sscanf(buf, "BBP.dir, GDKversion %u\n", &bbpversion) != 1) {
		GDKerror("BBPinit: old BBP without version number");
		GDKerror("dump the database using a compatible version,");
		GDKerror("then restore into new database using this version.\n");
		exit(1);
	}
	if (bbpversion != GDKLIBRARY &&
	    bbpversion != GDKLIBRARY_BADEMPTY &&
	    bbpversion != GDKLIBRARY_NOKEY &&
	    bbpversion != GDKLIBRARY_SORTEDPOS &&
	    bbpversion != GDKLIBRARY_OLDWKB &&
	    bbpversion != GDKLIBRARY_INSERTED &&
	    bbpversion != GDKLIBRARY_HEADED &&
	    bbpversion != GDKLIBRARY_TALIGN &&
	    bbpversion != GDKLIBRARY_NIL_NAN) {
		GDKfatal("BBPinit: incompatible BBP version: expected 0%o, got 0%o.\n"
			 "This database was probably created by %s version of MonetDB.",
			 GDKLIBRARY, bbpversion,
			 bbpversion > GDKLIBRARY ? "a newer" : "a too old");
	}
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: short BBP");
	}
	if (sscanf(buf, "%d %d %d", &ptrsize, &oidsize, &intsize) != 3) {
		GDKfatal("BBPinit: BBP.dir has incompatible format: pointer, OID, and max. integer sizes are missing");
	}
	if (ptrsize != SIZEOF_SIZE_T || oidsize != SIZEOF_OID) {
		GDKfatal("BBPinit: database created with incompatible server:\n"
			 "expected pointer size %d, got %d, expected OID size %d, got %d.",
			 SIZEOF_SIZE_T, ptrsize, SIZEOF_OID, oidsize);
	}
	if (intsize > SIZEOF_MAX_INT) {
		GDKfatal("BBPinit: database created with incompatible server:\n"
			 "expected max. integer size %d, got %d.",
			 SIZEOF_MAX_INT, intsize);
	}
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: short BBP");
	}
	/* when removing GDKLIBRARY_TALIGN, also remove the strstr
	 * call and just sscanf from buf */
	if ((s = strstr(buf, "BBPsize")) != NULL) {
		sscanf(s, "BBPsize=%d", &sz);
		sz = (int) (sz * BATMARGIN);
		if (sz > (bat) bbp->size)
			bbp->size = sz;
	}
	return bbpversion;
}

/* all errors are fatal */
void
BBPaddfarm(BBP* bbp, const char *dirname, int rolemask)
{
	if (dirname == NULL) {
		GDKfatal("BBPinit: Invalid directory specified");
	}

	if (strchr(dirname, '\n') != NULL) {
		GDKfatal("BBPaddfarm: no newline allowed in directory name\n");
	}
/*	if (rolemask == 0 || (rolemask & 1 && BBPfarms[0].dirname != NULL)) {*/

	if (bbp->directory &&
	    dirname != bbp->directory) { 
		/* what about the role mask? */
		GDKfatal("BBPaddfarm: Only one single farm is supported when only reading data\n");
	}

	/* do we need this? */
	if (bbp->directory == NULL) {
		bbp->directory = GDKstrdup(dirname);
	}
}

BBP*
BBPinit(const char* farm_dir)
{
	FILE *fp = NULL;
	struct stat st;
	unsigned bbpversion;
	str bbpdirstr;
	str backupbbpdirstr;
	int needcommit;
	BBP* bbp;

	bbp = GDKmalloc(sizeof(BBP));
	bbp->directory = NULL;

	BBPaddfarm(bbp, farm_dir, (1 << PERSISTENT) | (1 << TRANSIENT));

	bbpdirstr = GDKfilepath(bbp->directory, BATDIR, "BBP", "dir");
	backupbbpdirstr = GDKfilepath(bbp->directory, BAKDIR, "BBP", "dir");

	/* not performing any "cleanup" moves and deletions - we're only reading data here */

	/* try to obtain a BBP.dir from bakdir */
	if (stat(backupbbpdirstr, &st) == 0) {
		/* a "backup" directory in existence is in fact the normal state 
		   of affairs for a database (yes, it's weird); let's use it. */
		fp = GDKfilelocate(farm_dir, BAK_SUBDIR_ONLY DIR_SEP_STR  "BBP", "r", "dir");
		if (fp == NULL) {
			/* TODO: Shouldn't we use GDKfatal here? */
			GDKfree(bbpdirstr);
			GDKfree(backupbbpdirstr);
			GDKfree((void *) bbp->directory); /* we need to overlook the const'ness here */
			GDKfree(bbp); bbp = NULL;
			return NULL;
		}
	}
	else {
		GDKfatal(
			"Could not find " BAK_SUBDIR_ONLY DIR_SEP_STR "BBP.dir; "
			"cowardly refusing to try other possible locations. Try "
			"running MonetDB itself on the database first.");
	}
	assert(fp != NULL);

	/* scan the BBP.dir to obtain current size */
	bbp->limit = 0;
	memset(bbp->pool, 0, sizeof(bbp->pool));
	bbp->size = 1;

	bbpversion = BBPheader(bbp, fp);
	bbp->version = bbpversion;

	BBPextend(bbp, 0, false);		/* allocate BBP records */
	bbp->size = 1;

	needcommit = BBPreadEntries(bbp, fp, bbpversion);
	fclose(fp);

	/*
	 * Not calling BBPinithash nor BBPprepare, and not "cleaning leftovers", since BBPrecorver
	 * doesn't get called.
	 */

#ifdef GDKLIBRARY_SORTEDPOS
	if (bbpversion <= GDKLIBRARY_SORTEDPOS) {
		GDKfatal("Will not fix sortedness indication - run MonetDB on this DB first");
	}
#endif
#ifdef GDKLIBRARY_OLDWKB
	if (bbpversion <= GDKLIBRARY_OLDWKB) {
		GDKfatal("Will not accept accept the old version of 'Well-Known Binary' - run MonetDB on this DB first");
	}
#endif
#ifdef GDKLIBRARY_BADEMPTY
	if (bbpversion <= GDKLIBRARY_BADEMPTY)
		GDKfatal("Will not accept accept a DB persisted with an old version having the \"bad empty string\" issue.");
#endif
#ifdef GDKLIBRARY_NIL_NAN
	if (bbpversion <= GDKLIBRARY_NIL_NAN)
		GDKfatal("Will not change floating-point BATs to use NaN for the null value - run MonetDB on this DB first.");
#endif

	if (bbpversion < GDKLIBRARY || needcommit) {
		if (bbpversion < GDKLIBRARY) {
			GDKfatal(
				"Database was persisted by a newer version of MonetDB GDK (%o)"
				"than the one supported by this library (%o).\n", GDKLIBRARY, bbpversion);
		}
		else {
			GDKfatal("Some fix/change to the persisted data is required before it can be loaded; "
				"run MonetDB on this database first.");
		}
	}
	GDKfree(bbpdirstr);
	GDKfree(backupbbpdirstr);
	return bbp;

	/* No need for a bailout label since we never need to write when loading data. */
}


void
BBPexit(BBP* bbp)
{
	bat i;
	int skipped;

	/* No locking - there are no other threads touching the BBP */

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
						BATfree(b);
					}
				}
				BBPuncacheit(bbp, i, true);
				if (BBP_logical(bbp, i) != BBP_bak(bbp, i))
					GDKfree(BBP_logical(bbp, i));
				BBP_logical(bbp, i) = NULL;
			}
		}
	} while (skipped);
	GDKfree((void *) bbp->directory); /* discarding const qualifier since GDKfree is naughty */
	GDKfree(bbp);

}

BAT *
BBPgetdesc(BBP* bbp, bat i)
{
	if (is_bat_nil(i))
		return NULL;
	if (i < 0)
		i = -i;
	if (i != 0 && i < (bat) bbp->size && i && BBP_logical(bbp, i)) {
		return BBP_desc(bbp, i);
	}
	return NULL;
}


gdk_return
BBPcacheit(BBP* bbp, BAT *bn, bool lock)
{
	bat i = bn->batCacheid;
	int mode;

	(void) lock;
	if (i) {
		assert(i > 0);
	} else {
		GDKfatal("Should not get here.");
	}
	assert(bn->batCacheid > 0);

	mode = (BBP_status(bbp, i) | BBPLOADED) & ~(BBPLOADING | BBPDELETING);
	BBP_status_set(bbp, i, mode, "BBPcacheit");
	BBP_desc(bbp, i) = bn;

	/* cache it! */
	BBP_cache(bbp, i) = bn;
	return GDK_SUCCEED;
}

/*
 * BBPuncacheit changes the BBP status to swapped out.  Currently only
 * used in BBPfree (bat swapped out) and BBPclear (bat destroyed
 * forever).
 */

static void
BBPuncacheit(BBP* bbp, bat i, bool unloaddesc)
{
	if (i < 0)
		i = -i;
	if (BBPcheck(bbp, i, "BBPuncacheit")) {
		BAT *b = BBP_desc(bbp, i);

		if (b) {
			if (BBP_cache(bbp, i)) {
				BATDEBUG fprintf(stderr, "#uncache %d (%s)\n", (int) i, BBPname(bbp, i));

				BBP_cache(bbp, i) = NULL;

				/* clearing bits can be done without the lock */
				BBP_status_off(bbp, i, BBPLOADED, "BBPuncacheit");
			}
			if (unloaddesc) {
				BBP_desc(bbp, i) = NULL;
				BATdestroy(b);
			}
		}
	}
}

static inline int
incref(BBP* bbp, bat i, bool logical, bool lock)
{
	int refs;
	bat tp, tvp;
	BAT *b;
	bool load = false;

	if (is_bat_nil(i)) {
		/* Stefan: May this happen? Or should we better call
		 * GDKerror(), here? */
		/* GDKerror("BBPincref() called with bat_nil!\n"); */
		return 0;
	}

	if (!BBPcheck(bbp, i, logical ? "BBPretain" : "BBPfix"))
		return 0;

	b = BBP_desc(bbp, i);
	if (b == NULL) {
		/* should not have happened */
		return 0;
	}

	assert(BBP_refs(bbp, i) + BBP_lrefs(bbp, i) ||
	       BBP_status(bbp, i) & (BBPDELETED | BBPSWAPPED));
	if (logical) {
		/* parent BATs are not relevant for logical refs */
		tp = tvp = 0;
		refs = ++BBP_lrefs(bbp, i);
	} else {
		tp = b->theap.parentid;
		assert(tp >= 0);
		tvp = b->tvheap == 0 || b->tvheap->parentid == i ? 0 : b->tvheap->parentid;
		refs = ++BBP_refs(bbp, i);
		if (refs == 1 && (tp || tvp)) {
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
		if (tp) {
			BAT *pb;
			incref(bbp, tp, false, lock);
			pb = getBBPdescriptor(bbp, tp, lock);
			b->theap.base = pb->theap.base + (size_t) b->theap.base;
			/* if we shared the hash before, share it
			 * again note that if the parent's hash is
			 * destroyed, we also don't have a hash
			 * anymore */
			if (b->thash == (Hash *) -1)
				b->thash = pb->thash;
		}
		if (tvp) {
			incref(bbp, tvp, false, lock);
			(void) getBBPdescriptor(bbp, tvp, lock);
		}
		/* done loading, release descriptor */
		BBP_status_off(bbp, i, BBPLOADING, "BBPincref");
	}
	return refs;
}

int
BBPfix(BBP* bbp, bat i)
{
	return incref(bbp, i, false, 1);
}

int
BBPretain(BBP* bbp, bat i)
{
	return incref(bbp, i, true, 1); 
}

/*
 * BBPdescriptor checks whether BAT needs loading and does so if
 * necessary. You must have at least one fix on the BAT before calling
 * this.
 */
static BAT *
getBBPdescriptor(BBP* bbp, bat i, bool lock)
{
	bool load = false;
	BAT *b = NULL;

	assert(i > 0);
	if (!BBPcheck(bbp, i, "BBPdescriptor")) {
		return NULL;
	}
	/* Not using the following assertion, since when only reading data, the refs value is 0 for all columns
	assert(BBP_refs(bbp, i));
	*/
	if ((b = BBP_cache(bbp, i)) == NULL) {

		if (BBP_status(bbp, i) & BBPWAITING) {
			GDKfatal("There should not be any other threads to wait for when we're just loading data");
			return NULL;
		}
		if (BBPvalid(bbp, i)) {
			b = BBP_cache(bbp, i);
			if (b == NULL) {
				load = true;
				BATDEBUG {
					fprintf(stderr, "#BBPdescriptor set to unloading BAT %d\n", i);
				}
				BBP_status_on(bbp, i, BBPLOADING, "BBPdescriptor");
			}
		}
	}
	if (load) {
		IODEBUG fprintf(stderr, "#load %s\n", BBPname(bbp, i));

		b = BATload_intern(bbp, i, lock);
		bbp->swapped_in++;

		/* clearing bits can be done without the lock */
		BBP_status_off(bbp, i, BBPLOADING, "BBPdescriptor");
		/* Not calling BATassertProps(b) - let's trust MonetDB */
	}
	return b;
}

BAT *
BBPdescriptor(BBP* bbp, bat i)
{
	bool lock = true;

	return getBBPdescriptor(bbp, i, lock);
}

/*
 * BBPquickdesc loads a BAT descriptor without loading the entire BAT,
 * of which the result be used only for a *limited* number of
 * purposes. Specifically, during the global sync/commit, we do not
 * want to load any BATs that are not already loaded, both because
 * this costs performance, and because getting into memory shortage
 * during a commit is extremely dangerous. Loading a BAT tends not to
 * be required, since the commit actions mostly involve moving some
 * pointers in the BAT descriptor. However, some column types do
 * require loading the full bat. This is tested by the complexatom()
 * routine. Such columns are those of which the type has a fix/unfix
 * method, or those that have HeapDelete methods. The HeapDelete
 * actions are not always required and therefore the BBPquickdesc is
 * parametrized.
 */
static bool
complexatom(int t, int delaccess)
{
	if (t >= 0 && (BATatoms[t].atomFix || (delaccess && BATatoms[t].atomDel))) {
		return true;
	}
	return false;
}

BAT *
BBPquickdesc(BBP* bbp, bat bid, int delaccess)
{
	BAT *b;

	if (is_bat_nil(bid))
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
	    complexatom(b->ttype, delaccess)) {
		b = BATload_intern(bbp, bid, 1);
		bbp->swapped_in++;
	}
	return b;
}
