/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * @t The Goblin Database Kernel
 * @v Version 3.05
 * @a Martin L. Kersten, Peter Boncz, Niels Nes, Sjoerd Mullender
 *
 * @+ The Inner Core
 * The innermost library of the MonetDB database system is formed by
 * the library called GDK, an abbreviation of Goblin Database Kernel.
 * Its development was originally rooted in the design of a pure
 * active-object-oriented programming language, before development
 * was shifted towards a re-usable database kernel engine.
 *
 * GDK is a C library that provides ACID properties on a DSM model
 * @tex
 * [@cite{Copeland85}]
 * @end tex
 * , using main-memory
 * database algorithms
 * @tex
 * [@cite{Garcia-Molina92}]
 * @end tex
 *  built on virtual-memory
 * OS primitives and multi-threaded parallelism.
 * Its implementation has undergone various changes over its decade
 * of development, many of which were driven by external needs to
 * obtain a robust and fast database system.
 *
 * The coding scheme explored in GDK has also laid a foundation to
 * communicate over time experiences and to provide (hopefully)
 * helpful advice near to the place where the code-reader needs it.
 * Of course, over such a long time the documentation diverges from
 * reality. Especially in areas where the environment of this package
 * is being described.
 * Consider such deviations as historic landmarks, e.g. crystallization
 * of brave ideas and mistakes rectified at a later stage.
 *
 * @+ Short Outline
 * The facilities provided in this implementation are:
 * @itemize
 * @item
 * GDK or Goblin Database Kernel routines for session management
 * @item
 *  BAT routines that define the primitive operations on the
 * database tables (BATs).
 * @item
 *  BBP routines to manage the BAT Buffer Pool (BBP).
 * @item
 *  ATOM routines to manipulate primitive types, define new types
 * using an ADT interface.
 * @item
 *  HEAP routines for manipulating heaps: linear spaces of memory
 * that are GDK's vehicle of mass storage (on which BATs are built).
 * @item
 *  DELTA routines to access inserted/deleted elements within a
 * transaction.
 * @item
 *  HASH routines for manipulating GDK's built-in linear-chained
 * hash tables, for accelerating lookup searches on BATs.
 * @item
 *  TM routines that provide basic transaction management primitives.
 * @item
 *  TRG routines that provided active database support. [DEPRECATED]
 * @item
 *  ALIGN routines that implement BAT alignment management.
 * @end itemize
 *
 * The Binary Association Table (BAT) is the lowest level of storage
 * considered in the Goblin runtime system
 * @tex
 * [@cite{Goblin}]
 * @end tex
 * .  A BAT is a
 * self-descriptive main-memory structure that represents the
 * @strong{binary relationship} between two atomic types.  The
 * association can be defined over:
 * @table @code
 * @item void:
 *  virtual-OIDs: a densely ascending column of OIDs (takes zero-storage).
 * @item bit:
 *  Booleans, implemented as one byte values.
 * @item bte:
 *  Tiny (1-byte) integers (8-bit @strong{integer}s).
 * @item sht:
 *  Short integers (16-bit @strong{integer}s).
 * @item int:
 *  This is the C @strong{int} type (32-bit).
 * @item oid:
 *  Unique @strong{long int} values uses as object identifier. Highest
 *	    bit cleared always.  Thus, oids-s are 31-bit numbers on
 *	    32-bit systems, and 63-bit numbers on 64-bit systems.
 * @item ptr:
 * Memory pointer values. DEPRECATED.  Can only be stored in transient
 * BATs.
 * @item flt:
 *  The IEEE @strong{float} type.
 * @item dbl:
 *  The IEEE @strong{double} type.
 * @item lng:
 *  Longs: the C @strong{long long} type (64-bit integers).
 * @item hge:
 *  "huge" integers: the GCC @strong{__int128} type (128-bit integers).
 * @item str:
 *  UTF-8 strings (Unicode). A zero-terminated byte sequence.
 * @item bat:
 *  Bat descriptor. This allows for recursive administered tables, but
 *  severely complicates transaction management. Therefore, they CAN
 *  ONLY BE STORED IN TRANSIENT BATs.
 * @end table
 *
 * This model can be used as a back-end model underlying other -higher
 * level- models, in order to achieve @strong{better performance} and
 * @strong{data independence} in one go. The relational model and the
 * object-oriented model can be mapped on BATs by vertically splitting
 * every table (or class) for each attribute. Each such a column is
 * then stored in a BAT with type @strong{bat[oid,attribute]}, where
 * the unique object identifiers link tuples in the different BATs.
 * Relationship attributes in the object-oriented model hence are
 * mapped to @strong{bat[oid,oid]} tables, being equivalent to the
 * concept of @emph{join indexes} @tex [@cite{Valduriez87}] @end tex .
 *
 * The set of built-in types can be extended with user-defined types
 * through an ADT interface.  They are linked with the kernel to
 * obtain an enhanced library, or they are dynamically loaded upon
 * request.
 *
 * Types can be derived from other types. They represent something
 * different than that from which they are derived, but their internal
 * storage management is equal. This feature facilitates the work of
 * extension programmers, by enabling reuse of implementation code,
 * but is also used to keep the GDK code portable from 32-bits to
 * 64-bits machines: the @strong{oid} and @strong{ptr} types are
 * derived from @strong{int} on 32-bits machines, but is derived from
 * @strong{lng} on 64 bits machines. This requires changes in only two
 * lines of code each.
 *
 * To accelerate lookup and search in BATs, GDK supports one built-in
 * search accelerator: hash tables. We choose an implementation
 * efficient for main-memory: bucket chained hash
 * @tex
 * [@cite{LehCar86,Analyti92}]
 * @end tex
 * . Alternatively, when the table is sorted, it will resort to
 * merge-scan operations or binary lookups.
 *
 * BATs are built on the concept of heaps, which are large pieces of
 * main memory. They can also consist of virtual memory, in case the
 * working set exceeds main-memory. In this case, GDK supports
 * operations that cluster the heaps of a BAT, in order to improve
 * performance of its main-memory.
 *
 *
 * @- Rationale
 * The rationale for choosing a BAT as the building block for both
 * relational and object-oriented system is based on the following
 * observations:
 *
 * @itemize
 * @item -
 * Given the fact that CPU speed and main-memory increase in current
 * workstation hardware for the last years has been exceeding IO
 * access speed increase, traditional disk-page oriented algorithms do
 * no longer take best advantage of hardware, in most database
 * operations.
 *
 * Instead of having a disk-block oriented kernel with a large memory
 * cache, we choose to build a main-memory kernel, that only under
 * large data volumes slowly degrades to IO-bound performance,
 * comparable to traditional systems
 * @tex
 * [@cite{boncz95,boncz96}]
 * @end tex
 * .
 *
 * @item -
 * Traditional (disk-based) relational systems move too much data
 * around to save on (main-memory) join operations.
 *
 * The fully decomposed store (DSM
 * @tex
 * [@cite{Copeland85})]
 * @end tex
 * assures that only those attributes of a relation that are needed,
 * will have to be accessed.
 *
 * @item -
 * The data management issues for a binary association is much
 * easier to deal with than traditional @emph{struct}-based approaches
 * encountered in relational systems.
 *
 * @item -
 * Object-oriented systems often maintain a double cache, one with the
 * disk-based representation and a C pointer-based main-memory
 * structure.  This causes expensive conversions and replicated
 * storage management.  GDK does not do such `pointer swizzling'. It
 * used virtual-memory (@strong{mmap()}) and buffer management advice
 * (@strong{madvise()}) OS primitives to cache only once. Tables take
 * the same form in memory as on disk, making the use of this
 * technique transparent
 * @tex
 * [@cite{oo7}]
 * @end tex
 * .
 * @end itemize
 *
 * A RDBMS or OODBMS based on BATs strongly depends on our ability to
 * efficiently support tuples and to handle small joins, respectively.
 *
 * The remainder of this document describes the Goblin Database kernel
 * implementation at greater detail. It is organized as follows:
 * @table @code
 * @item @strong{GDK Interface}:
 *
 * It describes the global interface with which GDK sessions can be
 * started and ended, and environment variables used.
 *
 * @item @strong{Binary Association Tables}:
 *
 * As already mentioned, these are the primary data structure of GDK.
 * This chapter describes the kernel operations for creation,
 * destruction and basic manipulation of BATs and BUNs (i.e. tuples:
 * Binary UNits).
 *
 * @item @strong{BAT Buffer Pool:}
 *
 * All BATs are registered in the BAT Buffer Pool. This directory is
 * used to guide swapping in and out of BATs. Here we find routines
 * that guide this swapping process.
 *
 * @item @strong{GDK Extensibility:}
 *
 * Atoms can be defined using a unified ADT interface.  There is also
 * an interface to extend the GDK library with dynamically linked
 * object code.
 *
 * @item @strong{GDK Utilities:}
 *
 * Memory allocation and error handling primitives are
 * provided. Layers built on top of GDK should use them, for proper
 * system monitoring.  Thread management is also included here.
 *
 * @item @strong{Transaction Management:}
 *
 * For the time being, we just provide BAT-grained concurrency and
 * global transactions. Work is needed here.
 *
 * @item @strong{BAT Alignment:}
 * Due to the mapping of multi-ary datamodels onto the BAT model, we
 * expect many correspondences among BATs, e.g.
 * @emph{bat(oid,attr1),..  bat(oid,attrN)} vertical
 * decompositions. Frequent activities will be to jump from one
 * attribute to the other (`bunhopping'). If the head columns are
 * equal lists in two BATs, merge or even array lookups can be used
 * instead of hash lookups. The alignment interface makes these
 * relations explicitly manageable.
 *
 * In GDK, complex data models are mapped with DSM on binary tables.
 * Usually, one decomposes @emph{N}-ary relations into @emph{N} BATs
 * with an @strong{oid} in the head column, and the attribute in the
 * tail column.  There may well be groups of tables that have the same
 * sets of @strong{oid}s, equally ordered. The alignment interface is
 * intended to make this explicit.  Implementations can use this
 * interface to detect this situation, and use cheaper algorithms
 * (like merge-join, or even array lookup) instead.
 *
 * @item @strong{BAT Iterators:}
 *
 * Iterators are C macros that generally encapsulate a complex
 * for-loop.  They would be the equivalent of cursors in the SQL
 * model. The macro interface (instead of a function call interface)
 * is chosen to achieve speed when iterating main-memory tables.
 *
 * @item @strong{Common BAT Operations:}
 *
 * These are much used operations on BATs, such as aggregate functions
 * and relational operators. They are implemented in terms of BAT- and
 * BUN-manipulation GDK primitives.
 * @end table
 *
 * @+ Interface Files
 * In this section we summarize the user interface to the GDK library.
 * It consist of a header file (gdk.h) and an object library
 * (gdklib.a), which implements the required functionality. The header
 * file must be included in any program that uses the library. The
 * library must be linked with such a program.
 *
 * @- Database Context
 *
 * The MonetDB environment settings are collected in a configuration
 * file. Amongst others it contains the location of the database
 * directory.  First, the database directory is closed for other
 * servers running at the same time.  Second, performance enhancements
 * may take effect, such as locking the code into memory (if the OS
 * permits) and preloading the data dictionary.  An error at this
 * stage normally lead to an abort.
 */

#ifndef _GDK_H_
#define _GDK_H_

/* standard includes upon which all configure tests depend */
#ifdef HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
# include <sys/stat.h>
#endif
#include <stddef.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif

#include <ctype.h>		/* isspace etc. */

#ifdef HAVE_SYS_FILE_H
# include <sys/file.h>
#endif

#ifdef HAVE_DIRENT_H
# include <dirent.h>
#endif

#include <limits.h>		/* for *_MIN and *_MAX */
#include <float.h>		/* for FLT_MAX and DBL_MAX */

#include "gdk_system.h"
#include "gdk_posix.h"

#undef MIN
#undef MAX
#define MAX(A,B)	((A)<(B)?(B):(A))
#define MIN(A,B)	((A)>(B)?(B):(A))

/* defines from ctype with casts that allow passing char values */
#define GDKisspace(c)	isspace((unsigned char) (c))
#define GDKisalnum(c)	isalnum((unsigned char) (c))
#define GDKisdigit(c)	isdigit((unsigned char) (c))

#define BATDIR		"bat"
#define TEMPDIR_NAME	"TEMP_DATA"

#define DELDIR		BATDIR DIR_SEP_STR "DELETE_ME"
#define BAKDIR		BATDIR DIR_SEP_STR "BACKUP"
#define BAK_SUBDIR_ONLY                    "BACKUP"
#define SUBDIR		BAKDIR DIR_SEP_STR "SUBCOMMIT" /* note K, not T */
#define LEFTDIR		BATDIR DIR_SEP_STR "LEFTOVERS"
#define TEMPDIR		BATDIR DIR_SEP_STR TEMPDIR_NAME

/* A remnant of mutils.h */
#define MONETDB_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)

/*
   See `man mserver5` or tools/mserver/mserver5.1
   for a documentation of the following debug options.
*/

#define THRDMASK	(1)
#define THRDDEBUG	if (GDKdebug & THRDMASK)
#define CHECKMASK	(1<<1)
#define CHECKDEBUG	if (GDKdebug & CHECKMASK)
#define MEMMASK		(1<<2)
#define MEMDEBUG	if (GDKdebug & MEMMASK)
#define PROPMASK	(1<<3)
#define PROPDEBUG	if (GDKdebug & PROPMASK)
#define IOMASK		(1<<4)
#define IODEBUG		if (GDKdebug & IOMASK)
#define BATMASK		(1<<5)
#define BATDEBUG	if (GDKdebug & BATMASK)
/* PARSEMASK not used anymore
#define PARSEMASK	(1<<6)
#define PARSEDEBUG	if (GDKdebug & PARSEMASK)
*/
#define PARMASK		(1<<7)
#define PARDEBUG	if (GDKdebug & PARMASK)
/* HEADLESSMASK not used anymore
#define HEADLESSMASK	(1<<8)
#define HEADLESSDEBUG	if (GDKdebug & HEADLESSMASK)
*/
#define TMMASK		(1<<9)
#define TMDEBUG		if (GDKdebug & TMMASK)
#define TEMMASK		(1<<10)
#define TEMDEBUG	if (GDKdebug & TEMMASK)
/* DLMASK not used anymore
#define DLMASK		(1<<11)
#define DLDEBUG		if (GDKdebug & DLMASK)
*/
#define PERFMASK	(1<<12)
#define PERFDEBUG	if (GDKdebug & PERFMASK)
#define DELTAMASK	(1<<13)
#define DELTADEBUG	if (GDKdebug & DELTAMASK)
#define LOADMASK	(1<<14)
#define LOADDEBUG	if (GDKdebug & LOADMASK)
/* YACCMASK not used anymore
#define YACCMASK	(1<<15)
#define YACCDEBUG	if (GDKdebug & YACCMASK)
*/
/*
#define ?tcpip?		if (GDKdebug&(1<<16))
#define ?monet_multiplex?	if (GDKdebug&(1<<17))
#define ?ddbench?	if (GDKdebug&(1<<18))
#define ?ddbench?	if (GDKdebug&(1<<19))
#define ?ddbench?	if (GDKdebug&(1<<20))
*/
#define ACCELMASK	(1<<20)
#define ACCELDEBUG	if (GDKdebug & (ACCELMASK|ALGOMASK))
#define ALGOMASK	(1<<21)
#define ALGODEBUG	if (GDKdebug & ALGOMASK)
#define ESTIMASK	(1<<22)
#define ESTIDEBUG	if (GDKdebug & ESTIMASK)
/* XPROPMASK not used anymore
#define XPROPMASK	(1<<23)
#define XPROPDEBUG	if (GDKdebug & XPROPMASK)
*/

#define NOSYNCMASK	(1<<24)

#define DEADBEEFMASK	(1<<25)
#define DEADBEEFCHK	if (!(GDKdebug & DEADBEEFMASK))

#define ALLOCMASK	(1<<26)
#define ALLOCDEBUG	if (GDKdebug & ALLOCMASK)

/* M5, only; cf.,
 * monetdb5/mal/mal.h
 */
#define OPTMASK		(1<<27)
#define OPTDEBUG	if (GDKdebug & OPTMASK)

#define HEAPMASK	(1<<28)
#define HEAPDEBUG	if (GDKdebug & HEAPMASK)

#define FORCEMITOMASK	(1<<29)
#define FORCEMITODEBUG	if (GDKdebug & FORCEMITOMASK)

/*
 * @- GDK session handling
 * @multitable @columnfractions 0.08 0.7
 * @item int
 * @tab GDKinit (char *db, char *dbpath, int allocmap)
 * @item int
 * @tab GDKexit (int status)
 * @end multitable
 *
 * The session is bracketed by GDKinit and GDKexit. Initialization
 * involves setting up the administration for database access, such as
 * memory allocation for the database buffer pool.  During the exit
 * phase any pending transaction is aborted and the database is freed
 * for access by other users.  A zero is returned upon encountering an
 * erroneous situation.
 *
 * @- Definitions
 * The interface definitions for the application programs are shown
 * below.  The global variables should not be modified directly.
 */
#ifndef TRUE
#define TRUE		true
#define FALSE		false
#endif

#define IDLENGTH	64	/* maximum BAT id length */
#define BATMARGIN	1.2	/* extra free margin for new heaps */
#define BATTINY_BITS	8
#define BATTINY		((BUN)1<<BATTINY_BITS)	/* minimum allocation buncnt for a BAT */

enum {
	TYPE_void = 0,
	TYPE_bit,
	TYPE_bte,
	TYPE_sht,
	TYPE_bat,		/* BAT id: index in BBPcache */
	TYPE_int,
	TYPE_oid,
	TYPE_ptr,		/* C pointer! */
	TYPE_flt,
	TYPE_dbl,
	TYPE_lng,
#ifdef HAVE_HGE
	TYPE_hge,
#endif
	TYPE_str,
	/*
	 * "inlined" from the mtime MAL module for the reader library,
	 * which doesn't support MAL modules and doesn't load any dynamically.
	 */
	TYPE_date,
	TYPE_daytime,
	TYPE_timestamp,

	/* MonetDB code just assumes TYPE_str is the last builtin - we can't. */
	TYPE_last_builtin = TYPE_timestamp,
	TYPE_any = 255,		/* limit types to <255! */
};

typedef int8_t bit;
typedef int8_t bte;
typedef int16_t sht;
typedef int64_t lng;
typedef uint64_t ulng;

#define SIZEOF_OID	SIZEOF_SIZE_T
typedef size_t oid;
#define OIDFMT		"%zu"

typedef int bat;		/* Index into BBP */
typedef void *ptr;		/* Internal coding of types */

#define SIZEOF_PTR	SIZEOF_VOID_P
typedef float flt;
typedef double dbl;
typedef char *str;

#define SIZEOF_LNG		8
#define LL_CONSTANT(val)	INT64_C(val)
#define LLFMT			"%" PRId64
#define ULLFMT			"%" PRIu64

typedef oid var_t;		/* type used for heap index of var-sized BAT */
#define SIZEOF_VAR_T	SIZEOF_OID
#define VARFMT		OIDFMT

#if SIZEOF_VAR_T == SIZEOF_INT
#define VAR_MAX		((var_t) INT_MAX)
#else
#define VAR_MAX		((var_t) INT64_MAX)
#endif

typedef oid BUN;		/* BUN position */
#define SIZEOF_BUN	SIZEOF_OID
#define BUNFMT		OIDFMT
/* alternatively:
typedef size_t BUN;
#define SIZEOF_BUN	SIZEOF_SIZE_T
#define BUNFMT		"%zu"
*/
#if SIZEOF_BUN == SIZEOF_INT
#define BUN_NONE ((BUN) INT_MAX)
#else
#define BUN_NONE ((BUN) INT64_MAX)
#endif
#define BUN_MAX (BUN_NONE - 1)	/* maximum allowed size of a BAT */

#define BUN2 2
#define BUN4 4
#if SIZEOF_BUN > 4
#define BUN8 8
#endif
typedef uint16_t BUN2type;
typedef uint32_t BUN4type;
#if SIZEOF_BUN > 4
typedef uint64_t BUN8type;
#endif
#define BUN2_NONE ((BUN2type) UINT16_C(0xFFFF))
#define BUN4_NONE ((BUN4type) UINT32_C(0xFFFFFFFF))
#if SIZEOF_BUN > 4
#define BUN8_NONE ((BUN8type) UINT64_C(0xFFFFFFFFFFFFFFFF))
#endif


/*
 * @- Checking and Error definitions:
 */
typedef enum { GDK_FAIL, GDK_SUCCEED } gdk_return;

#define ATOMextern(t)	(ATOMstorage(t) >= TYPE_str)

typedef enum {
	PERSISTENT = 0,
	TRANSIENT,
} role_t;

/* Heap storage modes */
typedef enum {
	STORE_MEM     = 0,	/* load into GDKmalloced memory */
	STORE_MMAP    = 1,	/* mmap() into virtual memory */
	STORE_PRIV    = 2,	/* BAT copy of copy-on-write mmap */
	STORE_CMEM    = 3,	/* load into malloc (not GDKmalloc) memory*/
	STORE_NOWN    = 4,	/* memory not owned by the BAT */
	STORE_MMAPABS = 5,	/* mmap() into virtual memory from an
				 * absolute path (not part of dbfarm) */
	STORE_INVALID		/* invalid value, used to indicate error */
} storage_t;

typedef struct {
	size_t free;		/* index where free area starts. */
	size_t size;		/* size of the heap (bytes) */
	char *base;		/* base pointer in memory. */
	char filename[32];	/* file containing image of the heap */

	/*	bte farmid;		 * id of farm where heap is located */
		/* We only use one DB farm at a time with this library. */
	/* TODO: Does every heap really need to know about the farm dir? */
	const char* farm_dir;	/* directory of the farm where this heap is located */
	bool copied:1,		/* a copy of an existing map. */
		hashash:1,	/* the string heap contains hash values */
		cleanhash:1;	/* string heaps must clean hash */
		/* dirty:1;	* specific heap dirty marker */
			/* heaps cannot become dirty when we only read data - so not using this field*/

	storage_t storage;	/* storage mode (mmap/malloc). */
	storage_t newstorage;	/* new desired storage mode at re-allocation. */
	bat parentid;		/* cache id of VIEW parent bat */
} Heap;

typedef struct {
	int type;		/* type of index entity */
	int width;		/* width of hash entries */
	BUN nil;		/* nil representation */
	BUN lim;		/* collision list size */
	BUN mask;		/* number of hash buckets-1 (power of 2) */
	void *Hash;		/* hash table */
	void *Link;		/* collision list */
	Heap heap;		/* heap where the hash is stored */
} Hash;

/* Imprints not supported - we're just reading the column data proper. */

/*
 * @+ Binary Association Tables
 * Having gone to the previous preliminary definitions, we will now
 * introduce the structure of Binary Association Tables (BATs) in
 * detail. They are the basic storage unit on which GDK is modeled.
 *
 * The BAT holds an unlimited number of binary associations, called
 * BUNs (@strong{Binary UNits}).  The two attributes of a BUN are
 * called @strong{head} (left) and @strong{tail} (right) in the
 * remainder of this document.
 *
 *  @c image{http://monetdb.cwi.nl/projects/monetdb-mk/imgs/bat1,,,,feps}
 *
 * The above figure shows what a BAT looks like. It consists of two
 * columns, called head and tail, such that we have always binary
 * tuples (BUNs). The overlooking structure is the @strong{BAT
 * record}.  It points to a heap structure called the @strong{BUN
 * heap}.  This heap contains the atomic values inside the two
 * columns. If they are fixed-sized atoms, these atoms reside directly
 * in the BUN heap. If they are variable-sized atoms (such as string
 * or polygon), however, the columns has an extra heap for storing
 * those (such @strong{variable-sized atom heaps} are then referred to
 * as @strong{Head Heap}s and @strong{Tail Heap}s). The BUN heap then
 * contains integer byte-offsets (fixed-sized, of course) into a head-
 * or tail-heap.
 *
 * The BUN heap contains a contiguous range of BUNs. It starts after
 * the @strong{first} pointer, and finishes at the end in the
 * @strong{free} area of the BUN. All BUNs after the @strong{inserted}
 * pointer have been added in the last transaction (and will be
 * deleted on a transaction abort). All BUNs between the
 * @strong{deleted} pointer and the @strong{first} have been deleted
 * in this transaction (and will be reinserted at a transaction
 * abort).
 *
 * The location of a certain BUN in a BAT may change between
 * successive library routine invocations.  Therefore, one should
 * avoid keeping references into the BAT storage area for long
 * periods.
 *
 * Passing values between the library routines and the enclosing C
 * program is primarily through value pointers of type ptr. Pointers
 * into the BAT storage area should only be used for retrieval. Direct
 * updates of data stored in a BAT is forbidden. The user should
 * adhere to the interface conventions to guarantee the integrity
 * rules and to maintain the (hidden) auxiliary search structures.
 *
 * @- GDK variant record type
 * When manipulating values, MonetDB puts them into value records.
 * The built-in types have a direct entry in the union. Others should
 * be represented as a pointer of memory in pval or as a string, which
 * is basically the same. In such cases the len field indicates the
 * size of this piece of memory.
 */
typedef struct {
	union {			/* storage is first in the record */
		int ival;
		oid oval;
		sht shval;
		bte btval;
		flt fval;
		ptr pval;
		bat bval;
		str sval;
		dbl dval;
		lng lval;
#ifdef HAVE_HGE
		hge hval;
#endif
	} val;
	size_t len;
	int vtype;
} *ValPtr, ValRecord;

/* interface definitions */
gdk_export void *VALconvert(int typ, ValPtr t);
gdk_export char *VALformat(const ValRecord *res);
gdk_export ValPtr VALcopy(ValPtr dst, const ValRecord *src);
gdk_export ValPtr VALinit(ValPtr d, int tpe, const void *s);
gdk_export void VALempty(ValPtr v);
gdk_export void VALclear(ValPtr v);
gdk_export ValPtr VALset(ValPtr v, int t, void *p);
gdk_export void *VALget(ValPtr v);
gdk_export int VALcmp(const ValRecord *p, const ValRecord *q);
gdk_export int VALisnil(const ValRecord *v);

typedef struct PROPrec PROPrec;

/* see also comment near BATassertProps() for more information about
 * the properties */
typedef struct {
	str id;			/* label for column */

	uint16_t width;		/* byte-width of the atom array */
	int8_t type;		/* type id. */
	uint8_t shift;		/* log2 of bun width */
	bool varsized:1,	/* varsized/void (true) or fixedsized (false) */
		key:1,		/* no duplicate values present */
		unique:1,	/* no duplicate values allowed */
		nonil:1,	/* there are no nils in the column */
		nil:1,		/* there is a nil in the column */
		sorted:1,	/* column is sorted in ascending order */
		revsorted:1;	/* column is sorted in descending order */
	BUN nokey[2];		/* positions that prove key==FALSE */
	BUN nosorted;		/* position that proves sorted==FALSE */
	BUN norevsorted;	/* position that proves revsorted==FALSE */
	oid seq;		/* start of dense sequence */

	Heap heap;		/* space for the column. */
	Heap *vheap;		/* space for the varsized data. */
	Hash *hash;		/* hash table */

	/* no order index (and no imprints) when only reading data */

	/* Imprints *imprints;	* column imprints index */
	/* Heap *orderidx;	* order oid index */

	PROPrec *props;		/* list of dynamic properties stored in the bat descriptor */
} COLrec;

#define ORDERIDXOFF		3

/* assert that atom width is power of 2, i.e., width == 1<<shift */
#define assert_shift_width(shift,width) assert(((shift) == 0 && (width) == 0) || ((unsigned)1<<(shift)) == (unsigned)(width))

#define GDKLIBRARY_TALIGN	061036U	/* talign field in BBP.dir */
#define GDKLIBRARY_NIL_NAN	061037U	/* flt/dbl NIL not represented by NaN */
#define GDKLIBRARY_BLOB_SORT	061040U /* blob compare changed */
#define GDKLIBRARY		061041U

typedef struct BAT {
	/* static bat properties */
	bat batCacheid;		/* index into BBP */
	oid hseqbase;		/* head seq base */

	/* dynamic bat properties */
	/* MT_Id creator_tid;*/	/* which thread created it */
	bool
	 /* TODO: Get rid of the next 3 "dirty" flags - we never make anything dirty when reading only. */
	 batCopiedtodisk:1,	/* once written */
	 batDirtyflushed:1,	/* was dirty before commit started? */
	 batDirtydesc:1,	/* bat descriptor dirty marker */
	 batTransient:1;	/* should the BAT persist on disk? */
	uint8_t	/* adjacent bit fields are packed together (if they fit) */
	 batRestricted:2;	/* access privileges */
	role_t batRole;		/* role of the bat */
	uint16_t unused; 	/* value=0 for now (sneakily used by mat.c) */
	int batSharecnt;	/* incoming view count */

	/* delta status administration */
	BUN batInserted;	/* start of inserted elements */
	BUN batCount;		/* tuple count */
	BUN batCapacity;	/* tuple capacity */

	/* dynamic column properties */
	COLrec T;		/* column info */
} BAT;

typedef struct BATiter {
	BAT *b;
	oid tvid;
} BATiter;

/* macros to hide complexity of the BAT structure */
#define ttype		T.type
#define tkey		T.key
#define tunique		T.unique
#define tvarsized	T.varsized
#define tseqbase	T.seq
#define tsorted		T.sorted
#define trevsorted	T.revsorted
#define tident		T.id
#define torderidx	T.orderidx
#define twidth		T.width
#define tshift		T.shift
#define tnonil		T.nonil
#define tnil		T.nil
#define tnokey		T.nokey
#define tnosorted	T.nosorted
#define tnorevsorted	T.norevsorted
#define theap		T.heap
#define tvheap		T.vheap
#define thash		T.hash
#define timprints	T.imprints
#define tprops		T.props



/*
 * @- Heap Management
 * Heaps are the low-level entities of mass storage in
 * BATs. Currently, they can either be stored on disk, loaded into
 * memory, or memory mapped.
 * @multitable @columnfractions 0.08 0.7
 * @item int
 * @tab
 *  HEAPalloc (Heap *h, size_t nitems, size_t itemsize);
 * @item int
 * @tab
 *  HEAPfree (Heap *h, bool remove);
 * @item int
 * @tab
 *  HEAPextend (Heap *h, size_t size, bool mayshare);
 * @item int
 * @tab
 *  HEAPload (Heap *h, str nme,ext, bool trunc);
 * @item int
 * @tab
 *  HEAPsave (Heap *h, str nme,ext);
 * @item int
 * @tab
 *  HEAPcopy (Heap *dst,*src);
 * @item int
 * @tab
 *  HEAPdelete (Heap *dst, str o, str ext);
 * @item int
 * @tab
 *  HEAPwarm (Heap *h);
 * @end multitable
 *
 *
 * These routines should be used to alloc free or extend heaps; they
 * isolate you from the different ways heaps can be accessed.
 */

gdk_export void HEAP_initialize(
	Heap *heap,		/* nbytes -- Initial size of the heap. */
	size_t nbytes,		/* alignment -- for objects on the heap. */
	size_t nprivate,	/* nprivate -- Size of private space */
	int alignment		/* alignment restriction for allocated chunks */
	);

gdk_export var_t HEAP_malloc(Heap *heap, size_t nbytes);
gdk_export void HEAP_free(Heap *heap, var_t block);

/*
 * @- BAT construction
 * @multitable @columnfractions 0.08 0.7
 * @item @code{BAT* }
 * @tab COLnew (oid headseq, int tailtype, BUN cap, role_t role)
 * @item @code{BAT* }
 * @tab BATextend (BAT *b, BUN newcap)
 * @end multitable
 *
 * A temporary BAT is instantiated using COLnew with the type aliases
 * of the required binary association. The aliases include the
 * built-in types, such as TYPE_int....TYPE_ptr, and the atomic types
 * introduced by the user. The initial capacity to be accommodated
 * within a BAT is indicated by cap.  Their extend is automatically
 * incremented upon storage overflow.  Failure to create the BAT
 * results in a NULL pointer.
 *
 * The routine BATclone creates an empty BAT storage area with the
 * properties inherited from its argument.
 */
#define BATDELETE	(-9999)

gdk_export BAT *COLnew(oid hseq, int tltype, BUN capacity, role_t role)
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATdense(oid hseq, oid tseq, BUN cnt)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATextend(BAT *b, BUN newcap)
	__attribute__((__warn_unused_result__));

/* internal */
gdk_export uint8_t ATOMelmshift(int sz);

#define BATttype(b)	(BATtdense(b) ? (int8_t) TYPE_oid : (b)->ttype)
#define Tbase(b)	((b)->tvheap->base)

#define Tsize(b)	((b)->twidth)

#define tailsize(b,p)	((b)->ttype?((size_t)(p))<<(b)->tshift:0)

#define Tloc(b,p)	((void *)((b)->theap.base+(((size_t)(p))<<(b)->tshift)))

typedef var_t stridx_t;
#define SIZEOF_STRIDX_T SIZEOF_VAR_T
#define GDK_VARALIGN SIZEOF_STRIDX_T

#if SIZEOF_VAR_T == 8
#define VarHeapValRaw(b,p,w)						\
	((w) == 1 ? (var_t) ((uint8_t *) (b))[p] + GDK_VAROFFSET :	\
	 (w) == 2 ? (var_t) ((uint16_t *) (b))[p] + GDK_VAROFFSET :	\
	 (w) == 4 ? (var_t) ((uint32_t *) (b))[p] :			\
	 ((var_t *) (b))[p])
#else
#define VarHeapValRaw(b,p,w)						\
	((w) == 1 ? (var_t) ((uint8_t *) (b))[p] + GDK_VAROFFSET :	\
	 (w) == 2 ? (var_t) ((uint16_t *) (b))[p] + GDK_VAROFFSET :	\
	 ((var_t *) (b))[p])
#endif
#define VarHeapVal(b,p,w) ((size_t) VarHeapValRaw(b,p,w))
#define BUNtvaroff(bi,p) VarHeapVal((bi).b->theap.base, (p), (bi).b->twidth)

#define BUNtloc(bi,p)	Tloc((bi).b,p)
#define BUNtpos(bi,p)	Tpos(&(bi),p)
#define BUNtvar(bi,p)	(assert((bi).b->ttype && (bi).b->tvarsized), (void *) (Tbase((bi).b)+BUNtvaroff(bi,p)))
#define BUNtail(bi,p)	((bi).b->ttype?(bi).b->tvarsized?BUNtvar(bi,p):BUNtloc(bi,p):BUNtpos(bi,p))

/* return the oid value at BUN position p from the (v)oid bat b
 * works with any TYPE_void or TYPE_oid bat */
#define BUNtoid(b,p)	(assert(ATOMtype((b)->ttype) == TYPE_oid),	\
			 (is_oid_nil((b)->tseqbase)			\
			  ? ((b)->ttype == TYPE_void			\
			     ? (void) (p), oid_nil			\
			     : ((const oid *) (b)->theap.base)[p])	\
			  : (oid) ((b)->tseqbase + (BUN) (p))))

static inline BATiter
bat_iterator(BAT *b)
{
	BATiter bi;

	bi.b = b;
	bi.tvid = 0;
	return bi;
}

#define BUNlast(b)	(assert((b)->batCount <= BUN_MAX), (b)->batCount)

#define BATcount(b)	((b)->batCount)

/*
 * @- BAT properties
 * @multitable @columnfractions 0.08 0.7
 * @item BUN
 * @tab BATcount (BAT *b)
 * @item void
 * @tab BATsetcapacity (BAT *b, BUN cnt)
 * @item void
 * @tab BATsetcount (BAT *b, BUN cnt)
 * @item BAT *
 * @tab BATkey (BAT *b, bool onoff)
 * @item BAT *
 * @tab BATmode (BAT *b, bool transient)
 * @item BAT *
 * @tab BATsetaccess (BAT *b, restrict_t mode)
 * @item int
 * @tab BATdirty (BAT *b)
 * @item restrict_t
 * @tab BATgetaccess (BAT *b)
 * @end multitable
 *
 * The function BATcount returns the number of associations stored in
 * the BAT.
 *
 * The BAT is given a new logical name using BBPrename.
 *
 * The integrity properties to be maintained for the BAT are
 * controlled separately.  A key property indicates that duplicates in
 * the association dimension are not permitted.
 *
 * The persistency indicator tells the retention period of BATs.  The
 * system support two modes: PERSISTENT and TRANSIENT.
 * The PERSISTENT BATs are automatically saved upon session boundary
 * or transaction commit.  TRANSIENT BATs are removed upon transaction
 * boundary.  All BATs are initially TRANSIENT unless their mode is
 * changed using the routine BATmode.
 *
 * The BAT properties may be changed at any time using BATkey
 * and BATmode.
 *
 * Valid BAT access properties can be set with BATsetaccess and
 * BATgetaccess: BAT_READ, BAT_APPEND, and BAT_WRITE.  BATs can be
 * designated to be read-only. In this case some memory optimizations
 * may be made (slice and fragment bats can point to stable subsets of
 * a parent bat).  A special mode is append-only. It is then allowed
 * to insert BUNs at the end of the BAT, but not to modify anything
 * that already was in there.
 */
gdk_export BUN BATcount_no_nil(BAT *b);
gdk_export void BATsetcapacity(BAT *b, BUN cnt);
gdk_export void BATsetcount(BAT *b, BUN cnt);
gdk_export BUN BATgrows(BAT *b);
gdk_export gdk_return BATkey(BAT *b, bool onoff);
gdk_export gdk_return BATmode(BAT *b, bool transient);
gdk_export gdk_return BATroles(BAT *b, const char *tnme);
gdk_export void BAThseqbase(BAT *b, oid o);
gdk_export void BATtseqbase(BAT *b, oid o);

/*
 * TODO: Try to get rid of the following!
 * We should only have read-only BATs with the reader library
 */

/* The batRestricted field indicates whether a BAT is readonly.
 * we have modes: BAT_WRITE  = all permitted
 *                BAT_APPEND = append-only
 *                BAT_READ   = read-only
 * VIEW bats are always mapped read-only.
 */
typedef enum {
	BAT_WRITE,		  /* all kinds of access allowed */
	BAT_READ,		  /* only read-access allowed */
	BAT_APPEND,		  /* only reads and appends allowed */
} restrict_t;

gdk_export gdk_return BATsetaccess(BAT *b, restrict_t mode);
gdk_export restrict_t BATgetaccess(BAT *b);


#define BATdirty(b)	(!(b)->batCopiedtodisk ||			\
			 (b)->batDirtydesc ||				\
			 (b)->theap.dirty ||				\
			 ((b)->tvheap != NULL && (b)->tvheap->dirty))

#define BATcapacity(b)	(b)->batCapacity

/* weirdly-missing stuff here: BATclear, COLcopy, BATgroup, BATmsync */

#define NOFARM (-1) /* indicate to GDKfilepath to create relative path */

gdk_export char * GDKfilepath(const char* farm_dir, const char *dir, const char *name, const char *ext);

#define BATtordered(b)	((b)->tsorted)
#define BATtrevordered(b) ((b)->trevsorted)
/* BAT is dense (i.e., BATtvoid() is true and tseqbase is not NIL) */
#define BATtdense(b)	(!is_oid_nil((b)->tseqbase))
/* BATtvoid: BAT can be (or actually is) represented by TYPE_void */
#define BATtvoid(b)	(BATtdense(b) || (b)->ttype==TYPE_void)
#define BATtkey(b)	((b)->tkey || BATtdense(b))

/*
 * @+ BAT Buffer Pool
 * @multitable @columnfractions 0.08 0.7
 * @item int
 * @tab BBPfix (bat bi)
 * @item int
 * @tab BBPunfix (bat bi)
 * @item int
 * @tab BBPretain (bat bi)
 * @item int
 * @tab BBPrelease (bat bi)
 * @item str
 * @tab BBPname (bat bi)
 * @item bat
 * @tab BBPindex  (str nme)
 * @item BAT*
 * @tab BATdescriptor (bat bi)
 * @end multitable
 *
 * The BAT Buffer Pool module contains the code to manage the storage
 * location of BATs.
 *
 * The remaining BBP tables contain status information to load, swap
 * and migrate the BATs. The core table is BBPcache which contains a
 * pointer to the BAT descriptor with its heaps.  A zero entry means
 * that the file resides on disk. Otherwise it has been read or mapped
 * into memory.
 *
 * BATs loaded into memory are retained in a BAT buffer pool.  They
 * retain their position within the cache during their life cycle,
 * which make indexing BATs a stable operation.
 *
 * The BBPindex routine checks if a BAT with a certain name is
 * registered in the buffer pools. If so, it returns its BAT id.  The
 * BATdescriptor routine has a BAT id parameter, and returns a pointer
 * to the corresponding BAT record (after incrementing the reference
 * count). The BAT will be loaded into memory, if necessary.
 *
 * The structure of the BBP file obeys the tuple format for GDK.
 *
 * The status and BAT persistency information is encoded in the status
 * field.
 */
typedef struct {
	BAT *cache;		/* if loaded: BAT* handle */
	char *logical;		/* logical name (may point at bak) */
	char bak[16];		/* logical name backup (tmp_%o) */
	bat next;		/* next BBP slot in linked list */
	BAT *desc;		/* the BAT descriptor */
	char physical[20];	/* dir + basename for storage */
	str options;		/* A string list of options */
	int refs;		/* in-memory references on which the loaded status of a BAT relies */
	int lrefs;		/* logical references on which the existence of a BAT relies */
	volatile unsigned status; /* status mask used for spin locking */
	/* MT_Id pid;           non-zero thread-id if this BAT is private */
} BBPrec;

#define N_BBPINIT	1000
#if SIZEOF_VOID_P == 4
#define BBPINITLOG	11
#else
#define BBPINITLOG	14
#endif
#define BBPINIT		(1 << BBPINITLOG)

typedef struct BBP {
	/*
	 * absolute maximum number of BATs is N_BBPINIT * BBPINIT
	 * this also gives the longest possible "physical" name and "bak" name
	 * of a BAT: the "bak" name is "tmp_%o", so at most 12 + \0 bytes on
	 * 64 bit architecture and 11 + \0 on 32 bit architecture; the
	 * physical name is a bit more complicated, but the longest possible
	 * name is 17 + \0 bytes (16 + \0 on 32 bits) */
	BBPrec* pool[N_BBPINIT];
	const char* directory; /* formerly bbp_farm_dir */
	bat limit; /* current committed VM BBP array - formerly BBPlimit */
	size_t size; /* current used size of BBP array - formerly BBPsize */
	int swapped_in;		/* BATs swapped into BBP - formerly BBPin */
	int swapped_out;		/* BATs swapped out of BBP - formerly BBPout*/
	int version; /* The value we find as GDK version in BBP.dir; typically printed in octal */
} BBP;

/* fast defines without checks; internal use only  */
#define BBP_cache(bbp, i)     bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].cache
#define BBP_logical(bbp, i)   bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].logical
#define BBP_bak(bbp, i)       bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].bak
#define BBP_next(bbp, i)      bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].next
#define BBP_physical(bbp, i)  bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].physical
#define BBP_options(bbp, i)   bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].options
#define BBP_desc(bbp, i)      bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].desc
#define BBP_refs(bbp, i)      bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].refs
#define BBP_lrefs(bbp, i)     bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].lrefs
#define BBP_status(bbp, i)    bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].status
#define BBP_pid(bbp, i)	      bbp->pool[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].pid

/* macros that nicely check parameters */
#define BBPname(bbp, i)						\
	(BBPcheck(bbp, (i), "BBPname") ?				\
	 bbp->pool[(i) >> BBPINITLOG][(i) & (BBPINIT - 1)].logical :	\
	 "")
#define BBPvalid(bbp, i)	(BBP_logical(bbp, i) != NULL && *BBP_logical(bbp, i) != '.')
#define BATgetId(bbp, b)	BBPname(bbp, (b)->batCacheid)

#define BBPRENAME_ALREADY	(-1)
#define BBPRENAME_ILLEGAL	(-2)
#define BBPRENAME_LONG		(-3)

gdk_export void BBPlock(BBP* bbp);

gdk_export void BBPunlock(BBP* bbp);

/* 
 * TODO: The following is currently unused, but it can be used
 * to only read the descriptors, not the column data - and only
 * load _that_ when actuall requested.
 * What it does is only load the descriptor from a file.
 */
gdk_export BAT *BBPquickdesc(BBP* bbp, bat b, bool delaccess);

/* Even though we don't use streams, we need them forward-declared. */
struct stream;
typedef struct stream stream;

/*
 * @+ GDK Extensibility
 * GDK can be extended with new atoms, search accelerators and storage
 * modes.
 *
 * @- Atomic Type Descriptors
 * The atomic types over which the binary associations are maintained
 * are described by an atom descriptor.
 *  @multitable @columnfractions 0.08 0.7
 * @item void
 * @tab ATOMallocate    (str   nme);
 * @item int
 * @tab ATOMindex       (char *nme);
 * @item int
 * @tab ATOMdump        ();
 * @item void
 * @tab ATOMdelete      (int id);
 * @item str
 * @tab ATOMname        (int id);
 * @item unsigned int
 * @tab ATOMsize        (int id);
 * @item int
 * @tab ATOMvarsized    (int id);
 * @item ptr
 * @tab ATOMnilptr      (int id);
 * @item ssize_t
 * @tab ATOMfromstr     (int id, str s, size_t* len, ptr* v_dst);
 * @item ssize_t
 * @tab ATOMtostr       (int id, str s, size_t* len, ptr* v_dst);
 * @item hash_t
 * @tab ATOMhash        (int id, ptr val, in mask);
 * @item int
 * @tab ATOMcmp         (int id, ptr val_1, ptr val_2);
 * @item int
 * @tab ATOMfix         (int id, ptr v);
 * @item int
 * @tab ATOMunfix       (int id, ptr v);
 * @item int
 * @tab ATOMheap        (int id, Heap *hp, size_t cap);
 * @item int
 * @tab ATOMput         (int id, Heap *hp, BUN pos_dst, ptr val_src);
 * @item int
 * @tab ATOMdel         (int id, Heap *hp, BUN v_src);
 * @item size_t
 * @tab ATOMlen         (int id, ptr val);
 * @item ptr
 * @tab ATOMnil         (int id);
 * @item ssize_t
 * @tab ATOMformat      (int id, ptr val, char** buf);
 * @item int
 * @tab ATOMprint       (int id, ptr val, stream *fd);
 * @item ptr
 * @tab ATOMdup         (int id, ptr val );
 * @end multitable
 *
 * @- Atom Definition
 * User defined atomic types can be added to a running system with the
 * following interface:.
 *
 * @itemize
 * @item @emph{ATOMallocate()} registers a new atom definition if
 * there is no atom registered yet under that name.
 *
 * @item @emph{ATOMdelete()} unregisters an atom definition.
 *
 * @item @emph{ATOMindex()} looks up the atom descriptor with a certain name.
 * @end itemize
 *
 * @- Atom Manipulation
 *
 * @itemize
 * @item The @emph{ATOMname()} operation retrieves the name of an atom
 * using its id.
 *
 * @item The @emph{ATOMsize()} operation returns the atoms fixed size.
 *
 * @item The @emph{ATOMnilptr()} operation returns a pointer to the
 * nil-value of an atom. We usually take one dedicated value halfway
 * down the negative extreme of the atom range (if such a concept
 * fits), as the nil value.
 *
 * @item The @emph{ATOMnil()} operation returns a copy of the nil
 * value, allocated with GDKmalloc().
 *
 * @item The @emph{ATOMheap()} operation creates a new var-sized atom
 * heap in 'hp' with capacity 'cap'.
 *
 * @item The @emph{ATOMhash()} computes a hash index for a
 * value. `val' is a direct pointer to the atom value. Its return
 * value should be an hash_t between 0 and 'mask'.
 *
 * @item The @emph{ATOMcmp()} operation compares two atomic
 * values. Its parameters are pointers to atomic values.
 *
 * @item The @emph{ATOMlen()} operation computes the byte length for a
 * value.  `val' is a direct pointer to the atom value. Its return
 * value should be an integer between 0 and 'mask'.
 *
 * @item The @emph{ATOMdel()} operation deletes a var-sized atom from
 * its heap `hp'.  The integer byte-index of this value in the heap is
 * pointed to by `val_src'.
 *
 * @item The @emph{ATOMput()} operation inserts an atom `src_val' in a
 * BUN at `dst_pos'. This involves copying the fixed sized part in the
 * BUN. In case of a var-sized atom, this fixed sized part is an
 * integer byte-index into a heap of var-sized atoms. The atom is then
 * also copied into that heap `hp'.
 *
 * @item The @emph{ATOMfix()} and @emph{ATOMunfix()} operations do
 * bookkeeping on the number of references that a GDK application
 * maintains to the atom.  In MonetDB, we use this to count the number
 * of references directly, or through BATs that have columns of these
 * atoms. The only operator for which this is currently relevant is
 * BAT. The operators return the POST reference count to the
 * atom. BATs with fixable atoms may not be stored persistently.
 *
 * @item The @emph{ATOMfromstr()} parses an atom value from string
 * `s'. The memory allocation policy is the same as in
 * @emph{ATOMget()}. The return value is the number of parsed
 * characters or -1 on failure.  Also in case of failure, the output
 * parameter buf is a valid pointer or NULL.
 *
 * @item The @emph{ATOMprint()} prints an ASCII description of the
 * atom value pointed to by `val' on file descriptor `fd'. The return
 * value is the number of parsed characters.
 *
 * @item The @emph{ATOMformat()} is similar to @emph{ATOMprint()}. It
 * prints an atom on a newly allocated string. It must later be freed
 * with @strong{GDKfree}.  The number of characters written is
 * returned. This is minimally the size of the allocated buffer.
 *
 * @item The @emph{ATOMdup()} makes a copy of the given atom. The
 * storage needed for this is allocated and should be removed by the
 * user.
 * @end itemize
 *
 * These wrapper functions correspond closely to the interface
 * functions one has to provide for a user-defined atom. They
 * basically (with exception of @emph{ATOMput()}, @emph{ATOMprint()}
 * and @emph{ATOMformat()}) just have the atom id parameter prepended
 * to them.
 */

/* atomFromStr returns the number of bytes of the input string that
 * were processed.  atomToStr returns the length of the string
 * produced.  Both functions return -1 on (any kind of) failure.  If
 * *dst is not NULL, *len specifies the available space.  If there is
 * not enough space, or if *dst is NULL, *dst will be freed (if not
 * NULL) and a new buffer will be allocated and returned in *dst.
 * *len will be set to reflect the actual size allocated.  If
 * allocation fails, *dst will be NULL on return and *len is
 * undefined.  In any case, if the function returns, *buf is either
 * NULL or a valid pointer and then *len is the size of the area *buf
 * points to. */

typedef struct {
	/* simple attributes */
	char name[IDLENGTH];
	uint8_t storage;	/* stored as another type? */
	bool linear;		/* atom can be ordered linearly */
	uint16_t size;		/* fixed size of atom */

	/* automatically generated fields */
	const void *atomNull;	/* global nil value */

	/* generic (fixed + varsized atom) ADT functions */
	ssize_t (*atomFromStr) (const char *src, size_t *len, void **dst, bool external);
	ssize_t (*atomToStr) (char **dst, size_t *len, const void *src, bool external);
	void *(*atomRead) (void *dst, stream *s, size_t cnt);
	gdk_return (*atomWrite) (const void *src, stream *s, size_t cnt);
	int (*atomCmp) (const void *v1, const void *v2);
	BUN (*atomHash) (const void *v);
	/* optional functions */
	int (*atomFix) (const void *atom);
	int (*atomUnfix) (const void *atom);

	/* varsized atom-only ADT functions */
	var_t (*atomPut) (Heap *, var_t *off, const void *src);
	void (*atomDel) (Heap *, var_t *atom);
	size_t (*atomLen) (const void *atom);
	void (*atomHeap) (Heap *, size_t);
} atomDesc;

gdk_export atomDesc BATatoms[];
gdk_export int GDKatomcnt;

gdk_export int ATOMallocate(const char *nme);
gdk_export int ATOMindex(const char *nme);

/*
 * @- Multilevel Storage Modes
 *
 * We should bring in the compressed mode as the first, maybe
 * built-in, mode. We could then add for instance HTTP remote storage,
 * SQL storage, and READONLY (cd-rom) storage.
 *
 * @+ GDK Utilities
 * Interfaces for memory management, error handling, thread management
 * and system information.
 *
 * @- GDK memory management
 * @multitable @columnfractions 0.08 0.8
 * @item void*
 * @tab GDKmalloc (size_t size)
 * @item void*
 * @tab GDKzalloc (size_t size)
 * @item void*
 * @tab GDKmallocmax (size_t size, size_t *maxsize, int emergency)
 * @item void*
 * @tab GDKrealloc (void* pold, size_t size)
 * @item void*
 * @tab GDKreallocmax (void* pold, size_t size, size_t *maxsize, int emergency)
 * @item void
 * @tab GDKfree (void* blk)
 * @item str
 * @tab GDKstrdup (str s)
 * @item str
 * @tab GDKstrndup (str s, size_t n)
 * @end multitable
 *
 * These utilities are primarily used to maintain control over
 * critical interfaces to the C library.  Moreover, the statistic
 * routines help in identifying performance and bottlenecks in the
 * current implementation.
 *
 * Compiled with -DMEMLEAKS the GDK memory management log their
 * activities, and are checked on inconsistent frees and memory leaks.
 */

/* we prefer to use vm_alloc routines on size > GDKmmap */
gdk_export void *GDKmmap(const char *path, int mode, size_t len);

gdk_export size_t GDK_mem_maxsize;	/* max allowed size of committed memory */
gdk_export size_t GDK_vm_maxsize;	/* max allowed size of reserved vm */

gdk_export void *GDKmalloc(size_t size)
	__attribute__((__malloc__))
	__attribute__((__alloc_size__(1)))
	__attribute__((__warn_unused_result__));
gdk_export void *GDKzalloc(size_t size)
	__attribute__((__malloc__))
	__attribute__((__alloc_size__(1)))
	__attribute__((__warn_unused_result__));
gdk_export void *GDKrealloc(void *pold, size_t size)
	__attribute__((__alloc_size__(2)))
	__attribute__((__warn_unused_result__));
gdk_export void GDKfree(void *blk);
gdk_export str GDKstrdup(const char *s)
	__attribute__((__warn_unused_result__));
gdk_export str GDKstrndup(const char *s, size_t n)
	__attribute__((__warn_unused_result__));

#if !defined(NDEBUG) && !defined(STATIC_CODE_ANALYSIS)
/* In debugging mode, replace GDKmalloc and other functions with a
 * version that optionally prints calling information.
 *
 * We have two versions of this code: one using a GNU C extension, and
 * one using traditional C.  The GNU C version also prints the name of
 * the calling function.
 */
#ifdef __GNUC__
#define GDKmalloc(s)						\
	({							\
		size_t _size = (s);				\
		void *_res = GDKmalloc(_size);			\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#GDKmalloc(%zu) -> %p"		\
				" %s[%s:%d]\n",			\
				_size, _res,			\
				__func__, __FILE__, __LINE__);	\
		_res;						\
	})
#define GDKzalloc(s)						\
	({							\
		size_t _size = (s);				\
		void *_res = GDKzalloc(_size);			\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#GDKzalloc(%zu) -> %p"		\
				" %s[%s:%d]\n",			\
				_size, _res,			\
				__func__, __FILE__, __LINE__);	\
		_res;						\
	})
#define GDKrealloc(p, s)					\
	({							\
		void *_ptr = (p);				\
		size_t _size = (s);				\
		void *_res = GDKrealloc(_ptr, _size);		\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#GDKrealloc(%p,%zu) -> %p"	\
				" %s[%s:%d]\n",			\
				_ptr, _size, _res,		\
				__func__, __FILE__, __LINE__);	\
		_res;						\
	 })
#define GDKfree(p)						\
	({							\
		void *_ptr = (p);				\
		ALLOCDEBUG if (_ptr)				\
			fprintf(stderr,				\
				"#GDKfree(%p)"			\
				" %s[%s:%d]\n",			\
				_ptr,				\
				__func__, __FILE__, __LINE__);	\
		GDKfree(_ptr);					\
	})
#define GDKstrdup(s)						\
	({							\
		const char *_str = (s);				\
		void *_res = GDKstrdup(_str);			\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#GDKstrdup(len=%zu) -> %p"	\
				" %s[%s:%d]\n",			\
				_str ? strlen(_str) : 0,	\
				_res,				\
				__func__, __FILE__, __LINE__);	\
		_res;						\
	})
#define GDKstrndup(s, n)					\
	({							\
		const char *_str = (s);				\
		size_t _n = (n);				\
		void *_res = GDKstrndup(_str, _n);		\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#GDKstrndup(len=%zu) -> %p"	\
				" %s[%s:%d]\n",			\
				_n,				\
				_res,				\
				__func__, __FILE__, __LINE__);	\
		_res;						\
	})
#define GDKmmap(p, m, l)						\
	({								\
		const char *_path = (p);				\
		int _mode = (m);					\
		size_t _len = (l);					\
		void *_res = GDKmmap(_path, _mode, _len);		\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKmmap(%s,0x%x,%zu) -> %p"		\
				" %s[%s:%d]\n",				\
				_path ? _path : "NULL",			\
				(unsigned) _mode, _len,			\
				_res,					\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	 })
#define malloc(s)						\
	({							\
		size_t _size = (s);				\
		void *_res = malloc(_size);			\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#malloc(%zu) -> %p"		\
				" %s[%s:%d]\n",			\
				_size, _res,			\
				__func__, __FILE__, __LINE__);	\
		_res;						\
	})
#define calloc(n, s)						\
	({							\
		size_t _nmemb = (n);				\
		size_t _size = (s);				\
		void *_res = calloc(_nmemb,_size);		\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#calloc(%zu,%zu) -> %p"	\
				" %s[%s:%d]\n",			\
				_nmemb, _size, _res,		\
				__func__, __FILE__, __LINE__);	\
		_res;						\
	})
#define realloc(p, s)						\
	({							\
		void *_ptr = (p);				\
		size_t _size = (s);				\
		void *_res = realloc(_ptr, _size);		\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#realloc(%p,%zu) -> %p"	\
				" %s[%s:%d]\n",			\
				_ptr, _size, _res,		\
				__func__, __FILE__, __LINE__);	\
		_res;						\
	 })
#define free(p)							\
	({							\
		void *_ptr = (p);				\
		ALLOCDEBUG					\
			fprintf(stderr,				\
				"#free(%p)"			\
				" %s[%s:%d]\n",			\
				_ptr,				\
				__func__, __FILE__, __LINE__);	\
		free(_ptr);					\
	})
#else
static inline void *
GDKmalloc_debug(size_t size, const char *filename, int lineno)
{
	void *res = GDKmalloc(size);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKmalloc(%zu) -> %p [%s:%d]\n",
			   size, res, filename, lineno);
	return res;
}
#define GDKmalloc(s)	GDKmalloc_debug((s), __FILE__, __LINE__)
static inline void *
GDKzalloc_debug(size_t size, const char *filename, int lineno)
{
	void *res = GDKzalloc(size);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKzalloc(%zu) -> %p [%s:%d]\n",
			   size, res, filename, lineno);
	return res;
}
#define GDKzalloc(s)	GDKzalloc_debug((s), __FILE__, __LINE__)
static inline void *
GDKrealloc_debug(void *ptr, size_t size, const char *filename, int lineno)
{
	void *res = GDKrealloc(ptr, size);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKrealloc(%p,%zu) -> "
			   "%p [%s:%d]\n",
			   ptr, size, res,
			   filename, lineno);
	return res;
}
#define GDKrealloc(p, s)	GDKrealloc_debug((p), (s), __FILE__, __LINE__)
static inline void
GDKfree_debug(void *ptr, const char *filename, int lineno)
{
	ALLOCDEBUG fprintf(stderr, "#GDKfree(%p) [%s:%d]\n",
			   ptr, filename, lineno);
	GDKfree(ptr);
}
#define GDKfree(p)	GDKfree_debug((p), __FILE__, __LINE__)
static inline char *
GDKstrdup_debug(const char *str, const char *filename, int lineno)
{
	void *res = GDKstrdup(str);
	ALLOCDEBUG fprintf(stderr, "#GDKstrdup(len=%zu) -> "
			   "%p [%s:%d]\n",
			   str ? strlen(str) : 0, res, filename, lineno);
	return res;
}
#define GDKstrdup(s)	GDKstrdup_debug((s), __FILE__, __LINE__)
static inline char *
GDKstrndup_debug(const char *str, size_t n, const char *filename, int lineno)
{
	void *res = GDKstrndup(str, n);
	ALLOCDEBUG fprintf(stderr, "#GDKstrndup(len=%zu) -> "
			   "%p [%s:%d]\n",
			   n, res, filename, lineno);
	return res;
}
#define GDKstrndup(s, n)	GDKstrndup_debug((s), (n), __FILE__, __LINE__)
static inline void *
GDKmmap_debug(const char *path, int mode, size_t len, const char *filename, int lineno)
{
	void *res = GDKmmap(path, mode, len);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKmmap(%s,0x%x,%zu) -> "
			   "%p [%s:%d]\n",
			   path ? path : "NULL", mode, len,
			   res, filename, lineno);
	return res;
}
#define GDKmmap(p, m, l)	GDKmmap_debug((p), (m), (l), __FILE__, __LINE__)
static inline void *
malloc_debug(size_t size, const char *filename, int lineno)
{
	void *res = malloc(size);
	ALLOCDEBUG fprintf(stderr,
			   "#malloc(%zu) -> %p [%s:%d]\n",
			   size, res, filename, lineno);
	return res;
}
#define malloc(s)	malloc_debug((s), __FILE__, __LINE__)
static inline void *
calloc_debug(size_t nmemb, size_t size, const char *filename, int lineno)
{
	void *res = calloc(nmemb, size);
	ALLOCDEBUG fprintf(stderr,
			   "#calloc(%zu,%zu) -> "
			   "%p [%s:%d]\n",
			   nmemb, size, res, filename, lineno);
	return res;
}
#define calloc(n, s)	calloc_debug((n), (s), __FILE__, __LINE__)
static inline void *
realloc_debug(void *ptr, size_t size, const char *filename, int lineno)
{
	void *res = realloc(ptr, size);
	ALLOCDEBUG fprintf(stderr,
			   "#realloc(%p,%zu) -> "
			   "%p [%s:%d]\n",
			   ptr, size, res,
			   filename, lineno);
	return res;
}
#define realloc(p, s)	realloc_debug((p), (s), __FILE__, __LINE__)
static inline void
free_debug(void *ptr, const char *filename, int lineno)
{
	ALLOCDEBUG fprintf(stderr, "#free(%p) [%s:%d]\n",
			   ptr, filename, lineno);
	free(ptr);
}
#define free(p)	free_debug((p), __FILE__, __LINE__)
#endif
#endif

/*
 * @- GDK error handling
 *  @multitable @columnfractions 0.08 0.7
 * @item str
 * @tab
 *  GDKmessage
 * @item bit
 * @tab
 *  GDKfatal(str msg)
 * @item int
 * @tab
 *  GDKwarning(str msg)
 * @item int
 * @tab
 *  GDKerror (str msg)
 * @item int
 * @tab
 *  GDKgoterrors ()
 * @item int
 * @tab
 *  GDKsyserror (str msg)
 * @item str
 * @tab
 *  GDKerrbuf
 *  @item
 * @tab GDKsetbuf (str buf)
 * @end multitable
 *
 * The error handling mechanism is not sophisticated yet. Experience
 * should show if this mechanism is sufficient.  Most routines return
 * a pointer with zero to indicate an error.
 *
 * The error messages are also copied to standard output.  The last
 * error message is kept around in a global variable.
 *
 * Error messages can also be collected in a user-provided buffer,
 * instead of being echoed to a stream. This is a thread-specific
 * issue; you want to decide on the error mechanism on a
 * thread-specific basis.  This effect is established with
 * GDKsetbuf. The memory (de)allocation of this buffer, that must at
 * least be 1024 chars long, is entirely by the user. A pointer to
 * this buffer is kept in the pseudo-variable GDKerrbuf. Normally,
 * this is a NULL pointer.
 */
#define GDKMAXERRLEN	10240
#define GDKWARNING	"!WARNING: "
#define GDKERROR	"!ERROR: "
#define GDKMESSAGE	"!OS: "
#define GDKFATAL	"!FATAL: "

/* Data Distilleries uses ICU for internationalization of some MonetDB error messages */

gdk_export void GDKerror(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
gdk_export void GDKsyserror(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
gdk_export void GDKfatal(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));

#include "gdk_atoms.h"
#include "gdk_bbp.h"
#include "gdk_utils.h"


#ifdef NATIVE_WIN32
#ifdef _MSC_VER
#define fileno _fileno
#endif
#define fdopen _fdopen
#define putenv _putenv
#endif

#ifndef GDK_NOLINK

static inline bat
BBPcheck(BBP* bbp, bat x, const char *y)
{
	if (!is_bat_nil(x)) {
		assert(x > 0);

		if (x < 0 || x >= getBBPsize(bbp) || BBP_logical(bbp, x) == NULL) {
			CHECKDEBUG fprintf(stderr,"#%s: range error %d\n", y, (int) x);
		} else {
			return x;
		}
	}
	return 0;
}

static inline BAT *
BATdescriptor(BBP* bbp, bat i)
{
	BAT *b = NULL;

	if (BBPcheck(bbp, i, "BATdescriptor")) {
		if (BBPfix(bbp, i) <= 0)
			return NULL;
		b = BBP_cache(bbp, i);
		if (b == NULL)
			b = BBPdescriptor(bbp, i);
	}
	return b;
}

static inline void *
Tpos(BATiter *bi, BUN p)
{
	bi->tvid = bi->b->tseqbase;
	if (!is_oid_nil(bi->tvid))
		bi->tvid += p;
	return (void*)&bi->tvid;
}

#endif

/* The batRestricted field indicates whether a BAT is readonly.
 * we have modes: BAT_WRITE  = all permitted
 *                BAT_APPEND = append-only
 *                BAT_READ   = read-only
 * VIEW bats are always mapped read-only.
 */


/*
 * @+ Common BAT Operations
 * Much used, but not necessarily kernel-operations on BATs.
 *
 * For each BAT we maintain its dimensions as separately accessible
 * properties. They can be used to improve query processing at higher
 * levels.
 */
enum prop_t {
	GDK_MIN_VALUE = 3,	/* smallest non-nil value in BAT */
	GDK_MAX_VALUE,		/* largest non-nil value in BAT */
};

gdk_export void PROPdestroy(BAT *b);

/*
 *
 */
#define MAXPARAMS	32

#endif /* _GDK_H_ */
