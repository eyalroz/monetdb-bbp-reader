/**
 * @file a minimal set of definitions from MonetDB's gdk.h, necessary
 * (at this moment at least) for using the BBP reader library. It's
 * a bit of a chimera in the sense of being usable with multiple GDK
 * library versions.
 *
 * @note this is a C, not a C++, file
 *
 * @todo Make an effort to drop as many of the definitions here as
 * possible.
 */
#ifndef GDK_SNIPPET_H_
#define GDK_SNIPPET_H_

#if (GDK_VERSION != 061033) && \
	(GDK_VERSION != 061035) && \
	(GDK_VERSION != 061037) && \
	(GDK_VERSION != 061040) && \
	(GDK_VERSION != 061041)
#if __GNUC__
#define STRINGIFY_GDK_VERSION_HELPER(x) #x
#define STRINGIFY_GDK_VERSION(x) STRINGIFY_GDK_VERSION_HELPER(x)
#pragma message "GDK_VERSION IS: " STRINGIFY_GDK_VERSION(GDK_VERSION)
#error "Unsupported GDK_VERSION"
#endif // __GNUC__
#endif // GDK_VERSION check

#if GDK_VERSION < 0610401
typedef signed char bit;
typedef signed char bte;
typedef short sht;
#else
typedef int8_t bit;
typedef int8_t bte;
typedef int16_t sht;
#endif
#if GDK_VERSION >= 061040
typedef int64_t lng;
typedef uint64_t ulng;
#endif

#ifdef MONET_OID32
#define SIZEOF_OID	SIZEOF_INT
typedef unsigned int oid;
#else
#define SIZEOF_OID	SIZEOF_SIZE_T
typedef size_t oid;
#endif
#if SIZEOF_OID == SIZEOF_SIZE_T
#define OIDFMT		SZFMT
#else
#if SIZEOF_OID == SIZEOF_INT
#define OIDFMT		"%u"
#else
#define OIDFMT		ULLFMT
#endif
#endif

#define SIZEOF_WRD	SIZEOF_SSIZE_T
typedef ssize_t wrd;
typedef int bat;		/* Index into BBP */
typedef void *ptr;		/* Internal coding of types */

#define SIZEOF_PTR	SIZEOF_VOID_P
typedef float flt;
typedef double dbl;
typedef char *str;

typedef oid BUN;		/* BUN position */

#if GDK_VERSION >= 061041
typedef enum {
	PERSISTENT = 0,
	TRANSIENT,
} role_t;
#endif

/* Heap storage modes */
typedef enum {
	STORE_MEM     = 0,	/* load into GDKmalloced memory */
	STORE_MMAP    = 1,	/* mmap() into virtual memory */
	STORE_PRIV    = 2,	/* BAT copy of copy-on-write mmap */
#if GDK_VERSION >= 061035
	STORE_CMEM    = 3,	/* load into malloc (not GDKmalloc) memory*/
	STORE_NOWN    = 4,	/* memory not owned by the BAT */
	STORE_MMAPABS = 5,	/* mmap() into virtual memory from an
	                     * absolute path (not part of dbfarm) */
#endif
	STORE_INVALID		/* invalid value, used to indicate error */
} storage_t;

typedef struct {
	size_t free;		/* index where free area starts. */
	size_t size;		/* size of the heap (bytes) */
	char *base;		/* base pointer in memory. */
#if GDK_VERSION >= 061040
	char filename[32];	/* file containing image of the heap */
#else
	str filename;		/* file containing image of the heap */
#endif

#if GDK_VERSION >= 061041
	const char* farm_dir;	/* directory of the farm where this heap is located */
#endif
#if GDK_VERSION >= 061040
	bool
#else
	unsigned int
#endif
		copied:1	/* a copy of an existing map. */
		, hashash:1/* the string heap contains hash values */
#if GDK_VERSION <= 061040
		, forcemap:1  /* force STORE_MMAP even if heap exists */
#endif
#if GDK_VERSION >= 061040
		, cleanhash:1	/* string heaps must clean hash */
#endif
	;
	storage_t storage;	/* storage mode (mmap/malloc). */
	storage_t newstorage;	/* new desired storage mode at re-allocation. */
/*	bte farmid;		 * id of farm where heap is located */
#if GDK_VERSION < 061041
	const char* farm_dir;	/* directory of the farm where this heap is located */
#endif
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
#if GDK_VERSION >= 061040
	Heap heap;		/* heap where the hash is stored */
#else
	Heap *heap;		/* heap where the hash is stored */
#endif
} Hash;


typedef struct PROPrec PROPrec;


typedef struct {
	str id;			/* label for head/tail column */

#if GDK_VERSION <= 061040
	unsigned short width;	/* byte-width of the atom array */
	bte type;		/* type id. */
	bte shift;		/* log2 of bunwidth */
#else
	uint16_t width;		/* byte-width of the atom array */
	int8_t type;		/* type id. */
	uint8_t shift;		/* log2 of bun width */
#endif

#if GDK_VERSION >= 061040
	bool
#else
	unsigned int
#endif
	 varsized:1,		/* varsized (1) or fixedsized (0) */
#if GDK_VERSION == 061033
	 key:2,			/* duplicates allowed? */
#else
#if GDK_VERSION >= 061035
	 key:1,			/* no duplicate values present */
	 unique:1,		/* no duplicate values allowed */
#endif
#endif

#if GDK_VERSION <= 061040
	 dense:1,		/* OID only: only consecutive values */
#endif
	 nonil:1,		/* there are no nils in the column */
	 nil:1,			/* there is a nil in the column */
	 sorted:1,		/* column is sorted in ascending order */
	 revsorted:1;		/* column is sorted in descending order */
#if GDK_VERSION < 061037
	oid align;		/* OID for sync alignment */
#endif
	BUN nokey[2];		/* positions that prove key ==FALSE */
	BUN nosorted;		/* position that proves sorted==FALSE */
	BUN norevsorted;	/* position that proves revsorted==FALSE */
#if GDK_VERSION < 061040
	BUN nodense;		/* position that proves dense==FALSE */
#endif
	oid seq;		/* start of dense (head) sequence */

	Heap heap;		/* space for the column. */
	Heap *vheap;		/* space for the varsized data. */
	Hash *hash;		/* hash table */

#if GDK_VERSION >= 061035
	/* No order index (and no imprints) when only reading data */

/*	Imprints *imprints;	* column imprints index */
/*	Heap *orderidx;		* order oid index */
#endif

	PROPrec *props;		/* list of dynamic properties stored in the bat descriptor */
} COLrec;


typedef struct BATstore BATstore;

/* macros's to hide complexity of BAT structure */

#if GDK_VERSION == 061033
#define batPersistence	S->persistence
#define batCopiedtodisk	S->copiedtodisk
#define batDirty	S->dirty
#define batConvert	S->convert
#define batDirtyflushed	S->dirtyflushed
#define batDirtydesc	S->descdirty
#define batFirst	S->first
#define batCount	S->count
#define batCapacity	S->capacity
#define batSharecnt	S->sharecnt
#define batRestricted	S->restricted
#define htype		H->type
#define ttype		T->type
#define hkey		H->key
#define tkey		T->key
#define hvarsized	H->varsized
#define tvarsized	T->varsized
#define hseqbase	H->seq
#define tseqbase	T->seq
#define hsorted		H->sorted
#define hrevsorted	H->revsorted
#define tsorted		T->sorted
#define trevsorted	T->revsorted
#define hdense		H->dense
#define tdense		T->dense
#define hident		H->id
#define tident		T->id
#define halign		H->align
#define talign		T->align

#else
#if (GDK_VERSION >= 061035) && (GDK_VERSION <= 061040)

#define batPersistence	S.persistence
#define batCopiedtodisk	S.copiedtodisk
#define batDirty	S.dirty
#define batConvert	S.convert
#define batDirtyflushed	S.dirtyflushed
#define batDirtydesc	S.descdirty
#define batInserted	S.inserted
#define batCount	S.count
#define batCapacity	S.capacity
#define batSharecnt	S.sharecnt
#define batRestricted	S.restricted
#define batRole		S.role
#define creator_tid	S.tid
#define ttype		T.type
#define tkey		T.key
#define tunique		T.unique
#define tvarsized	T.varsized
#define tseqbase	T.seq
#define tsorted		T.sorted
#define trevsorted	T.revsorted
#define tdense		T.dense
#define tident		T.id
#if GDK_VERSION < 061037
#define talign		T.align
#endif
#define torderidx	T.orderidx
#define twidth		T.width
#define tshift		T.shift
#define tnonil		T.nonil
#define tnil		T.nil
#define tnokey		T.nokey
#define tnosorted	T.nosorted
#define tnorevsorted	T.norevsorted
#if GDK_VERSION < 061040
#define tnodense	T.nodense
#endif
#define theap		T.heap
#define tvheap		T.vheap
#define thash		T.hash
#define timprints	T.imprints
#define tprops		T.props

#else

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

#endif /* (GDK_VERSION >= 061035) && (GDK_VERSION <= 061040) */
#endif /* GDK_VERSION == 061033 */

typedef oid var_t;		/* type used for heap index of var-sized BAT */
#define SIZEOF_VAR_T	SIZEOF_OID

#if SIZEOF_VAR_T < SIZEOF_VOID_P
/* NEW 11/4/2009: when compiled with 32-bits oids/var_t on 64-bits
 * systems, align heap strings on 8 byte boundaries always (wasting 4
 * padding bytes on avg). Note that in heaps where duplicate
 * elimination is successful, such padding occurs anyway (as an aside,
 * a better implementation with two-bytes pointers in the string heap
 * hash table, could reduce that padding to avg 1 byte wasted -- see
 * TODO below).
 *
 * This 8 byte alignment allows the offset in the fixed part of the
 * BAT string column to be interpreted as an index, which should be
 * multiplied by 8 to get the position (VARSHIFT). The overall effect
 * is that 32GB heaps can be addressed even when oids are limited to
 * 4G tuples.
 *
 * In the future, we might extend this such that the string alignment
 * is set in the BAT header (columns with long strings take more
 * storage space, but could tolerate more padding).  It would mostly
 * work, only the sort routine and strPut/strLocate (which do not see
 * the BAT header) extra parameters would be needed in their APIs.
 */
typedef unsigned short stridx_t;
#define SIZEOF_STRIDX_T SIZEOF_SHORT
#if GDK_VERSION <= 061033
#define GDK_VARSHIFT 3
#endif /* GDK_VERSION <= 061033 */
#define GDK_VARALIGN (1<<GDK_VARSHIFT)
#else
typedef var_t stridx_t;
#define SIZEOF_STRIDX_T SIZEOF_VAR_T
#if GDK_VERSION <= 061033
#define GDK_VARSHIFT 0
#endif /* GDK_VERSION <= 061033 */
#define GDK_VARALIGN SIZEOF_STRIDX_T
#endif /* SIZEOF_VAR_T < SIZEOF_VOID_P */



/* from gdk_atoms.h */

/* string heaps:
 * - strings are 8 byte aligned
 * - start with a 1024 bucket hash table
 * - heaps < 64KB are fully duplicate eliminated with this hash tables
 * - heaps >= 64KB are opportunistically (imperfect) duplicate
 *   eliminated as only the last 128KB chunk is considered and there
 *   is no linked list
 * - buckets and next pointers are unsigned short "indices"
 * - indices should be multiplied by 8 and takes from ELIMBASE to get
 *   an offset
 * Note that a 64KB chunk of the heap contains at most 8K 8-byte
 * aligned strings. The 1K bucket list means that in worst load, the
 * list length is 8 (OK).
 */
#define GDK_STRHASHTABLE	(1<<10)	/* 1024 */
#define GDK_STRHASHMASK		(GDK_STRHASHTABLE-1)
#define GDK_STRHASHSIZE		(GDK_STRHASHTABLE * sizeof(stridx_t))
#define GDK_ELIMPOWER		16	/* 64KB is the threshold */
#define GDK_ELIMDOUBLES(h)	((h)->free < GDK_ELIMLIMIT)
#define GDK_ELIMLIMIT		(1<<GDK_ELIMPOWER)	/* equivalently: ELIMBASE == 0 */
#define GDK_ELIMBASE(x)		(((x) >> GDK_ELIMPOWER) << GDK_ELIMPOWER)
#if GDK_VERSION <= 061033
#define GDK_VAROFFSET		((var_t) (GDK_STRHASHSIZE >> GDK_VARSHIFT))
#else
#define GDK_VAROFFSET		((var_t) GDK_STRHASHSIZE)
#endif

/* and now back to gdk.h... */

#if GDK_VERSION < 061041
#if SIZEOF_VAR_T == 8
#define VarHeapValRaw(b,p,w)						\
	((w) == 1 ? (var_t) ((unsigned char *) (b))[p] + GDK_VAROFFSET : \
	 (w) == 2 ? (var_t) ((unsigned short *) (b))[p] + GDK_VAROFFSET : \
	 (w) == 4 ? (var_t) ((unsigned int *) (b))[p] :			\
	 ((var_t *) (b))[p])
#else
#define VarHeapValRaw(b,p,w)						\
	((w) == 1 ? (var_t) ((unsigned char *) (b))[p] + GDK_VAROFFSET : \
	 (w) == 2 ? (var_t) ((unsigned short *) (b))[p] + GDK_VAROFFSET : \
	 ((var_t *) (b))[p])
#endif /* SIZEOF_VAR_T == 8 */
#else
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
#endif  /* SIZEOF_VAR_T == 8 */
#endif /* GDK_VERSION < 061041 */

#if GDK_VERSION <= 061033
#define VarHeapVal(b,p,w) ((size_t) VarHeapValRaw(b,p,w)  << GDK_VARSHIFT)
#else
#define VarHeapVal(b,p,w) ((size_t) VarHeapValRaw(b,p,w))
#endif /* GDK_VERSION <= 061033 */

/*
 * gdk.h has a full definition of the following,
 * but we only need a forward declarations
 */
struct BBP;
struct BAT;

#endif /* GDK_SNIPPET_H_ */
