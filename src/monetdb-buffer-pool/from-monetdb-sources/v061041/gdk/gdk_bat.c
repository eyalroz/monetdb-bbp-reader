/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * @a M. L. Kersten, P. Boncz, N. Nes
 * @* BAT Module
 * In this Chapter we describe the BAT implementation in more detail.
 * The routines mentioned are primarily meant to simplify the library
 * implementation.
 *
 * @+ BAT Construction
 * BATs are implemented in several blocks of memory, prepared for disk
 * storage and easy shipment over a network.
 *
 * The BAT starts with a descriptor, which indicates the required BAT
 * library version and the BAT administration details.  In particular,
 * it describes the binary relationship maintained and the location of
 * fields required for storage.
 *
 * The general layout of the BAT in this implementation is as follows.
 * Each BAT comes with a heap for the loc-size buns and, optionally,
 * with heaps to manage the variable-sized data items of both
 * dimensions.  The buns are assumed to be stored as loc-size objects.
 * This is essentially an array of structs to store the associations.
 * The size is determined at BAT creation time using an upper bound on
 * the number of elements to be accommodated.  In case of overflow,
 * its storage space is extended automatically.
 *
 * The capacity of a BAT places an upper limit on the number of BUNs
 * to be stored initially. The actual space set aside may be quite
 * large.  Moreover, the size is aligned to int boundaries to speedup
 * access and avoid some machine limitations.
 *
 * Initialization of the variable parts rely on type specific routines
 * called atomHeap.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#ifdef ALIGN
#undef ALIGN
#endif
#define ALIGN(n,b)	((b)?(b)*(1+(((n)-1)/(b))):n)

#define ATOMneedheap(tpe) (BATatoms[tpe].atomHeap != NULL)

static char *BATstring_h = "h";
static char *BATstring_t = "t";

static inline bool
default_ident(char *s)
{
	return (s == BATstring_h || s == BATstring_t);
}

/*
 * Computes floor(log_2(sz)); the 'shift'
 * is how much you need to left-shift an index
 * into a BAT column heap to get
 * the offset in bytes into the heap.
 *
 * ... and actually, we're not expecting sz to
 * be anything other than a power of 2
 */
uint8_t
ATOMelmshift(int sz)
{
	uint8_t sh;
	int i = sz >> 1;

	for (sh = 0; i != 0; sh++) {
		i >>= 1;
	}
	return sh;
}

/* Moved from gdk_batop.c */
void
PROPdestroy(BAT *b)
{
	PROPrec *p = b->tprops;
	PROPrec *n;

	b->tprops = NULL;
	while (p) {
		n = p->next;
		/* hand-inling this:
		/* VALclear(&p->v); */
		/* into this: */
		{
			ValPtr v = &p->v;
			if (ATOMextern(v->vtype)) {
				if (v->val.pval && v->val.pval != ATOMnilptr(v->vtype))
					GDKfree(v->val.pval);
			}
			v->len = 0;
			v->val.oval = oid_nil;
			v->vtype = TYPE_void;
		}
		GDKfree(p);
		p = n;
	}
}

/* free a cached BAT; leave the bat descriptor cached */
void
BATfree(BAT *b)
{
	if (b == NULL)
		return;

	/* deallocate all memory for a bat */
	assert(b->batCacheid > 0);
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	b->tident = BATstring_t;
	PROPdestroy(b);
	HASHfree(b);
	/*
	 * No imprints and order-index when only loading data
	 */
/*	IMPSfree(b); */
/*	OIDXfree(b); */

	if (b->ttype)
		HEAPfree(&b->theap);
	else
		assert(!b->theap.base);
	if (b->tvheap) {
		assert(b->tvheap->parentid == b->batCacheid);
		HEAPfree(b->tvheap);
	}
}

/* free a cached BAT descriptor */
void
BATdestroy(BAT *b)
{
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	b->tident = BATstring_t;
	if (b->tvheap)
		GDKfree(b->tvheap);
	PROPdestroy(b);
	GDKfree(b);
}

gdk_return
BATroles(BAT *b, const char *tnme)
{
	if (b == NULL)
		return GDK_SUCCEED;
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	if (tnme)
		b->tident = GDKstrdup(tnme);
	else
		b->tident = BATstring_t;
	return b->tident ? GDK_SUCCEED : GDK_FAIL;
}

