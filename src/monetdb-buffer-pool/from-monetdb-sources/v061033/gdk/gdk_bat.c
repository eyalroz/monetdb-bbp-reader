/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

static char *BATstring_h = "h";
static char *BATstring_t = "t";

static inline int
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
bte
ATOMelmshift(int sz)
{
	bte sh;
	int i = sz >> 1;

	for (sh = 0; i != 0; sh++) {
		i >>= 1;
	}
	return sh;
}

// Moved from gdk_batop.c
void
PROPdestroy(PROPrec *p)
{
	PROPrec *n;

	while (p) {
		n = p->next;
		if (p->v.vtype == TYPE_str)
			GDKfree(p->v.val.sval);
		GDKfree(p);
		p = n;
	}
}

/* free a cached BAT; leave the bat descriptor cached */
void
BATfree(BBP* bbp, BAT *b)
{
	if (b == NULL)
		return;

	/* deallocate all memory for a bat */
	if (b->batCacheid < 0)
		b = BBP_cache(bbp, -(b->batCacheid));
	if (b->hident && !default_ident(b->hident))
		GDKfree(b->hident);
	b->hident = BATstring_h;
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	b->tident = BATstring_t;
	if (b->H->props)
		PROPdestroy(b->H->props);
	b->H->props = NULL;
	if (b->T->props)
		PROPdestroy(b->T->props);
	b->T->props = NULL;
	HASHfree(b);
//	Imprints unused when just loading data
//	IMPSfree(b);
	if (b->htype)
		HEAPfree(&b->H->heap, 0);
	else
		assert(!b->H->heap.base);
	if (b->ttype)
		HEAPfree(&b->T->heap, 0);
	else
		assert(!b->T->heap.base);
	if (b->H->vheap) {
		assert(b->H->vheap->parentid == b->batCacheid);
		HEAPfree(b->H->vheap, 0);
	}
	if (b->T->vheap) {
		assert(b->T->vheap->parentid == b->batCacheid);
		HEAPfree(b->T->vheap, 0);
	}

	b = BBP_cache(bbp, -b->batCacheid);
	if (b) {
		BBP_cache(bbp, b->batCacheid) = NULL;
	}
}

/* free a cached BAT descriptor */
void
BATdestroy( BATstore *bs )
{
	if (bs->H.id && !default_ident(bs->H.id))
		GDKfree(bs->H.id);
	bs->H.id = BATstring_h;
	if (bs->T.id && !default_ident(bs->T.id))
		GDKfree(bs->T.id);
	bs->T.id = BATstring_t;
	if (bs->H.vheap)
		GDKfree(bs->H.vheap);
	if (bs->T.vheap)
		GDKfree(bs->T.vheap);
	if (bs->H.props)
		PROPdestroy(bs->H.props);
	if (bs->T.props)
		PROPdestroy(bs->T.props);
	GDKfree(bs);
}

void
BATroles(BAT *b, const char *hnme, const char *tnme)
{
	if (b == NULL)
		return;
	if (b->hident && !default_ident(b->hident))
		GDKfree(b->hident);
	if (hnme)
		b->hident = GDKstrdup(hnme);
	else
		b->hident = BATstring_h;
	if (b->tident && !default_ident(b->tident))
		GDKfree(b->tident);
	if (tnme)
		b->tident = GDKstrdup(tnme);
	else
		b->tident = BATstring_t;
}

