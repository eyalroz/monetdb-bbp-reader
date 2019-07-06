/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

void
HASHfree(BAT *b)
{
	if (b) {
		if (b->T->hash && b->T->hash != (Hash *) -1) {
			if (b->T->hash != (Hash *) 1) {
				/* hashes are never dirty when we're only loading data */
				HEAPfree(b->T->hash->heap, 0);
				GDKfree(b->T->hash->heap);
				GDKfree(b->T->hash);
				b->T->hash = (Hash *) 1;
			}
		} else {
			b->T->hash = NULL;
		}
	}
}
