/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

void
HASHfree(BAT *b)
{
	if (b) {
		if (b->thash && b->thash != (Hash *) -1) {
			if (b->thash != (Hash *) 1) {
				/* The semantics of HEAPfree have changed, and it now
				   does not reclaim the memory where the heap is at
				   (otherwise this next line would not have made sense) */
				HEAPfree(&b->thash->heap);
				GDKfree(b->thash);
				b->thash = (Hash *) 1;
			}
		} else {
			b->thash = NULL;
		}
	}
}
