/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _GDK_BBP_H_
#define _GDK_BBP_H_

#include "gdk.h"

#define BBPLOADED	1	/* set if bat in memory */
#define BBPSWAPPED	2	/* set if dirty bat is not in memory */
#define BBPTMP		4	/* set if non-persistent bat has image on disk */

/* These 4 symbols indicate what the persistence state is of a bat.
 * - If the bat was persistent at the last commit (or at startup
 *   before the first commit), BBPEXISTING or BBPDELETED is set.
 * - If the bat is to be persistent after the next commit, BBPEXISTING
 *   or BBPNEW is set (i.e. (status&BBPPERSISTENT) != 0).
 * - If the bat was transient at the last commit (or didn't exist),
 *   BBPNEW is set, or none of these flag values is set.
 * - If the bat is to be transient at the next commit, BBPDELETED is
 *   set, or none of these flag values is set.
 * BATmode() switches between BBPDELETED and BBPEXISTING (bat was
 * persistent at last commit), or between BBPNEW and 0 (bat was
 * transient or didn't exist at last commit).
 * Committing a bat switches from BBPNEW to BBPEXISTING, or turns off
 * BBPDELETED.
 * In any case, only at most one of BBPDELETED, BBPEXISTING, and
 * BBPNEW may be set at any one time.
 *
 * In short,
 * BBPEXISTING -- bat was and should remain persistent;
 * BBPDELETED -- bat was persistent at last commit and should be transient;
 * BBPNEW -- bat was transient at last commit and should be persistent;
 * none of the above -- bat was and should remain transient.
 */
#define BBPDELETED	16	/* set if bat persistent at last commit is now transient */
#define BBPEXISTING	32	/* set if bat was already persistent at end of last commit */
#define BBPNEW		64	/* set if bat has become persistent since last commit */
#define BBPPERSISTENT	(BBPEXISTING|BBPNEW)	/* mask for currently persistent bats */

#define BBPSTATUS	127

#define BBPUNLOADING	128	/* set while we are unloading */
#define BBPLOADING	256	/* set while we are loading */
#define BBPSAVING       512	/* set while we are saving */
#define BBPRENAMED	1024	/* set when bat is renamed in this transaction */
#define BBPDELETING	2048	/* set while we are deleting (special case in module unload) */
#define BBPUNSTABLE	(BBPUNLOADING|BBPDELETING)	/* set while we are unloading */
#define BBPWAITING      (BBPUNLOADING|BBPLOADING|BBPSAVING|BBPDELETING)

#define BBPTRIM_ALL	(((size_t)1) << (sizeof(size_t)*8 - 2))	/* very large positive size_t */

gdk_export bat getBBPsize(BBP* buffer_pool); /* current occupied size of BBP array */

/* Exposed for data loading only */
BBP* BBPinit(const char* farm_dir);
void gdk_bbp_reset(BBP* buffer_pool);
void BBPexit(BBP* buffer_pool);

/* query interface */
gdk_export bat BBPindex(BBP* buffer_pool, const char *nme);
gdk_export BAT *BBPdescriptor(BBP* buffer_pool, bat b);

/* swapping interface */
gdk_export gdk_return BBPsync(BBP* buffer_pool, int cnt, bat *subcommit);
gdk_export int BBPfix(BBP* buffer_pool, bat b);
gdk_export int BBPunfix(BBP* buffer_pool, bat b);
gdk_export int BBPretain(BBP* buffer_pool, bat b);
gdk_export int BBPrelease(BBP* buffer_pool, bat b);
gdk_export void BBPkeepref(BBP* buffer_pool, bat i);
gdk_export void BBPshare(BBP* buffer_pool, bat b);

#endif /* _GDK_BBP_H_ */
