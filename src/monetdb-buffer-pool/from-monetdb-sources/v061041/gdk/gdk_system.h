/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#ifndef _GDK_SYSTEM_H_
#define _GDK_SYSTEM_H_

#define gdk_export extern

/* debug and errno integers */
gdk_export int GDKdebug;

/*
 * @- sleep
 */

gdk_export void MT_sleep_ms(unsigned int ms);
	/* Used by Windows code in filesystem-related code, see gdk_posix.c */

#endif /* _GDK_SYSTEM_H_ */
