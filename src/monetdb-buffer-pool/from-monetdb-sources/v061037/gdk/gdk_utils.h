/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _GDK_UTILS_H_
#define _GDK_UTILS_H_

#include <setjmp.h>

#if SIZEOF_VOID_P==8
#define GDK_VM_MAXSIZE	LL_CONSTANT(4398046511104)	/* :-) a 64-bit OS: 4TB */
#else
#define GDK_VM_MAXSIZE	LL_CONSTANT(1610612736)	/* :-| a 32-bit OS: 1.5GB */
#endif

#endif /* _GDK_UTILS_H_ */
