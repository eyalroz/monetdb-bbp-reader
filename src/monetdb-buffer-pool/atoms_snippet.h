#pragma once
#ifndef ATOMS_SNIPPET_H_
#define ATOMS_SNIPPET_H_

#include "gdk_snippet.h"

/* representation of the SQL null value (which monetdb refers to as nil).
 * TODO: What about null values of other types, such as date, time and
 * so on?
 */
extern const bte bte_nil;
extern const sht sht_nil;
extern const int int_nil;
extern const flt flt_nil;
extern const dbl dbl_nil;
extern const lng lng_nil;
#ifdef HAVE_HGE
extern const hge hge_nil;
#endif
extern const oid oid_nil;
extern const wrd wrd_nil;
extern const char str_nil[2];
extern const ptr ptr_nil;

#endif /* ATOMS_SNIPPET_H_ */
