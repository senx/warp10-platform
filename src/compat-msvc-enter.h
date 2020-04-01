/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPAT_MSVC_ENTER_H
#define TIMESCALEDB_COMPAT_MSVC_ENTER_H

/*
 * Not all exported data symbols in PostgreSQL are marked with PGDLLIMPORT,
 * which causes errors during linking. This hack turns all extern symbols into
 * properly exported symbols so we can use them in our code. Only necessary
 * for files that use these incorrectly unlabeled data symbols (e.g. extension.c)
 *
 * NOTE: Applies to data symbols only, not functions
 */
#ifdef _MSC_VER
#undef PGDLLIMPORT
#define PGDLLIMPORT
#define extern extern _declspec(dllimport)
#endif /* _MSC_VER */

#endif /* TIMESCALEDB_COMPAT_MSVC_ENTER_H */
