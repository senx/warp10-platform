/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_INSERT_H
#define TIMESCALEDB_HYPERTABLE_INSERT_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <foreign/fdwapi.h>

#include "hypertable.h"

typedef struct HypertableInsertPath
{
	CustomPath cpath;
} HypertableInsertPath;

typedef struct HypertableInsertState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
} HypertableInsertState;

extern void ts_hypertable_insert_fixup_tlist(Plan *plan);
extern Path *ts_hypertable_insert_path_create(PlannerInfo *root, ModifyTablePath *mtpath);

#endif /* TIMESCALEDB_HYPERTABLE_INSERT_H */
