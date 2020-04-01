/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_CONSTRAINT_H
#define TIMESCALEDB_CHUNK_CONSTRAINT_H

#include <postgres.h>
#include <nodes/pg_list.h>

#include "catalog.h"
#include "hypertable.h"

typedef struct ChunkConstraint
{
	FormData_chunk_constraint fd;
} ChunkConstraint;

typedef struct ChunkConstraints
{
	MemoryContext mctx;
	int16 capacity;
	int16 num_constraints;
	int16 num_dimension_constraints;
	ChunkConstraint *constraints;
} ChunkConstraints;

#define chunk_constraints_get(cc, i) &((cc)->constraints[i])

#define is_dimension_constraint(cc) ((cc)->fd.dimension_slice_id > 0)

typedef struct Chunk Chunk;
typedef struct DimensionSlice DimensionSlice;
typedef struct Hypercube Hypercube;
typedef struct ChunkScanCtx ChunkScanCtx;

extern TSDLLEXPORT ChunkConstraints *ts_chunk_constraints_alloc(int size_hint, MemoryContext mctx);
extern ChunkConstraints *ts_chunk_constraint_scan_by_chunk_id(int32 chunk_id, Size count_hint,
															  MemoryContext mctx);
extern ChunkConstraints *ts_chunk_constraints_copy(ChunkConstraints *constraints);
extern int ts_chunk_constraint_scan_by_dimension_slice(DimensionSlice *slice, ChunkScanCtx *ctx,
													   MemoryContext mctx);
extern int ts_chunk_constraint_scan_by_dimension_slice_to_list(DimensionSlice *slice, List **list,
															   MemoryContext mctx);
extern int ts_chunk_constraint_scan_by_dimension_slice_id(int32 dimension_slice_id,
														  ChunkConstraints *ccs,
														  MemoryContext mctx);
extern int ts_chunk_constraints_add_dimension_constraints(ChunkConstraints *ccs, int32 chunk_id,
														  Hypercube *cube);
extern TSDLLEXPORT int ts_chunk_constraints_add_inheritable_constraints(ChunkConstraints *ccs,
																		int32 chunk_id,
																		Oid hypertable_oid);
extern TSDLLEXPORT void ts_chunk_constraints_insert_metadata(ChunkConstraints *ccs);
extern TSDLLEXPORT void ts_chunk_constraints_create(ChunkConstraints *ccs, Oid chunk_oid,
													int32 chunk_id, Oid hypertable_oid,
													int32 hypertable_id);
extern void ts_chunk_constraint_create_on_chunk(Chunk *chunk, Oid constraint_oid);
extern int ts_chunk_constraint_delete_by_hypertable_constraint_name(
	int32 chunk_id, char *hypertable_constraint_name, bool delete_metadata, bool drop_constraint);
extern int ts_chunk_constraint_delete_by_chunk_id(int32 chunk_id, ChunkConstraints *ccs);
extern int ts_chunk_constraint_delete_by_dimension_slice_id(int32 dimension_slice_id);
extern int ts_chunk_constraint_delete_by_constraint_name(int32 chunk_id,
														 const char *constraint_name,
														 bool delete_metadata,
														 bool drop_constraint);
extern void ts_chunk_constraint_recreate(ChunkConstraint *cc, Oid chunk_oid);
extern int ts_chunk_constraint_rename_hypertable_constraint(int32 chunk_id, const char *oldname,
															const char *newname);

extern char *
ts_chunk_constraint_get_name_from_hypertable_constraint(Oid chunk_relid,
														const char *hypertable_constraint_name);

#endif /* TIMESCALEDB_CHUNK_CONSTRAINT_H */
