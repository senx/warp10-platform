/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/* This file contains the code for processing continuous aggregate
 * DDL statements which are of the form:
 *
 * CREATE VIEW <name> WITH (ts_continuous = [option] )
 * AS  <select query>
 * The entry point for the code is
 * tsl_process_continuous_agg_viewstmt
 * The bulk of the code that creates the underlying tables/views etc. is in
 * cagg_create
 *
 */
#include <postgres.h>
#include <access/reloptions.h>
#include <access/sysattr.h>
#include <access/xact.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_trigger.h>
#include <catalog/pg_type.h>
#include <catalog/toasting.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <commands/trigger.h>
#include <commands/view.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/pg_list.h>
#include <optimizer/clauses.h>
#include <optimizer/tlist.h>
#include <parser/analyze.h>
#include <parser/parse_func.h>
#include <parser/parse_oper.h>
#include <parser/parse_relation.h>
#include <parser/parse_type.h>
#include <rewrite/rewriteHandler.h>
#include <rewrite/rewriteManip.h>
#include <utils/builtins.h>
#include <utils/catcache.h>
#include <utils/int8.h>
#include <utils/ruleutils.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "create.h"

#include "catalog.h"
#include "compat.h"
#include "continuous_agg.h"
#include "dimension.h"
#include "extension_constants.h"
#include "func_cache.h"
#include "hypertable_cache.h"
#include "hypertable.h"
#include "continuous_aggs/job.h"
#include "dimension.h"
#include "continuous_agg.h"
#include "options.h"
#include "utils.h"
#include "errors.h"

#define FINALFN "finalize_agg"
#define PARTIALFN "partialize_agg"
#define CHUNKIDFROMRELID "chunk_id_from_relid"
#define DEFAULT_MATPARTCOLUMN_NAME "time_partition_col"
#define MATPARTCOL_INTERVAL_FACTOR 10
#define HT_DEFAULT_CHUNKFN "calculate_chunk_interval"
#define CAGG_INVALIDATION_TRIGGER "continuous_agg_invalidation_trigger"
#define BOUNDARY_FUNCTION "cagg_watermark"
#define INTERNAL_TO_DATE_FUNCTION "to_date"
#define INTERNAL_TO_TSTZ_FUNCTION "to_timestamp"
#define INTERNAL_TO_TS_FUNCTION "to_timestamp_without_timezone"

#define DEFAULT_MAX_INTERVAL_MULTIPLIER 20
#define DEFAULT_MAX_INTERVAL_MAX_BUCKET_WIDTH (PG_INT64_MAX / DEFAULT_MAX_INTERVAL_MULTIPLIER)

/*switch to ts user for _timescaledb_internal access */
#define SWITCH_TO_TS_USER(schemaname, newuid, saved_uid, saved_secctx)                             \
	do                                                                                             \
	{                                                                                              \
		if (schemaname &&                                                                          \
			strncmp(schemaname, INTERNAL_SCHEMA_NAME, strlen(INTERNAL_SCHEMA_NAME)) == 0)          \
			newuid = ts_catalog_database_info_get()->owner_uid;                                    \
		else                                                                                       \
			newuid = InvalidOid;                                                                   \
		if (newuid != InvalidOid)                                                                  \
		{                                                                                          \
			GetUserIdAndSecContext(&saved_uid, &saved_secctx);                                     \
			SetUserIdAndSecContext(uid, saved_secctx | SECURITY_LOCAL_USERID_CHANGE);              \
		}                                                                                          \
	} while (0)

#define RESTORE_USER(newuid, saved_uid, saved_secctx)                                              \
	do                                                                                             \
	{                                                                                              \
		if (newuid != InvalidOid)                                                                  \
			SetUserIdAndSecContext(saved_uid, saved_secctx);                                       \
	} while (0);

#define PRINT_MATCOLNAME(colbuf, type, original_query_resno, colno)                                \
	do                                                                                             \
	{                                                                                              \
		int ret = snprintf(colbuf, NAMEDATALEN, "%s_%d_%d", type, original_query_resno, colno);    \
		if (ret < 0 || ret >= NAMEDATALEN)                                                         \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INTERNAL_ERROR),                                              \
					 errmsg("bad materialization table column name")));                            \
	} while (0);

#define PRINT_MATINTERNAL_NAME(buf, prefix, hypertable_id)                                         \
	do                                                                                             \
	{                                                                                              \
		int ret = snprintf(buf, NAMEDATALEN, prefix, hypertable_id);                               \
		if (ret < 0 || ret > NAMEDATALEN)                                                          \
		{                                                                                          \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INTERNAL_ERROR),                                              \
					 errmsg(" bad materialization internal name")));                               \
		}                                                                                          \
	} while (0);

/* Note that we set rowsecurity to false here */
#define CAGG_MAKEQUERY(selquery, srcquery)                                                         \
	do                                                                                             \
	{                                                                                              \
		selquery = makeNode(Query);                                                                \
		selquery->commandType = CMD_SELECT;                                                        \
		selquery->querySource = srcquery->querySource;                                             \
		selquery->queryId = srcquery->queryId;                                                     \
		selquery->canSetTag = srcquery->canSetTag;                                                 \
		selquery->utilityStmt = copyObject(srcquery->utilityStmt);                                 \
		selquery->resultRelation = 0;                                                              \
		selquery->hasAggs = true;                                                                  \
		selquery->hasRowSecurity = false;                                                          \
	} while (0);

typedef struct MatTableColumnInfo
{
	List *matcollist;		 /* column defns for materialization tbl*/
	List *partial_seltlist;  /* tlist entries for populating the materialization table columns */
	List *partial_grouplist; /* group clauses used for populating the materialization table */
	List *mat_groupcolname_list; /* names of columns that are populated by the group-by clause
									correspond to the partial_grouplist.
									time_bucket column is not included here: it is the
									matpartcolname */
	int matpartcolno;			 /*index of partitioning column in matcollist */
	char *matpartcolname;		 /*name of the partition column */
} MatTableColumnInfo;

typedef struct FinalizeQueryInfo
{
	List *final_seltlist;   /*select target list for finalize query */
	Node *final_havingqual; /*having qual for finalize query */
	Query *final_userquery; /* user query used to compute the finalize_query */
} FinalizeQueryInfo;

typedef struct CAggTimebucketInfo
{
	int32 htid;				/* hypertable id */
	Oid htoid;				/* hypertable oid */
	AttrNumber htpartcolno; /*primary partitioning column */
							/* This should also be the column used by time_bucket */
	Oid htpartcoltype;
	int64 htpartcol_interval_len; /* interval length setting for primary partitioning column */
	int64 bucket_width;			  /*bucket_width of time_bucket */
} CAggTimebucketInfo;

typedef struct AggPartCxt
{
	struct MatTableColumnInfo *mattblinfo;
	bool addcol;
	Oid ignore_aggoid;
	int original_query_resno;
} AggPartCxt;

/* STATIC functions defined on the structs above */
static void mattablecolumninfo_init(MatTableColumnInfo *matcolinfo, List *collist, List *tlist,
									List *grouplist);
static Var *mattablecolumninfo_addentry(MatTableColumnInfo *out, Node *input,
										int original_query_resno);
static void mattablecolumninfo_addinternal(MatTableColumnInfo *matcolinfo,
										   RangeTblEntry *usertbl_rte, int32 usertbl_htid);
static int32 mattablecolumninfo_create_materialization_table(MatTableColumnInfo *matcolinfo,
															 int32 hypertable_id, RangeVar *mat_rel,
															 CAggTimebucketInfo *origquery_tblinfo,
															 bool create_addl_index,
															 ObjectAddress *mataddress);
static Query *mattablecolumninfo_get_partial_select_query(MatTableColumnInfo *matcolinfo,
														  Query *userview_query);

static void caggtimebucketinfo_init(CAggTimebucketInfo *src, int32 hypertable_id,
									Oid hypertable_oid, AttrNumber hypertable_partition_colno,
									Oid hypertable_partition_coltype,
									int64 hypertable_partition_col_interval);
static void caggtimebucket_validate(CAggTimebucketInfo *tbinfo, List *groupClause,
									List *targetList);
static void finalizequery_init(FinalizeQueryInfo *inp, Query *orig_query,
							   MatTableColumnInfo *mattblinfo);
static Query *finalizequery_get_select_query(FinalizeQueryInfo *inp, List *matcollist,
											 ObjectAddress *mattbladdress);
static bool is_valid_bucketing_function(Oid funcid);

static Const *cagg_boundary_make_lower_bound(Oid type);
static Oid cagg_get_boundary_converter_funcoid(Oid typoid);
static Oid relation_oid(NameData schema, NameData name);
static Query *build_union_query(CAggTimebucketInfo *tbinfo, MatTableColumnInfo *mattblinfo,
								Query *q1, Query *q2);

/* create a entry for the materialization table in table CONTINUOUS_AGGS */
static void
create_cagg_catlog_entry(int32 matht_id, int32 rawht_id, char *user_schema, char *user_view,
						 char *partial_schema, char *partial_view, int64 bucket_width,
						 int64 refresh_lag, int64 max_interval_per_job,
						 int64 ignore_invalidation_older_than, int32 job_id, char *direct_schema,
						 char *direct_view)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	NameData user_schnm, user_viewnm, partial_schnm, partial_viewnm, direct_schnm, direct_viewnm;
	Datum values[Natts_continuous_agg];
	bool nulls[Natts_continuous_agg] = { false };
	CatalogSecurityContext sec_ctx;

	namestrcpy(&user_schnm, user_schema);
	namestrcpy(&user_viewnm, user_view);
	namestrcpy(&partial_schnm, partial_schema);
	namestrcpy(&partial_viewnm, partial_view);
	namestrcpy(&direct_schnm, direct_schema);
	namestrcpy(&direct_viewnm, direct_view);
	rel = heap_open(catalog_get_table_id(catalog, CONTINUOUS_AGG), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	memset(values, 0, sizeof(values));
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_mat_hypertable_id)] = matht_id;
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_raw_hypertable_id)] = rawht_id;
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_schema)] =
		NameGetDatum(&user_schnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_name)] =
		NameGetDatum(&user_viewnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_schema)] =
		NameGetDatum(&partial_schnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_name)] =
		NameGetDatum(&partial_viewnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_bucket_width)] = Int64GetDatum(bucket_width);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_job_id)] = job_id;
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_refresh_lag)] = Int64GetDatum(refresh_lag);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_schema)] =
		NameGetDatum(&direct_schnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_name)] =
		NameGetDatum(&direct_viewnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_max_interval_per_job)] =
		Int64GetDatum(max_interval_per_job);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_ignore_invalidation_older_than)] =
		Int64GetDatum(ignore_invalidation_older_than);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	heap_close(rel, RowExclusiveLock);
}

/* create hypertable for the table referred by mat_tbloid
 * matpartcolname - partition column for hypertable
 * timecol_interval - is the partitioning column's interval for hypertable partition
 */
static void
cagg_create_hypertable(int32 hypertable_id, Oid mat_tbloid, const char *matpartcolname,
					   int64 mat_tbltimecol_interval)
{
	bool created;
	int flags = 0;
	NameData mat_tbltimecol;
	DimensionInfo *time_dim_info;
	ChunkSizingInfo *chunk_sizing_info;
	namestrcpy(&mat_tbltimecol, matpartcolname);
	time_dim_info = ts_dimension_info_create_open(mat_tbloid,
												  &mat_tbltimecol,
												  Int64GetDatum(mat_tbltimecol_interval),
												  INT8OID,
												  InvalidOid);
	// TODO fix this after change in C interface
	chunk_sizing_info = ts_chunk_sizing_info_get_default_disabled(mat_tbloid);
	chunk_sizing_info->colname = matpartcolname;
	created = ts_hypertable_create_from_info(mat_tbloid,
											 hypertable_id,
											 flags,
											 time_dim_info,
											 NULL,
											 NULL,
											 NULL,
											 chunk_sizing_info);
	if (!created)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("continuous agg could not create hypertable for relid")));
	}
}

static bool
check_trigger_exists_hypertable(Oid relid, char *trigname)
{
	Relation tgrel;
	ScanKeyData skey[1];
	SysScanDesc tgscan;
	HeapTuple tuple;
	bool trg_found = false;

	tgrel = heap_open(TriggerRelationId, AccessShareLock);
	ScanKeyInit(&skey[0],
				Anum_pg_trigger_tgrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(tgscan)))
	{
		Form_pg_trigger trig = (Form_pg_trigger) GETSTRUCT(tuple);
		if (namestrcmp(&(trig->tgname), trigname) == 0)
		{
			trg_found = true;
			break;
		}
	}
	systable_endscan(tgscan);
	heap_close(tgrel, AccessShareLock);
	return trg_found;
}

/* add continuous agg invalidation trigger to hypertable
 * relid - oid of hypertable
 * trigarg - argument to pass to trigger (the hypertable id from timescaledb catalog as a string)
 */
static void
cagg_add_trigger_hypertable(Oid relid, char *trigarg)
{
	ObjectAddress objaddr;
	char *relname = get_rel_name(relid);
	Oid schemaid = get_rel_namespace(relid);
	char *schema = get_namespace_name(schemaid);
	Cache *hcache;
	Hypertable *ht;

	CreateTrigStmt stmt = {
		.type = T_CreateTrigStmt,
		.row = true,
		.timing = TRIGGER_TYPE_AFTER,
		.trigname = CAGGINVAL_TRIGGER_NAME,
		.relation = makeRangeVar(schema, relname, -1),
		.funcname =
			list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(CAGG_INVALIDATION_TRIGGER)),
		.args = list_make1(makeString(trigarg)),
		.events = TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_DELETE,
	};
	if (check_trigger_exists_hypertable(relid, CAGGINVAL_TRIGGER_NAME))
		return;
	ht = ts_hypertable_cache_get_cache_and_entry(relid, false, &hcache);
	objaddr = ts_hypertable_create_trigger(ht, &stmt, NULL);
	if (!OidIsValid(objaddr.objectId))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not create continuous aggregate trigger")));
	ts_cache_release(hcache);
}

/* add additional indexes to materialization table for the columns derived from
 * the group-by column list of the partial select query
 * if partial select query has:
 * GROUP BY timebucket_expr, <grpcol1, grpcol2, grpcol3 ...>
 * index on mattable is <grpcol1, timebucketcol>, <grpcol2, timebucketcol> ... and so on.
 * i.e. #indexes =(  #grp-cols - 1)
 */
static void
mattablecolumninfo_add_mattable_index(MatTableColumnInfo *matcolinfo, Hypertable *ht)
{
	IndexStmt stmt = {
		.type = T_IndexStmt,
		.accessMethod = DEFAULT_INDEX_TYPE,
		.idxname = NULL,
		.relation = makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), 0),
		.tableSpace = get_tablespace_name(get_rel_tablespace(ht->main_table_relid)),
	};
	IndexElem timeelem = { .type = T_IndexElem,
						   .name = matcolinfo->matpartcolname,
						   .ordering = SORTBY_DESC };
	ListCell *le = NULL;
	foreach (le, matcolinfo->mat_groupcolname_list)
	{
		NameData indxname;
		ObjectAddress indxaddr;
		HeapTuple indxtuple;
		char *grpcolname = (char *) lfirst(le);
		IndexElem grpelem = { .type = T_IndexElem, .name = grpcolname };
		stmt.indexParams = list_make2(&grpelem, &timeelem);
		indxaddr = DefineIndexCompat(ht->main_table_relid,
									 &stmt,
									 InvalidOid,
									 false,  /* is alter table */
									 false,  /* check rights */
									 false,  /* skip_build */
									 false); /* quiet */
		indxtuple = SearchSysCache1(RELOID, ObjectIdGetDatum(indxaddr.objectId));

		if (!HeapTupleIsValid(indxtuple))
			elog(ERROR, "cache lookup failed for index relid %d", indxaddr.objectId);
		indxname = ((Form_pg_class) GETSTRUCT(indxtuple))->relname;
		elog(NOTICE,
			 "adding index %s ON %s.%s USING BTREE(%s, %s)",
			 NameStr(indxname),
			 NameStr(ht->fd.schema_name),
			 NameStr(ht->fd.table_name),
			 grpcolname,
			 matcolinfo->matpartcolname);
		ReleaseSysCache(indxtuple);
	}
}

/*
 * Create the materialization hypertable root by faking up a
 * CREATE TABLE parsetree and passing it to DefineRelation.
 * Reuse the information from ViewStmt:
 *   Remove the options on the into clause that we will not honour
 *   Modify the relname to ts_internal_<name>
 *  Parameters:
 *  mat_rel - relation information for the materialization table
 *  origquery_tblinfo - user query's tbale information. used for setting up thr partitioning of the
 * hypertable mataddress - return the ObjectAddress RETURNS: hypertable id of the materialization
 * table
 */
static int32
mattablecolumninfo_create_materialization_table(MatTableColumnInfo *matcolinfo, int32 hypertable_id,
												RangeVar *mat_rel,
												CAggTimebucketInfo *origquery_tblinfo,
												bool create_addl_index, ObjectAddress *mataddress)
{
	Oid uid, saved_uid;
	int sec_ctx;
	char *matpartcolname = matcolinfo->matpartcolname;
	CreateStmt *create;
	Datum toast_options;
	int64 matpartcol_interval;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	int32 mat_htid;
	Oid mat_relid;
	Cache *hcache;
	Hypertable *ht = NULL;
	Oid owner = GetUserId();

	create = makeNode(CreateStmt);
	create->relation = mat_rel;
	create->tableElts = matcolinfo->matcollist;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = NULL;
	create->oncommit = ONCOMMIT_NOOP;
	create->tablespacename = NULL;
	create->if_not_exists = false;

	/*  Create the materialization table.  */
	SWITCH_TO_TS_USER(mat_rel->schemaname, uid, saved_uid, sec_ctx);
	*mataddress = DefineRelationCompat(create, RELKIND_RELATION, owner, NULL, NULL);
	CommandCounterIncrement();
	mat_relid = mataddress->objectId;

	/* NewRelationCreateToastTable calls CommandCounterIncrement */
	toast_options =
		transformRelOptions((Datum) 0, create->options, "toast", validnsps, true, false);
	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	NewRelationCreateToastTable(mat_relid, toast_options);
	RESTORE_USER(uid, saved_uid, sec_ctx);

	/*convert the mat. table to a hypertable */
	matpartcol_interval = MATPARTCOL_INTERVAL_FACTOR * (origquery_tblinfo->htpartcol_interval_len);
	cagg_create_hypertable(hypertable_id, mat_relid, matpartcolname, matpartcol_interval);

	/* retrieve the hypertable id from the cache */
	ht = ts_hypertable_cache_get_cache_and_entry(mat_relid, false, &hcache);
	mat_htid = ht->fd.id;

	/* create additional index on the group-by columns for the materialization table */
	if (create_addl_index)
		mattablecolumninfo_add_mattable_index(matcolinfo, ht);
	ts_cache_release(hcache);
	return mat_htid;
}

/* Use the userview query to create the partial query to populate
 * the materialization columns and remove HAVING clause and ORDER BY
 */
static Query *
mattablecolumninfo_get_partial_select_query(MatTableColumnInfo *mattblinfo, Query *userview_query)
{
	Query *partial_selquery;
	CAGG_MAKEQUERY(partial_selquery, userview_query);
	partial_selquery->rtable = copyObject(userview_query->rtable);
	partial_selquery->jointree = copyObject(userview_query->jointree);
	partial_selquery->targetList = mattblinfo->partial_seltlist;
	partial_selquery->groupClause = mattblinfo->partial_grouplist;
	partial_selquery->havingQual = NULL;
	partial_selquery->sortClause = NULL;
	return partial_selquery;
}
/* create a  view for the query using the SELECt stmt sqlquery
 * and view name from RangeVar viewrel
 */
static ObjectAddress
create_view_for_query(Query *selquery, RangeVar *viewrel)
{
	Oid uid, saved_uid;
	int sec_ctx;
	ObjectAddress address;
	CreateStmt *create;
	List *selcollist = NIL;
	Oid owner = GetUserId();
	ListCell *lc;
	foreach (lc, selquery->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (!tle->resjunk)
		{
			ColumnDef *col = makeColumnDef(tle->resname,
										   exprType((Node *) tle->expr),
										   exprTypmod((Node *) tle->expr),
										   exprCollation((Node *) tle->expr));
			selcollist = lappend(selcollist, col);
		}
	}

	create = makeNode(CreateStmt);
	create->relation = viewrel;
	create->tableElts = selcollist;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = NULL;
	create->oncommit = ONCOMMIT_NOOP;
	create->tablespacename = NULL;
	create->if_not_exists = false;

	/*  Create the view. viewname is in viewrel.
	 */
	SWITCH_TO_TS_USER(viewrel->schemaname, uid, saved_uid, sec_ctx);
	address = DefineRelationCompat(create, RELKIND_VIEW, owner, NULL, NULL);
	CommandCounterIncrement();
	StoreViewQuery(address.objectId, selquery, false);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
	return address;
}

/* initialize caggtimebucket */
static void
caggtimebucketinfo_init(CAggTimebucketInfo *src, int32 hypertable_id, Oid hypertable_oid,
						AttrNumber hypertable_partition_colno, Oid hypertable_partition_coltype,
						int64 hypertable_partition_col_interval)
{
	src->htid = hypertable_id;
	src->htoid = hypertable_oid;
	src->htpartcolno = hypertable_partition_colno;
	src->htpartcoltype = hypertable_partition_coltype;
	src->htpartcol_interval_len = hypertable_partition_col_interval;
	src->bucket_width = 0; /*invalid value */
}

/* Check if the group-by clauses has exactly 1 time_bucket(.., <col>)
 * where <col> is the hypertable's partitioning column.
 */
static void
caggtimebucket_validate(CAggTimebucketInfo *tbinfo, List *groupClause, List *targetList)
{
	ListCell *l;
	bool found = false;
	foreach (l, groupClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(l);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, targetList);
		if (IsA(tle->expr, FuncExpr))
		{
			FuncExpr *fe = ((FuncExpr *) tle->expr);
			Const *width_arg;
			Node *col_arg;

			if (!is_valid_bucketing_function(fe->funcid))
				continue;

			if (found)
				elog(ERROR,
					 "multiple time_bucket functions not permitted in continuous aggregate "
					 "query");
			else
				found = true;

			/*only column allowed : time_bucket('1day', <column> ) */
			col_arg = lsecond(fe->args);
			if (!(IsA(col_arg, Var)) || ((Var *) col_arg)->varattno != tbinfo->htpartcolno)
				elog(ERROR,
					 "time_bucket function for continuous aggregate query should be called "
					 "on the dimension column of the hypertable ");
			if (!IsA(linitial(fe->args), Const))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("first argument to time_bucket function should be a constant for "
								"continuous aggregate query")));
			}
			width_arg = (Const *) linitial(fe->args);
			tbinfo->bucket_width =
				ts_interval_value_to_internal(width_arg->constvalue, width_arg->consttype);
		}
	}
	if (!found)
	{
		elog(ERROR, "no valid bucketing function found for continuous aggregate query");
	}
}

static int64
get_refresh_lag(Oid column_type, int64 bucket_width, WithClauseResult *with_clause_options)
{
	if (with_clause_options[ContinuousViewOptionRefreshLag].is_default)
		return bucket_width * 2;
	return continuous_agg_parse_refresh_lag(column_type, with_clause_options);
}

static int64
get_max_interval_per_job(Oid column_type, WithClauseResult *with_clause_options, int64 bucket_width)
{
	if (with_clause_options[ContinuousViewOptionMaxIntervalPerRun].is_default)
	{
		return (bucket_width < DEFAULT_MAX_INTERVAL_MAX_BUCKET_WIDTH) ?
				   DEFAULT_MAX_INTERVAL_MULTIPLIER * bucket_width :
				   PG_INT64_MAX;
	}
	return continuous_agg_parse_max_interval_per_job(column_type,
													 with_clause_options,
													 bucket_width);
}

static int64
get_ignore_invalidation_older_than(Oid column_type, WithClauseResult *with_clause_options)
{
	if (with_clause_options[ContinuousViewOptionIgnoreInvalidationOlderThan].is_default)
		return PG_INT64_MAX;
	return continuous_agg_parse_ignore_invalidation_older_than(column_type, with_clause_options);
}

static bool
cagg_agg_validate(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref *agg = (Aggref *) node;
		HeapTuple aggtuple;
		Form_pg_aggregate aggform;
		if (agg->aggorder || agg->aggdistinct || agg->aggfilter)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregates with FILTER / DISTINCT / ORDER BY are not supported for "
							"continuous "
							"aggregate query")));
		}
		/* Fetch the pg_aggregate row */
		aggtuple = SearchSysCache1(AGGFNOID, agg->aggfnoid);
		if (!HeapTupleIsValid(aggtuple))
			elog(ERROR, "cache lookup failed for aggregate %u", agg->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggtuple);
		if (aggform->aggkind != 'n')
		{
			ReleaseSysCache(aggtuple);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ordered set/hypothetical aggregates are not supported by "
							"continuous aggregate query")));
		}
		if (aggform->aggcombinefn == InvalidOid ||
			(aggform->aggtranstype == INTERNALOID && aggform->aggdeserialfn == InvalidOid))
		{
			ReleaseSysCache(aggtuple);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregates which are not parallelizable are not supported by "
							"continuous aggregate query")));
		}
		ReleaseSysCache(aggtuple);

		return false;
	}
	return expression_tree_walker(node, cagg_agg_validate, context);
}

static CAggTimebucketInfo
cagg_validate_query(Query *query)
{
	CAggTimebucketInfo ret;
	Cache *hcache;
	Hypertable *ht = NULL;
	RangeTblRef *rtref = NULL;
	RangeTblEntry *rte;
	List *fromList;

	if (query->commandType != CMD_SELECT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only SELECT query permitted for continuous aggregate query")));
	}
	if (query->hasWindowFuncs || query->hasSubLinks || query->hasDistinctOn ||
		query->hasRecursive || query->hasModifyingCTE || query->hasForUpdate ||
		query->hasRowSecurity
#if !PG96
		|| query->hasTargetSRFs
#endif
		|| query->cteList || query->groupingSets || query->distinctClause || query->setOperations ||
		query->limitOffset || query->limitCount || query->sortClause)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query for continuous aggregate")));
	}
	if (!query->groupClause)
	{
		/*query can have aggregate without group by , so look
		 * for groupClause*/
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("SELECT query for continuous aggregate should have at least 1 aggregate "
						"function and a GROUP BY clause with time_bucket")));
	}
	/*validate aggregates allowed */
	cagg_agg_validate((Node *) query->targetList, NULL);
	cagg_agg_validate((Node *) query->havingQual, NULL);

	fromList = query->jointree->fromlist;
	if (list_length(fromList) != 1 || !IsA(linitial(fromList), RangeTblRef))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "only 1 hypertable is permitted in SELECT query for continuous aggregate")));
	}
	/* check if we have a hypertable in the FROM clause */
	rtref = linitial_node(RangeTblRef, query->jointree->fromlist);
	rte = list_nth(query->rtable, rtref->rtindex - 1);
	/* FROM only <tablename> sets rte->inh to false */
	if (rte->relkind != RELKIND_RELATION || rte->tablesample || rte->inh == false)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid SELECT query for continuous aggregate")));
	}
	if (rte->relkind == RELKIND_RELATION)
	{
		Dimension *part_dimension = NULL;
		ht = ts_hypertable_cache_get_cache_and_entry(rte->relid, false, &hcache);

		/* there can only be one continuous aggregate per table */
		switch (ts_continuous_agg_hypertable_status(ht->fd.id))
		{
			case HypertableIsMaterialization:
			case HypertableIsMaterializationAndRaw:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypertable is a continuous aggregate materialization table"),
						 errhint(
							 "creating continuous aggregates based on continuous aggregates is not "
							 "yet supported")));
			case HypertableIsRawTable:
				break;
			case HypertableIsNotContinuousAgg:
				break;
			default:
				Assert(false && "unreachable");
		}

		/* get primary partitioning column information */
		part_dimension = hyperspace_get_open_dimension(ht->space, 0);

		/* NOTE: if we ever allow custom partitioning functions we'll need to
		 *       change part_dimension->fd.column_type to partitioning_type
		 *       below, along with any other fallout
		 */
		if (part_dimension->partitioning != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("continuous aggregate do not support custom partitioning functions")));
		caggtimebucketinfo_init(&ret,
								ht->fd.id,
								ht->main_table_relid,
								part_dimension->column_attno,
								part_dimension->fd.column_type,
								part_dimension->fd.interval_length);

		if (IS_INTEGER_TYPE(ts_dimension_get_partition_type(part_dimension)) &&
			(strlen(NameStr(part_dimension->fd.integer_now_func)) == 0 ||
			 strlen(NameStr(part_dimension->fd.integer_now_func_schema)) == 0))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("continuous aggregate requires integer_now func to be set on "
							"integer-based hypertables")));

		ts_cache_release(hcache);
	}

	/*check row security settings for the table */
	if (ts_has_row_security(rte->relid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "continuous aggregate query cannot be created on table with row security")));
	}
	/* we need a GROUP By clause with time_bucket on the partitioning
	 * column of the hypertable
	 */
	Assert(query->groupClause);

	caggtimebucket_validate(&ret, query->groupClause, query->targetList);
	return ret;
}

/* add ts_internal_cagg_final to bytea column.
 * bytea column is the internal state for an agg. Pass info for the agg as "inp".
 * inpcol = bytea column.
 * This function returns an aggref
 * ts_internal_cagg_final( Oid, Oid, bytea, NULL::output_typeid)
 * the arguments are a list of targetentry
 */
static Oid
get_finalizefnoid()
{
	Oid finalfnoid;
	Oid finalfnargtypes[] = { TEXTOID,  NAMEOID,	  NAMEOID, get_array_type(NAMEOID),
							  BYTEAOID, ANYELEMENTOID };
	List *funcname = list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(FINALFN));
	int nargs = sizeof(finalfnargtypes) / sizeof(finalfnargtypes[0]);
	finalfnoid = LookupFuncName(funcname, nargs, finalfnargtypes, false);
	return finalfnoid;
}

/* Build a [N][2] array where N is number of arguments and the inner array is of [schema_name,
 * type_name] */
static Datum
get_input_types_array_datum(Aggref *original_aggregate)
{
	ListCell *lc;
	MemoryContext builder_context =
		AllocSetContextCreate(CurrentMemoryContext, "input types builder", ALLOCSET_DEFAULT_SIZES);
	Oid name_array_type_oid = get_array_type(NAMEOID);
	ArrayBuildStateArr *outer_builder =
		initArrayResultArr(name_array_type_oid, NAMEOID, builder_context, false);
	Datum result;

	foreach (lc, original_aggregate->args)
	{
		TargetEntry *te = lfirst(lc);
		Oid type_oid = exprType((Node *) te->expr);
		ArrayBuildState *schema_name_builder = initArrayResult(NAMEOID, builder_context, false);
		HeapTuple tp;
		Form_pg_type typtup;
		char *schema_name;
		Name type_name = (Name) palloc0(NAMEDATALEN);
		Datum schema_datum;
		Datum type_name_datum;
		Datum inner_array_datum;

		tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for type %u", type_oid);

		typtup = (Form_pg_type) GETSTRUCT(tp);
		namecpy(type_name, &typtup->typname);
		schema_name = get_namespace_name(typtup->typnamespace);
		ReleaseSysCache(tp);

		type_name_datum = NameGetDatum(type_name);
		/* using name in because creating from a char * (that may be null or too long) */
		schema_datum = DirectFunctionCall1(namein, CStringGetDatum(schema_name));

		accumArrayResult(schema_name_builder, schema_datum, false, NAMEOID, builder_context);
		accumArrayResult(schema_name_builder, type_name_datum, false, NAMEOID, builder_context);

		inner_array_datum = makeArrayResult(schema_name_builder, CurrentMemoryContext);

		accumArrayResultArr(outer_builder,
							inner_array_datum,
							false,
							name_array_type_oid,
							builder_context);
	}
	result = makeArrayResultArr(outer_builder, CurrentMemoryContext, false);

	MemoryContextDelete(builder_context);
	return result;
}

/* creates an aggref of the form:
 * finalize-agg(
 *                "sum(int)" TEXT,
 *                collation_schema_name NAME, collation_name NAME,
 *                input_types_array NAME[N][2],
 *                <partial-column-name> BYTEA,
 *                null::<return-type of sum(int)>
 *             )
 * here sum(int) is the input aggregate "inp" in the parameter-list
 */
static Aggref *
get_finalize_aggref(Aggref *inp, Var *partial_state_var)
{
	Aggref *aggref;
	TargetEntry *te;
	char *agggregate_signature;
	Const *aggregate_signature_const, *collation_schema_const, *collation_name_const,
		*input_types_const, *return_type_const;
	Oid name_array_type_oid = get_array_type(NAMEOID);
	Var *partial_bytea_var;
	List *tlist = NIL;
	int tlist_attno = 1;
	List *argtypes = NIL;
	char *collation_name = NULL, *collation_schema_name = NULL;
	Datum collation_name_datum = (Datum) 0;
	Datum collation_schema_datum = (Datum) 0;
	Oid finalfnoid = get_finalizefnoid();

	argtypes = list_make5_oid(TEXTOID, NAMEOID, NAMEOID, name_array_type_oid, BYTEAOID);
	argtypes = lappend_oid(argtypes, inp->aggtype);

	aggref = makeNode(Aggref);
	aggref->aggfnoid = finalfnoid;
	aggref->aggtype = inp->aggtype;
	aggref->aggcollid = inp->aggcollid;
	aggref->inputcollid = inp->inputcollid;
	aggref->aggtranstype = InvalidOid; /* will be set by planner */
	aggref->aggargtypes = argtypes;
	aggref->aggdirectargs = NULL; /*relevant for hypothetical set aggs*/
	aggref->aggorder = NULL;
	aggref->aggdistinct = NULL;
	aggref->aggfilter = NULL;
	aggref->aggstar = false;
	aggref->aggvariadic = false;
	aggref->aggkind = AGGKIND_NORMAL;
	aggref->aggsplit = AGGSPLIT_SIMPLE; // TODO make sure plannerdoes not change this ???
	aggref->location = -1;				/*unknown */
										/* construct the arguments */
	agggregate_signature = DatumGetCString(DirectFunctionCall1(regprocedureout, inp->aggfnoid));
	aggregate_signature_const = makeConst(TEXTOID,
										  -1,
										  DEFAULT_COLLATION_OID,
										  -1,
										  CStringGetTextDatum(agggregate_signature),
										  false,
										  false /* passbyval */
	);
	te = makeTargetEntry((Expr *) aggregate_signature_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	if (OidIsValid(inp->inputcollid))
	{
		/* similar to generate_collation_name */
		HeapTuple tp;
		Form_pg_collation colltup;
		tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(inp->inputcollid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for collation %u", inp->inputcollid);
		colltup = (Form_pg_collation) GETSTRUCT(tp);
		collation_name = pstrdup(NameStr(colltup->collname));
		collation_name_datum = DirectFunctionCall1(namein, CStringGetDatum(collation_name));

		collation_schema_name = get_namespace_name(colltup->collnamespace);
		if (collation_schema_name != NULL)
			collation_schema_datum =
				DirectFunctionCall1(namein, CStringGetDatum(collation_schema_name));
		ReleaseSysCache(tp);
	}
	collation_schema_const = makeConst(NAMEOID,
									   -1,
									   InvalidOid,
									   NAMEDATALEN,
									   collation_schema_datum,
									   (collation_schema_name == NULL) ? true : false,
									   false /* passbyval */
	);
	te = makeTargetEntry((Expr *) collation_schema_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	collation_name_const = makeConst(NAMEOID,
									 -1,
									 InvalidOid,
									 NAMEDATALEN,
									 collation_name_datum,
									 (collation_name == NULL) ? true : false,
									 false /* passbyval */
	);
	te = makeTargetEntry((Expr *) collation_name_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	input_types_const = makeConst(get_array_type(NAMEOID),
								  -1,
								  InvalidOid,
								  -1,
								  get_input_types_array_datum(inp),
								  false,
								  false /* passbyval */
	);
	te = makeTargetEntry((Expr *) input_types_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	partial_bytea_var = copyObject(partial_state_var);
	te = makeTargetEntry((Expr *) partial_bytea_var, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	return_type_const = makeNullConst(inp->aggtype, -1, inp->aggcollid);
	te = makeTargetEntry((Expr *) return_type_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	Assert(tlist_attno == 7);
	aggref->args = tlist;
	return aggref;
}

/* creates a partialize expr for the passed in agg:
 * partialize_agg( agg)
 */
static FuncExpr *
get_partialize_funcexpr(Aggref *agg)
{
	FuncExpr *partialize_fnexpr;
	Oid partfnoid, partargtype;
	partargtype = ANYELEMENTOID;
	partfnoid = LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(PARTIALFN)),
							   1,
							   &partargtype,
							   false);
	partialize_fnexpr = makeFuncExpr(partfnoid,
									 BYTEAOID,
									 list_make1(agg), /*args*/
									 InvalidOid,
									 InvalidOid,
									 COERCE_EXPLICIT_CALL);
	return partialize_fnexpr;
}

/*
 * check if the supplied oid belongs to a valid bucket function
 * for continuous aggregates
 * We only support 2-arg variants of time_bucket
 */
static bool
is_valid_bucketing_function(Oid funcid)
{
	FuncInfo *finfo = ts_func_cache_get_bucketing_func(funcid);

	return finfo != NULL && finfo->is_timescaledb_func && finfo->nargs == 2;
}

/*initialize MatTableColumnInfo */
static void
mattablecolumninfo_init(MatTableColumnInfo *matcolinfo, List *collist, List *tlist, List *grouplist)
{
	matcolinfo->matcollist = collist;
	matcolinfo->partial_seltlist = tlist;
	matcolinfo->partial_grouplist = grouplist;
	matcolinfo->mat_groupcolname_list = NIL;
	matcolinfo->matpartcolno = -1;
	matcolinfo->matpartcolname = NULL;
}
/*
 * Add Information required to create and populate the materialization table
 * columns
 * a ) create a columndef for the materialization table
 * b) create the corresponding expr to populate the column of the materialization table (e..g for a
 * column that is an aggref, we create a partialize_agg expr to populate the column Returns: the Var
 * corresponding to the newly created column of the materialization table
 * Notes: make sure the materialization table columns do not save
 * values computed by mutable function.
 */
static Var *
mattablecolumninfo_addentry(MatTableColumnInfo *out, Node *input, int original_query_resno)
{
	int matcolno = list_length(out->matcollist) + 1;
	char colbuf[NAMEDATALEN];
	char *colname;
	TargetEntry *part_te = NULL;
	ColumnDef *col;
	Var *var;
	Oid coltype, colcollation;
	int32 coltypmod;
	if (contain_mutable_functions(input))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only immutable functions are supported for continuous aggregate query"),
				 errhint("Many time-based function that are not immutable have immutable "
						 "alternatives that require specifying the timezone explicitly")));
	}
	switch (nodeTag(input))
	{
		case T_Aggref:
		{
			FuncExpr *fexpr = get_partialize_funcexpr((Aggref *) input);
			PRINT_MATCOLNAME(colbuf, "agg", original_query_resno, matcolno);
			colname = colbuf;
			coltype = BYTEAOID;
			coltypmod = -1;
			colcollation = InvalidOid;
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = makeTargetEntry((Expr *) fexpr, matcolno, pstrdup(colname), false);
		}
		break;
		case T_TargetEntry:
		{
			TargetEntry *tle = (TargetEntry *) input;
			bool timebkt_chk = false;

			if (IsA(tle->expr, FuncExpr))
				timebkt_chk = is_valid_bucketing_function(((FuncExpr *) tle->expr)->funcid);

			if (tle->resname)
				colname = pstrdup(tle->resname);
			else
			{
				if (timebkt_chk)
					colname = DEFAULT_MATPARTCOLUMN_NAME;
				else
				{
					PRINT_MATCOLNAME(colbuf, "grp", original_query_resno, matcolno);
					colname = colbuf;
				}
			}

			if (timebkt_chk)
			{
				tle->resname = pstrdup(colname);
				out->matpartcolno = matcolno - 1;
				out->matpartcolname = pstrdup(colname);
			}
			else
			{
				out->mat_groupcolname_list = lappend(out->mat_groupcolname_list, pstrdup(colname));
			}
			coltype = exprType((Node *) tle->expr);
			coltypmod = exprTypmod((Node *) tle->expr);
			colcollation = exprCollation((Node *) tle->expr);
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = (TargetEntry *) copyObject(input);
			/*need to project all the partial entries so that materialization table is filled */
			part_te->resjunk = false;
			if (timebkt_chk)
			{
				col->is_not_null = true;
			}
			if (part_te->resname == NULL)
			{
				part_te->resname = pstrdup(colname);
			}
		}
		break;
		default:
			elog(ERROR, "invalid node type %d", nodeTag(input));
			break;
	}
	Assert(list_length(out->matcollist) == list_length(out->partial_seltlist));
	Assert(col != NULL);
	Assert(part_te != NULL);
	out->matcollist = lappend(out->matcollist, col);
	out->partial_seltlist = lappend(out->partial_seltlist, part_te);
	var = makeVar(1, matcolno, coltype, coltypmod, colcollation, 0);
	return var;
}

/*add internal columns for the materialization table */
static void
mattablecolumninfo_addinternal(MatTableColumnInfo *matcolinfo, RangeTblEntry *usertbl_rte,
							   int32 usertbl_htid)
{
	Index maxRef;
	int colno = list_length(matcolinfo->partial_seltlist) + 1;
	ColumnDef *col;
	Var *chunkfn_arg1;
	FuncExpr *chunk_fnexpr;
	Oid chunkfnoid;
	Oid argtype[] = { OIDOID };
	Oid rettype = INT4OID;
	TargetEntry *chunk_te;
	Oid sortop, eqop;
	bool hashable;
	ListCell *lc;
	SortGroupClause *grpcl;

	/* add a chunk_id column for materialization table */
	Node *vexpr = (Node *) makeVar(1, colno, INT4OID, -1, InvalidOid, 0);
	col = makeColumnDef(CONTINUOUS_AGG_CHUNK_ID_COL_NAME,
						exprType(vexpr),
						exprTypmod(vexpr),
						exprCollation(vexpr));
	matcolinfo->matcollist = lappend(matcolinfo->matcollist, col);

	/* need to add an entry to the target list for computing chunk_id column
	: chunk_for_tuple( htid, table.*)
	*/
	chunkfnoid =
		LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(CHUNKIDFROMRELID)),
					   sizeof(argtype) / sizeof(argtype[0]),
					   argtype,
					   false);
	chunkfn_arg1 = makeVar(1, TableOidAttributeNumber, OIDOID, -1, 0, 0);

	chunk_fnexpr = makeFuncExpr(chunkfnoid,
								rettype,
								list_make1(chunkfn_arg1),
								InvalidOid,
								InvalidOid,
								COERCE_EXPLICIT_CALL);
	chunk_te = makeTargetEntry((Expr *) chunk_fnexpr,
							   colno,
							   pstrdup(CONTINUOUS_AGG_CHUNK_ID_COL_NAME),
							   false);
	matcolinfo->partial_seltlist = lappend(matcolinfo->partial_seltlist, chunk_te);
	/*any internal column needs to be added to the group-by clause as well */
	maxRef = 0;
	foreach (lc, matcolinfo->partial_seltlist)
	{
		Index ref = ((TargetEntry *) lfirst(lc))->ressortgroupref;

		if (ref > maxRef)
			maxRef = ref;
	}
	chunk_te->ressortgroupref =
		maxRef + 1; /* used by sortgroupclause to identify the targetentry */
	grpcl = makeNode(SortGroupClause);
	get_sort_group_operators(exprType((Node *) chunk_te->expr),
							 false,
							 true,
							 false,
							 &sortop,
							 &eqop,
							 NULL,
							 &hashable);
	grpcl->tleSortGroupRef = chunk_te->ressortgroupref;
	grpcl->eqop = eqop;
	grpcl->sortop = sortop;
	grpcl->nulls_first = false;
	grpcl->hashable = hashable;

	matcolinfo->partial_grouplist = lappend(matcolinfo->partial_grouplist, grpcl);
}

static Node *
add_aggregate_partialize_mutator(Node *node, AggPartCxt *cxt)
{
	if (node == NULL)
		return NULL;
	/* modify the aggref and create a partialize(aggref) expr
	 * for the materialization.
	 * Add a corresponding  columndef for the mat table.
	 * Replace the aggref with the ts_internal_cagg_final fn.
	 * using a Var for the corresponding column in the mat table.
	 * All new Vars have varno = 1 (for RTE 1)
	 */
	if (IsA(node, Aggref))
	{
		Aggref *newagg;
		Var *var;

		if (cxt->ignore_aggoid == ((Aggref *) node)->aggfnoid)
			return node; /*don't process this further */

		/* step 1: create partialize( aggref) column
		 * for materialization table */
		var = mattablecolumninfo_addentry(cxt->mattblinfo, node, cxt->original_query_resno);
		cxt->addcol = true;
		/* step 2: create finalize_agg expr using var
		 * for the clumn added to the materialization table
		 */
		/* This is a var for the column we created */
		newagg = get_finalize_aggref((Aggref *) node, var);
		return (Node *) newagg;
	}
	return expression_tree_mutator(node, add_aggregate_partialize_mutator, cxt);
}

/* This code modifies modquery */
/* having clause needs transformation
 * original query is
 * select a, count(b), min(c)
 * from ..
 * group by a
 * having a> 10 or count(b) > 20 or min(d) = 4
 * we get a mat table
 * a, partial(countb), partial(minc) after processing
 * the target list. We need to add entries from the having clause
 * so the modified mat table is
 * a, partial(count), partial(minc), partial(mind)
 * and the new select from the mat table is
 * i.e. we have
 * select col1, finalize(col2), finalize(col3)
 * from ..
 * group by col1
 * having col1 > 10 or finalize(col2) > 20 or finalize(col4) = 4
 * Note: col# = corresponding column from the mat table
 */
typedef struct Cagg_havingcxt
{
	TargetEntry *old;
	TargetEntry *new;
	bool found;
} cagg_havingcxt;

/* if we find a target entry  expr that matches the node , then replace it with the
 * expression from  new target entry.
 */
static Node *
replace_having_qual_mutator(Node *node, cagg_havingcxt *cxt)
{
	if (node == NULL)
		return NULL;
	if (equal(node, cxt->old->expr))
	{
		cxt->found = true;
		return (Node *) cxt->new->expr;
	}
	return expression_tree_mutator(node, replace_having_qual_mutator, cxt);
}

/* modify the havingqual and replace exprs that already occur in targetlist
 * with entries from new target list
 * RETURNS: havingQual
 */
static Node *
replace_targetentry_in_havingqual(Query *origquery, List *newtlist)
{
	Node *having = copyObject(origquery->havingQual);
	List *origtlist = origquery->targetList;
	List *modtlist = newtlist;
	ListCell *lc, *lc2;
	cagg_havingcxt hcxt;

	/* if we have any exprs that are in the targetlist, then we already have columns
	 * for them in the mat table. So replace with the correct expr
	 */
	forboth (lc, origtlist, lc2, modtlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		TargetEntry *modte = (TargetEntry *) lfirst(lc2);
		hcxt.old = te;
		hcxt.new = modte;
		hcxt.found = false;
		having =
			(Node *) expression_tree_mutator((Node *) having, replace_having_qual_mutator, &hcxt);
	}
	return having;
}

/*
Init the finalize query data structure.
Parameters:
orig_query - the original query from user view that is being used as template for the finalize query
tlist_aliases - aliases for the view select list
materialization table columns are created . This will be returned in  the mattblinfo

DO NOT modify orig_query. Make a copy if needed.
SIDE_EFFCT: the data structure in mattblinfo is modified as a side effect by adding new materialize
table columns and partialize exprs.
*/
static void
finalizequery_init(FinalizeQueryInfo *inp, Query *orig_query, MatTableColumnInfo *mattblinfo)
{
	AggPartCxt cxt;
	ListCell *lc;
	Node *newhavingQual;
	int resno = 1;

	inp->final_userquery = copyObject(orig_query);
	inp->final_seltlist = NIL;
	inp->final_havingqual = NULL;

	/* Set up the final_seltlist and final_havingqual entries */
	cxt.mattblinfo = mattblinfo;
	cxt.ignore_aggoid = InvalidOid;

	/* We want all the entries in the targetlist (resjunk or not)
	 * in the materialization  table defintion so we include group-by/having clause etc.
	 * We have to do 3 things here: 1) create a column for mat table , 2) partialize_expr to
	 * populate it and 3) modify the target entry to be a finalize_expr that selects from the
	 * materialization table
	 */
	foreach (lc, orig_query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		TargetEntry *modte = copyObject(tle);
		cxt.addcol = false;
		cxt.original_query_resno = resno;
		/* if tle has aggrefs , get the corresponding
		 * finalize_agg expression and save it in modte
		 * also add correspong materialization table column info
		 * for the aggrefs in tle. */
		modte = (TargetEntry *) expression_tree_mutator((Node *) modte,
														add_aggregate_partialize_mutator,
														&cxt);
		/* We need columns for non-aggregate targets
		 * if it is not a resjunk OR appears in the grouping clause
		 */
		if (cxt.addcol == false && (tle->resjunk == false || tle->ressortgroupref > 0))
		{
			Var *var;
			var =
				mattablecolumninfo_addentry(cxt.mattblinfo, (Node *) tle, cxt.original_query_resno);
			/* fix the expression for the target entry */
			modte->expr = (Expr *) var;
		}
		/* Construct the targetlist for the query on the
		 * materialization table. The TL maps 1-1 with the original
		 * query:
		 * e.g select a, min(b)+max(d) from foo group by a,timebucket(a);
		 * becomes
		 * select <a-col>,
		 * ts_internal_cagg_final(..b-col ) + ts_internal_cagg_final(..d-col)
		 * from mattbl
		 * group by a-col, timebucket(a-col)
		 */
		/*we copy the modte target entries , resnos should be the same for final_selquery and
		 * origquery . so tleSortGroupReffor the targetentry can be reused, only table info needs to
		 * be modified
		 */
		Assert(modte->resno == resno);
		resno++;
		if (IsA(modte->expr, Var))
		{
			modte->resorigcol = ((Var *) modte->expr)->varattno;
		}
		inp->final_seltlist = lappend(inp->final_seltlist, modte);
	}
	/* all grouping clause elements are in targetlist already.
	   so let's check the having clause */
	newhavingQual = replace_targetentry_in_havingqual(inp->final_userquery, inp->final_seltlist);
	/* we might still have aggs in havingqual which don't appear in the targetlist , but don't
	 * overwrite finalize_agg exprs that we have in the havingQual*/
	cxt.addcol = false;
	cxt.ignore_aggoid = get_finalizefnoid();
	cxt.original_query_resno = 0;
	inp->final_havingqual =
		expression_tree_mutator((Node *) newhavingQual, add_aggregate_partialize_mutator, &cxt);
}
/* Create select query with the finalize aggregates
 * for the materialization table
 * matcollist - column list for mat table
 * mattbladdress - materialization table ObjectAddress
 */
static Query *
finalizequery_get_select_query(FinalizeQueryInfo *inp, List *matcollist,
							   ObjectAddress *mattbladdress)
{
	Query *final_selquery = NULL;
	ListCell *lc;
	/*
	 * for initial cagg creation rtable will have only 1 entry,
	 * for alter table rtable will have multiple entries with our
	 * RangeTblEntry as last member.
	 */
	RangeTblEntry *rte = llast_node(RangeTblEntry, inp->final_userquery->rtable);
	FromExpr *fromexpr;
	Var *result;
	rte->relid = mattbladdress->objectId;
	rte->rtekind = RTE_RELATION;
	rte->relkind = RELKIND_RELATION;
	rte->tablesample = NULL;
	rte->eref->colnames = NIL;
	/* aliases for column names for the materialization table*/
	foreach (lc, matcollist)
	{
		ColumnDef *cdef = (ColumnDef *) lfirst(lc);
		Value *attrname = makeString(cdef->colname);
		rte->eref->colnames = lappend(rte->eref->colnames, attrname);
	}
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
	result = makeWholeRowVar(rte, list_length(inp->final_userquery->rtable), 0, true);
	result->location = 0;
	markVarForSelectPriv(NULL, result, rte);
	/* 2. Fixup targetlist with the correct rel information */
	foreach (lc, inp->final_seltlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (IsA(tle->expr, Var))
		{
			tle->resorigtbl = rte->relid;
			tle->resorigcol = ((Var *) tle->expr)->varattno;
		}
	}

	CAGG_MAKEQUERY(final_selquery, inp->final_userquery);
	final_selquery->rtable = inp->final_userquery->rtable; /*fixed up above */
	/* fixup from list. No quals on original table should be
	 * present here - they should be on the query that populates the mattable (partial_selquery)
	 */
	Assert(list_length(inp->final_userquery->jointree->fromlist) == 1);
	fromexpr = inp->final_userquery->jointree;
	fromexpr->quals = NULL;
	final_selquery->jointree = fromexpr;
	final_selquery->targetList = inp->final_seltlist;
	final_selquery->groupClause = inp->final_userquery->groupClause;
	final_selquery->sortClause = inp->final_userquery->sortClause;
	/* copy the having clause too */
	final_selquery->havingQual = inp->final_havingqual;

	return final_selquery;
}

/* Assign aliases to the targetlist in the query according to the column_names provided
 * in the CREATE VIEW statement.
 */
static void
fixup_userview_query_tlist(Query *userquery, List *tlist_aliases)
{
	if (tlist_aliases != NIL)
	{
		ListCell *lc;
		ListCell *alist_item = list_head(tlist_aliases);
		foreach (lc, userquery->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			/* junk columns don't get aliases */
			if (tle->resjunk)
				continue;
			tle->resname = pstrdup(strVal(lfirst(alist_item)));
			alist_item = lnext(alist_item);
			if (alist_item == NULL)
				break; /* done assigning aliases */
		}

		if (alist_item != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("too many column names were specified")));
	}
}

/* Modifies the passed in ViewStmt to do the following
 * a) Create a hypertable for the continuous agg materialization.
 * b) create a view that references the underlying
 * materialization table instead of the original table used in
 * the CREATE VIEW stmt.
 * Example:
 * CREATE VIEW mcagg ...
 * AS  select a, min(b)+max(d) from foo group by a,timebucket(a);
 *
 * Step 1. create a materialiation table which stores the partials for the
 * aggregates and the grouping columns + internal columns.
 * So we have a table like ts_internal_mcagg_tab
 * with columns:
 *( a, col1, col2, col3, internal-columns)
 * where col1 =  partialize(min(b)), col2= partialize(max(d)),
 * col3= timebucket(a))
 *
 * Step 2: Create a view with modified select query
 * CREATE VIEW mcagg
 * as
 * select a, finalize( col1) + finalize(col2))
 * from ts_internal_mcagg
 * group by a, col3
 *
 * Step 3: Create a view to populate the materialization table
 * create view ts_internal_mcagg_view
 * as
 * select a, partialize(min(b)), partialize(max(d)), timebucket(a), <internal-columns>
 * from foo
 * group by <internal-columns> , a , timebucket(a);
 *
 * Notes: ViewStmt->query is the raw parse tree
 * panquery is the output of running parse_anlayze( ViewStmt->query)
 *               so don't recreate invalidation trigger.
 */
static void
cagg_create(ViewStmt *stmt, Query *panquery, CAggTimebucketInfo *origquery_ht,
			WithClauseResult *with_clause_options)
{
	ObjectAddress mataddress;
	char relnamebuf[NAMEDATALEN];
	MatTableColumnInfo mattblinfo;
	FinalizeQueryInfo finalqinfo;
	CatalogSecurityContext sec_ctx;
	bool is_create_mattbl_index;

	Query *final_selquery;
	Query *partial_selquery;	/* query to populate the mattable*/
	Query *orig_userview_query; /* copy of the original user query for dummy view */
	RangeTblEntry *usertbl_rte;
	Oid nspid;
	RangeVar *part_rel = NULL, *mat_rel = NULL, *dum_rel = NULL;
	int32 materialize_hypertable_id;
	int32 job_id;
	char trigarg[NAMEDATALEN];
	int ret;
	Interval *refresh_interval =
		DatumGetIntervalP(with_clause_options[ContinuousViewOptionRefreshInterval].parsed);
	int64 refresh_lag = get_refresh_lag(origquery_ht->htpartcoltype,
										origquery_ht->bucket_width,
										with_clause_options);
	int64 max_interval_per_job = get_max_interval_per_job(origquery_ht->htpartcoltype,
														  with_clause_options,
														  origquery_ht->bucket_width);
	int64 ignore_invalidation_older_than =
		get_ignore_invalidation_older_than(origquery_ht->htpartcoltype, with_clause_options);

	/* assign the column_name aliases in CREATE VIEW to the query. No other modifications to
	 * panquery */
	fixup_userview_query_tlist(panquery, stmt->aliases);
	mattablecolumninfo_init(&mattblinfo, NIL, NIL, copyObject(panquery->groupClause));
	finalizequery_init(&finalqinfo, panquery, &mattblinfo);

	/* invalidate all options on the stmt before using it
	 * The options are valid only for internal use (ts_continuous)
	 */
	stmt->options = NULL;

	/* Step 0: add any internal columns needed for materialization based
		on the user query's table
	*/
	usertbl_rte = list_nth(panquery->rtable, 0);
	mattablecolumninfo_addinternal(&mattblinfo, usertbl_rte, origquery_ht->htid);

	/* Step 1: create the materialization table */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	materialize_hypertable_id = ts_catalog_table_next_seq_id(ts_catalog_get(), HYPERTABLE);
	ts_catalog_restore_user(&sec_ctx);
	PRINT_MATINTERNAL_NAME(relnamebuf, "_materialized_hypertable_%d", materialize_hypertable_id);
	mat_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);
	is_create_mattbl_index = with_clause_options[ContinuousViewOptionCreateGroupIndex].is_default;
	mattablecolumninfo_create_materialization_table(&mattblinfo,
													materialize_hypertable_id,
													mat_rel,
													origquery_ht,
													is_create_mattbl_index,
													&mataddress);
	/* Step 2: create view with select finalize from materialization
	 * table
	 */
	final_selquery =
		finalizequery_get_select_query(&finalqinfo, mattblinfo.matcollist, &mataddress);

	if (with_clause_options[ContinuousViewOptionMaterializedOnly].parsed == BoolGetDatum(false))
		final_selquery = build_union_query(origquery_ht, &mattblinfo, final_selquery, panquery);

	create_view_for_query(final_selquery, stmt->view);

	/* Step 3: create the internal view with select partialize(..)
	 */
	partial_selquery = mattablecolumninfo_get_partial_select_query(&mattblinfo, panquery);

	PRINT_MATINTERNAL_NAME(relnamebuf, "_partial_view_%d", materialize_hypertable_id);
	part_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);
	create_view_for_query(partial_selquery, part_rel);

	/* Additional miscellaneous steps */
	/* create a dummy view to store the user supplied view query. This is to get PG
	 * to display the view correctly without having to replicate the PG source code for make_viewdef
	 */
	orig_userview_query = copyObject(panquery);
	PRINT_MATINTERNAL_NAME(relnamebuf, "_direct_view_%d", materialize_hypertable_id);
	dum_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);
	create_view_for_query(orig_userview_query, dum_rel);

	/* register the BGW job to process continuous aggs*/
	job_id =
		ts_continuous_agg_job_add(origquery_ht->htid, origquery_ht->bucket_width, refresh_interval);

	/* Step 4 add catalog table entry for the objects we just created */
	nspid = RangeVarGetCreationNamespace(stmt->view);
	create_cagg_catlog_entry(materialize_hypertable_id,
							 origquery_ht->htid,
							 get_namespace_name(nspid), /*schema name for user view */
							 stmt->view->relname,
							 part_rel->schemaname,
							 part_rel->relname,
							 origquery_ht->bucket_width,
							 refresh_lag,
							 max_interval_per_job,
							 ignore_invalidation_older_than,
							 job_id,
							 dum_rel->schemaname,
							 dum_rel->relname);

	/* Step 5 create trigger on raw hypertable -specified in the user view query*/
	ret = snprintf(trigarg, NAMEDATALEN, "%d", origquery_ht->htid);
	if (ret < 0 || ret >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("bad argument to continuous aggregate trigger")));
	cagg_add_trigger_hypertable(origquery_ht->htoid, trigarg);

	return;
}

/* entry point for creating continuous aggregate view
 * step 1 : validate query
 * step 2: create underlying tables and views
 */
bool
tsl_process_continuous_agg_viewstmt(ViewStmt *stmt, const char *query_string, void *pstmt,
									WithClauseResult *with_clause_options)
{
	Query *query = NULL;
	CAggTimebucketInfo timebucket_exprinfo;
	Oid nspid;
#if !PG96
	PlannedStmt *pstmt_info = (PlannedStmt *) pstmt;
	RawStmt *rawstmt = NULL;
	/* we have a continuous aggregate query. convert to Query structure
	 */
	rawstmt = makeNode(RawStmt);
	rawstmt->stmt = (Node *) copyObject(stmt->query);
	rawstmt->stmt_location = pstmt_info->stmt_location;
	rawstmt->stmt_len = pstmt_info->stmt_len;
	query = parse_analyze(rawstmt, query_string, NULL, 0, NULL);
#else
	query = parse_analyze(copyObject(stmt->query), query_string, NULL, 0);
#endif

	nspid = RangeVarGetCreationNamespace(stmt->view);
	if (get_relname_relid(stmt->view->relname, nspid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("continuous aggregate query \"%s\" already exists", stmt->view->relname),
				 errhint("drop and recreate if needed.  This will drop the underlying "
						 "materialization")));
		return true;
	}

	timebucket_exprinfo = cagg_validate_query(query);

	cagg_create(stmt, query, &timebucket_exprinfo, with_clause_options);
	return true;
}

/*
 * update the view definition of an existing continuous aggregate
 */
void
cagg_update_view_definition(ContinuousAgg *agg, Hypertable *mat_ht,
							WithClauseResult *with_clause_options)
{
	ListCell *lc1, *lc2;
	int sec_ctx;
	Oid uid, saved_uid;
	/* cagg view created by the user */
	Oid user_view_oid = relation_oid(agg->data.user_view_schema, agg->data.user_view_name);
	Relation user_view_rel = relation_open(user_view_oid, AccessShareLock);
	Query *user_query = get_view_query(user_view_rel);
	relation_close(user_view_rel, AccessShareLock);

	Oid direct_view_oid = relation_oid(agg->data.direct_view_schema, agg->data.direct_view_name);

	Relation direct_view_rel = relation_open(direct_view_oid, AccessShareLock);
	Query *direct_query = get_view_query(direct_view_rel);

	CAggTimebucketInfo timebucket_exprinfo = cagg_validate_query(direct_query);

	relation_close(direct_view_rel, AccessShareLock);
	FinalizeQueryInfo fqi;
	MatTableColumnInfo mattblinfo;
	ObjectAddress mataddress = {
		.classId = RelationRelationId,
		.objectId = mat_ht->main_table_relid,
	};

	mattablecolumninfo_init(&mattblinfo, NIL, NIL, copyObject(direct_query->groupClause));
	finalizequery_init(&fqi, direct_query, &mattblinfo);

	Query *view_query = finalizequery_get_select_query(&fqi, mattblinfo.matcollist, &mataddress);

	/* adjust varnos in targetlist */
	Assert(list_length(view_query->rtable) > 1);
	foreach (lc1, view_query->targetList)
	{
		ChangeVarNodes((Node *) lfirst_node(TargetEntry, lc1),
					   1,
					   list_length(view_query->rtable),
					   0);
	}

	if (with_clause_options[ContinuousViewOptionMaterializedOnly].parsed == BoolGetDatum(false))
		view_query = build_union_query(&timebucket_exprinfo, &mattblinfo, view_query, direct_query);

	/*
	 * adjust names in the targetlist of the updated view to match the view definition
	 */
	Assert(list_length(view_query->targetList) == list_length(user_query->targetList));

	forboth (lc1, view_query->targetList, lc2, user_query->targetList)
	{
		TargetEntry *view_tle, *user_tle;
		view_tle = lfirst_node(TargetEntry, lc1);
		user_tle = lfirst_node(TargetEntry, lc2);
		view_tle->resname = user_tle->resname;
	}

	SWITCH_TO_TS_USER(NameStr(agg->data.user_view_schema), uid, saved_uid, sec_ctx);
	StoreViewQuery(user_view_oid, view_query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
}

/*
 * create Const of proper type for lower bound of watermark when
 * watermark has not been set yet
 */
static Const *
cagg_boundary_make_lower_bound(Oid type)
{
	Datum value;
	int16 typlen;
	bool typbyval;

	get_typlenbyval(type, &typlen, &typbyval);

	switch (type)
	{
		case INT2OID:
			value = Int16GetDatum(PG_INT16_MIN);
			break;
		case DATEOID:
		case INT4OID:
			value = Int32GetDatum(PG_INT32_MIN);
			break;
		case INT8OID:
		case TIMESTAMPTZOID:
		case TIMESTAMPOID:
			value = Int64GetDatum(PG_INT64_MIN);
			break;
		default:
			/* validation at earlier stages should prevent ever reaching this */
			ereport(ERROR,
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
					 errmsg("unsupported datatype \"%s\" for continuous aggregate",
							format_type_be(type))));
			pg_unreachable();
	}
	return makeConst(type, -1, InvalidOid, typlen, value, false, typbyval);
}

/*
 * get oid of function to convert from our internal representation to postgres representation
 */
static Oid
cagg_get_boundary_converter_funcoid(Oid typoid)
{
	char *function_name;
	Oid argtyp[] = { INT8OID };

	switch (typoid)
	{
		case DATEOID:
			function_name = INTERNAL_TO_DATE_FUNCTION;
			break;
		case TIMESTAMPOID:
			function_name = INTERNAL_TO_TS_FUNCTION;
			break;
		case TIMESTAMPTZOID:
			function_name = INTERNAL_TO_TSTZ_FUNCTION;
			break;
		default:
			/*
			 * this should never be reached and unsupported datatypes
			 * should be caught at much earlier stages
			 */
			ereport(ERROR,
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
					 errmsg("no converter function defined for datatype: %s",
							format_type_be(typoid))));
			pg_unreachable();
	}

	List *func_name = list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(function_name));
	Oid converter_oid = LookupFuncName(func_name, lengthof(argtyp), argtyp, false);

	Assert(OidIsValid(converter_oid));

	return converter_oid;
}

static FuncExpr *
build_conversion_call(Oid type, FuncExpr *boundary)
{
	/*
	 * if the partitioning column type is not integer we need to convert to proper representation
	 */
	switch (type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			/* nothing to do for int types */
			return boundary;
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			/* date/timestamp/timestamptz need to be converted since we store them differently from
			 * postgres format */
			Oid converter_oid = cagg_get_boundary_converter_funcoid(type);
			return makeFuncExpr(converter_oid,
								type,
								list_make1(boundary),
								InvalidOid,
								InvalidOid,
								COERCE_EXPLICIT_CALL);
		}

		default:
			/* all valid types should be handled above, this should never be reached and error
			 * handling at earlier stages should catch this */
			ereport(ERROR,
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
					 errmsg("unsupported datatype for continuous aggregates: %s",
							format_type_be(type))));
			pg_unreachable();
	}
}

/*
 * build function call that returns boundary for a hypertable
 * wrapped in type conversion calls when required
 */
static FuncExpr *
build_boundary_call(int32 ht_id, Oid type)
{
	Oid argtyp[] = { OIDOID };
	FuncExpr *boundary;

	Oid boundary_func_oid =
		LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(BOUNDARY_FUNCTION)),
					   lengthof(argtyp),
					   argtyp,
					   false);
	List *func_args =
		list_make1(makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(ht_id), false, true));

	boundary = makeFuncExpr(boundary_func_oid,
							INT8OID,
							func_args,
							InvalidOid,
							InvalidOid,
							COERCE_EXPLICIT_CALL);

	return build_conversion_call(type, boundary);
}

static Node *
build_union_query_quals(int32 ht_id, Oid partcoltype, Oid opno, int varno, AttrNumber attno)
{
	Var *var = makeVar(varno, attno, partcoltype, -1, InvalidOid, InvalidOid);
	FuncExpr *boundary = build_boundary_call(ht_id, partcoltype);

	CoalesceExpr *coalesce = makeNode(CoalesceExpr);
	coalesce->coalescetype = partcoltype;
	coalesce->coalescecollid = InvalidOid;
	coalesce->args = list_make2(boundary, cagg_boundary_make_lower_bound(partcoltype));

	return (Node *) make_opclause(opno,
								  BOOLOID,
								  false,
								  (Expr *) var,
								  (Expr *) coalesce,
								  InvalidOid,
								  InvalidOid);
}

static RangeTblEntry *
make_subquery_rte(Query *subquery, const char *aliasname)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	ListCell *lc;

	rte->rtekind = RTE_SUBQUERY;
	rte->relid = InvalidOid;
	rte->subquery = subquery;
	rte->alias = makeAlias(aliasname, NIL);
	rte->eref = copyObject(rte->alias);

	foreach (lc, subquery->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (!tle->resjunk)
			rte->eref->colnames = lappend(rte->eref->colnames, makeString(pstrdup(tle->resname)));
	}

	rte->lateral = false;
	rte->inh = false; /* never true for subqueries */
	rte->inFromCl = true;

	return rte;
}

/*
 * build union query combining the materialized data with data from the raw data hypertable
 *
 * q1 is the query on the materialization hypertable with the finalize call
 * q2 is the query on the raw hypertable which was supplied in the inital CREATE VIEW statement
 */
static Query *
build_union_query(CAggTimebucketInfo *tbinfo, MatTableColumnInfo *mattblinfo, Query *q1, Query *q2)
{
	ListCell *lc1, *lc2;
	List *col_types = NIL;
	List *col_typmods = NIL;
	List *col_collations = NIL;
	List *tlist = NIL;
	int varno;
	AttrNumber attno;

	Assert(list_length(q1->targetList) == list_length(q2->targetList));

	q1 = copyObject(q1);
	q2 = copyObject(q2);

	TypeCacheEntry *tce = lookup_type_cache(tbinfo->htpartcoltype, TYPECACHE_LT_OPR);

	varno = list_length(q1->rtable);
	attno = mattblinfo->matpartcolno + 1;
	q1->jointree->quals =
		build_union_query_quals(tbinfo->htid, tbinfo->htpartcoltype, tce->lt_opr, varno, attno);
	attno =
		get_attnum(tbinfo->htoid, get_attname_compat(tbinfo->htoid, tbinfo->htpartcolno, false));
	varno = list_length(q2->rtable);
	q2->jointree->quals = build_union_query_quals(tbinfo->htid,
												  tbinfo->htpartcoltype,
												  get_negator(tce->lt_opr),
												  varno,
												  attno);

	Query *query = makeNode(Query);
	SetOperationStmt *setop = makeNode(SetOperationStmt);
	RangeTblEntry *rte_q1 = make_subquery_rte(q1, "*SELECT* 1");
	RangeTblEntry *rte_q2 = make_subquery_rte(q2, "*SELECT* 2");
	RangeTblRef *ref_q1 = makeNode(RangeTblRef);
	RangeTblRef *ref_q2 = makeNode(RangeTblRef);

	query->commandType = CMD_SELECT;
	query->rtable = list_make2(rte_q1, rte_q2);
	query->setOperations = (Node *) setop;

	setop->op = SETOP_UNION;
	setop->all = true;
	ref_q1->rtindex = 1;
	ref_q2->rtindex = 2;
	setop->larg = (Node *) ref_q1;
	setop->rarg = (Node *) ref_q2;

	forboth (lc1, q1->targetList, lc2, q2->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc1);
		TargetEntry *tle2 = lfirst_node(TargetEntry, lc2);
		TargetEntry *tle_union;
		Var *expr;

		if (!tle->resjunk)
		{
			col_types = lappend_int(col_types, exprType((Node *) tle->expr));
			col_typmods = lappend_int(col_typmods, exprTypmod((Node *) tle->expr));
			col_collations = lappend_int(col_collations, exprCollation((Node *) tle->expr));

			expr = makeVarFromTargetEntry(1, tle);
			/*
			 * we need to use resname from q2 because that is the query from the
			 * initial CREATE VIEW statement so the VIEW can be updated in place
			 */
			tle_union = makeTargetEntry((Expr *) copyObject(expr),
										list_length(tlist) + 1,
										tle2->resname,
										false);
			tle_union->resorigtbl = expr->varno;
			tle_union->resorigcol = expr->varattno;
			tlist = lappend(tlist, tle_union);
		}
	}

	query->targetList = tlist;

	setop->colTypes = col_types;
	setop->colTypmods = col_typmods;
	setop->colCollations = col_collations;

	return query;
}

/*
 * return Oid for a schema-qualified relation
 */
static Oid
relation_oid(NameData schema, NameData name)
{
	return get_relname_relid(NameStr(name), get_namespace_oid(NameStr(schema), false));
}
