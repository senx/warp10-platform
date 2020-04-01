-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SET timescaledb.license_key='CommunityLicense';

CREATE OR REPLACE FUNCTION test_reorder(job_id INTEGER)
RETURNS TABLE(
chunk_oid INTEGER,
index_oid INTEGER
)
AS :TSL_MODULE_PATHNAME, 'ts_test_auto_reorder'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION test_drop_chunks(job_id INTEGER)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_auto_drop_chunks'
LANGUAGE C VOLATILE STRICT;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE FUNCTION check_chunk_oid(chunk_id REGCLASS, chunk_oid REGCLASS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	count INTEGER;
BEGIN
	select count(*) from pg_class as pgc, _timescaledb_catalog.chunk as c where pgc.relname=c.table_name and c.id=chunk_id and pgc.oid=chunk_oid INTO count;
	return (count = 1);
END
$BODY$;

CREATE FUNCTION check_index_oid(index_oid REGCLASS, hypertable_oid REGCLASS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	count INTEGER;
BEGIN
	select count(*) from pg_index where indexrelid=index_oid and indrelid=hypertable_oid INTO count;
	return (count = 1);
END
$BODY$;

CREATE TABLE test_table(time timestamptz, chunk_id int);
SELECT create_hypertable('test_table', 'time');

-- These inserts should create 5 different chunks
INSERT INTO test_table VALUES (now() - INTERVAL '3 weeks', 1);
INSERT INTO test_table VALUES (now(), 2);
INSERT INTO test_table VALUES (now() - INTERVAL '5 months', 3);
INSERT INTO test_table VALUES (now() - INTERVAL '3 months', 4);
INSERT INTO test_table VALUES (now() - INTERVAL '8 months', 5);

SELECT COUNT(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table';

-- Make sure reorder correctly selects chunks to reorder
-- by starting with oldest chunks
select add_reorder_policy('test_table', 'test_table_time_idx') as reorder_job_id \gset
select * from _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;

select * from _timescaledb_config.bgw_job where job_type IN ('reorder');
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

-- Make a manual calls to reorder: make sure the correct chunk is called
-- Chunk 5 should be first
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

-- Confirm that reorder was called on the correct chunk Oid
select check_chunk_oid(5, :reorder_chunk_oid);
select check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- Chunk 3 is next
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select check_chunk_oid(3, :reorder_chunk_oid);
select check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- Chunk 4 is next
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select check_chunk_oid(4, :reorder_chunk_oid);
select check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- The following calls should not reorder any chunk, because they're all too new
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

INSERT INTO test_table VALUES (now() - INTERVAL '7 days', 6);

-- This call should reorder chunk 1
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select check_chunk_oid(1, :reorder_chunk_oid);
select check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- Should not reorder anything, because all chunks are too new
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

select remove_reorder_policy('test_table');

-- Now do drop_chunks test
select add_drop_chunks_policy('test_table', INTERVAL '4 months', true) as drop_chunks_job_id \gset

SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table';

-- Now simulate drop_chunks running automatically by calling it explicitly
select test_drop_chunks(:drop_chunks_job_id);
-- Should have 4 chunks left
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset before_
select :before_count=4;

-- Make sure this second call does nothing
select test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset after_

-- Should be true
select :before_count=:after_count;

INSERT INTO test_table VALUES (now() - INTERVAL '2 weeks', 1);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset before_

-- This call should also do nothing
select test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset after_

-- Should be true
select :before_count=:after_count;

select remove_drop_chunks_policy('test_table');

-- Now test reorder chunk selection when there is space partitioning
TRUNCATE test_table;
SELECT add_dimension('public.test_table', 'chunk_id', 2);

INSERT INTO test_table VALUES (now() - INTERVAL '3 weeks', 1);
INSERT INTO test_table VALUES (now(), 2);
INSERT INTO test_table VALUES (now() - INTERVAL '5 months', 3);
INSERT INTO test_table VALUES (now() - INTERVAL '3 months', 4);
INSERT INTO test_table VALUES (now() - INTERVAL '3 months', -4);
INSERT INTO test_table VALUES (now() - INTERVAL '8 months', 5);
INSERT INTO test_table VALUES (now() - INTERVAL '8 months', -5);

select add_reorder_policy('test_table', 'test_table_time_idx') as reorder_job_id \gset
-- Should be nothing in the chunk_stats table
select count(*) from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id;

-- Make a manual calls to reorder: make sure the correct (oldest) chunk is called
select chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

-- Confirm that reorder was called on the correct chunk Oid
select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Now run reorder again and pick the next oldest chunk
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

-- Confirm that reorder was called on the correct chunk Oid
select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Again
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Again
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Again
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Ran out of chunks, so should be a noop
select * from test_reorder(:reorder_job_id) \gset  reorder_

-- Corner case: when there are no recent-enough chunks to reorder,
-- DO NOT reorder any new chunks created by space partitioning.
-- We only want to reorder when new dimension_slices on time are created.
INSERT INTO test_table VALUES (now() - INTERVAL '5 months', -5);
INSERT INTO test_table VALUES (now() - INTERVAL '3 weeks', -5);
INSERT INTO test_table VALUES (now(), -25);

-- Should be noop
select * from test_reorder(:reorder_job_id) \gset  reorder_

-- But if we create a new time dimension, reorder it
INSERT INTO test_table VALUES (now() - INTERVAL '1 year', 1);
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Should be noop again
select * from test_reorder(:reorder_job_id) \gset  reorder_

CREATE TABLE test_table_int(time bigint, junk int);
CREATE TABLE test_overflow_smallint(time smallint, junk int);

SELECT create_hypertable('test_table_int', 'time', chunk_time_interval => 1);
SELECT create_hypertable('test_overflow_smallint', 'time', chunk_time_interval => 1);

create or replace function dummy_now() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 1::BIGINT';
create or replace function dummy_now2() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 2::BIGINT';
create or replace function overflow_now() returns SMALLINT LANGUAGE SQL IMMUTABLE as  'SELECT 32767::SMALLINT';

CREATE TABLE test_table_perm(time timestamp PRIMARY KEY);
SELECT create_hypertable('test_table_perm', 'time', chunk_time_interval => 1);

\set ON_ERROR_STOP 0
-- we cannot add a drop_chunks policy on a table whose open dimension is not time and no now_func is set
select add_drop_chunks_policy('test_table_int', INTERVAL '4 months', true);
\set ON_ERROR_STOP 1

INSERT INTO test_table_int VALUES (-2, -2), (-1, -1), (0,0), (1, 1), (2, 2), (3, 3);
\c :TEST_DBNAME :ROLE_SUPERUSER;
CREATE USER unprivileged;
\c :TEST_DBNAME unprivileged
-- should fail as the user has no permissions on the table
\set ON_ERROR_STOP 0
select set_integer_now_func('test_table_int', 'dummy_now');
\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_SUPERUSER;
DROP USER unprivileged;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

select set_integer_now_func('test_table_int', 'dummy_now');
select * from test_table_int;
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table_int';
select add_drop_chunks_policy('test_table_int', 1, true) as drop_chunks_job_id \gset

-- Now simulate drop_chunks running automatically by calling it explicitly
select test_drop_chunks(:drop_chunks_job_id);
select * from test_table_int;
-- Should have 4 chunks left
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table_int' \gset before_
select :before_count=4;

-- Make sure this second call does nothing
select test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table_int' \gset after_

-- Should be true
select :before_count=:after_count;

INSERT INTO test_table_int VALUES (42, 42);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table_int' \gset before_

-- This call should also do nothing
select test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table_int' \gset after_

-- Should be true
select :before_count=:after_count;

INSERT INTO test_table_int VALUES (-1, -1);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table_int' \gset add_one_
select :before_count+1=:add_one_count;
select test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table_int' \gset after_

-- (-1,-1) was in droping range so it should be dropped by background job
select :before_count=:after_count;

select set_integer_now_func('test_table_int', 'dummy_now2', replace_if_exists=>true);
select * from test_table_int;
select test_drop_chunks(:drop_chunks_job_id);
-- added one to now() so time entry with value 0 should be dropped now
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table_int' \gset after_
select :before_count=:after_count+1;
select * from test_table_int;

-- make the now() function invalid -- returns INT and not BIGINT
drop function dummy_now2();
create or replace function dummy_now2() returns INT LANGUAGE SQL IMMUTABLE as  'SELECT 2::INT';

\set ON_ERROR_STOP 0
select test_drop_chunks(:drop_chunks_job_id);
\set ON_ERROR_STOP 1

-- test the expected use case of set_integer_now_func
 create function nowstamp() returns bigint language sql STABLE as 'SELECT extract(epoch from now())::BIGINT';
select set_integer_now_func('test_table_int', 'nowstamp', replace_if_exists=>true);
select test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table_int' \gset after_
select :after_count=0;

-- test the case when now()-interval overflows
select set_integer_now_func('test_overflow_smallint', 'overflow_now');
select add_drop_chunks_policy('test_overflow_smallint', -2) as drop_chunks_job_id \gset
\set ON_ERROR_STOP 0
select test_drop_chunks(:drop_chunks_job_id);
\set ON_ERROR_STOP 1


-- test the case when partitioning function and now function are set.
create table part_time_now_func(time float8, temp float8);

create or replace function time_partfunc(unixtime float8)
    returns bigint language plpgsql immutable as
$body$
declare
    retval bigint;
begin

    retval := unixtime::bigint;
    raise notice 'time value for % is %', unixtime, retval;
    return retval;
end
$body$;
create or replace function dummy_now() returns bigint language sql immutable as  'select 2::bigint';

select create_hypertable('part_time_now_func', 'time', time_partitioning_func => 'time_partfunc', chunk_time_interval=>1);
insert into part_time_now_func values
(1.1, 23.4), (2.2, 22.3), (3.3, 42.3);
select * from part_time_now_func;
select set_integer_now_func('part_time_now_func', 'dummy_now');
select add_drop_chunks_policy('part_time_now_func', 0) as drop_chunks_job_id \gset
select test_drop_chunks(:drop_chunks_job_id);
select * from part_time_now_func;
select remove_drop_chunks_policy('part_time_now_func');
\c :TEST_DBNAME :ROLE_SUPERUSER
-- todo:: this currently fails:
-- alter function dummy_now rename to dummy_now_renamed;
alter schema public rename to new_public;
select * from  _timescaledb_catalog.dimension;
alter schema new_public rename to public;

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
select add_reorder_policy('test_table_perm', 'test_table_perm_pkey');
select remove_reorder_policy('test_table');

select add_drop_chunks_policy('test_table_perm', INTERVAL '4 months', true);
select remove_drop_chunks_policy('test_table');

\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
