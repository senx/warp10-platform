-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set TEST_DBNAME_2 :TEST_DBNAME _2

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE DATABASE :TEST_DBNAME_2;

DROP EXTENSION timescaledb;
--no extension
\dx
SELECT 1;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE EXTENSION timescaledb VERSION 'mock-1';
SELECT 1;
\dx

CREATE EXTENSION IF NOT EXISTS timescaledb VERSION 'mock-1';
CREATE EXTENSION IF NOT EXISTS timescaledb VERSION 'mock-2';

DROP EXTENSION timescaledb;
\set ON_ERROR_STOP 0
--test that we cannot accidentally load another library version
CREATE EXTENSION IF NOT EXISTS timescaledb VERSION 'mock-2';
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_SUPERUSER
--no extension
\dx
SELECT 1;

CREATE EXTENSION timescaledb VERSION 'mock-1';
--same backend as create extension;
SELECT 1;
\dx

--start new backend;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT 1;
SELECT 1;
--test fn call after load
SELECT mock_function();
\dx

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
--test fn call as first command
SELECT mock_function();

--use guc to prevent loading
\c :TEST_DBNAME :ROLE_SUPERUSER
SET timescaledb.disable_load = 'on';
SELECT 1;
SELECT 1;
SET timescaledb.disable_load = 'off';
SELECT 1;
\set ON_ERROR_STOP 0
SET timescaledb.disable_load = 'not bool';
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_SUPERUSER
RESET ALL;
SELECT 1;

\c :TEST_DBNAME :ROLE_SUPERUSER
SET timescaledb.disable_load TO DEFAULT;
SELECT 1;

\c :TEST_DBNAME :ROLE_SUPERUSER
RESET timescaledb.disable_load;
SELECT 1;

\c :TEST_DBNAME :ROLE_SUPERUSER
SET timescaledb.other = 'on';
SELECT 1;

\set ON_ERROR_STOP 0
--cannot update extension after .so of previous version already loaded
ALTER EXTENSION timescaledb UPDATE TO 'mock-2';
\set ON_ERROR_STOP 1

\c :TEST_DBNAME_2 :ROLE_SUPERUSER
\dx
CREATE EXTENSION timescaledb VERSION 'mock-1';
\dx
--start a new backend to update
\c :TEST_DBNAME_2 :ROLE_SUPERUSER
ALTER EXTENSION timescaledb UPDATE TO 'mock-2';
SELECT 1;
\dx

--drop extension
DROP EXTENSION timescaledb;
SELECT 1;
\dx


\c :TEST_DBNAME_2 :ROLE_SUPERUSER
CREATE EXTENSION timescaledb VERSION 'mock-2';
SELECT 1;
\dx

-- test db 1 still has old version
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT 1;
\dx

--try a broken upgrade
\c :TEST_DBNAME_2 :ROLE_SUPERUSER
\dx
\set ON_ERROR_STOP 0
ALTER EXTENSION timescaledb UPDATE TO 'mock-3';
\set ON_ERROR_STOP 1
--should still be on mock-2
SELECT 1;
\dx

--drop extension
DROP EXTENSION timescaledb;
SELECT 1;
\dx

--create extension anew, only upgrade was broken
\c :TEST_DBNAME_2 :ROLE_SUPERUSER
CREATE EXTENSION timescaledb VERSION 'mock-3';
SELECT 1;
\dx
DROP EXTENSION timescaledb;
SELECT 1;

--mismatched version errors
\c :TEST_DBNAME_2 :ROLE_SUPERUSER
--mock-4 has mismatched versions, so the .so load should be fatal
SELECT format($$\! utils/test_fatal_command.sh %1$s "CREATE EXTENSION timescaledb VERSION 'mock-4'"$$, :'TEST_DBNAME_2') as command_to_run \gset
:command_to_run
--mock-4 not installed.
\dx

\c :TEST_DBNAME_2 :ROLE_SUPERUSER
--broken version and drop
CREATE EXTENSION timescaledb VERSION 'mock-broken';

\set ON_ERROR_STOP 0
--intentional broken version
\dx
SELECT 1;
SELECT 1;
--cannot drop extension; already loaded broken version
DROP EXTENSION timescaledb;
\set ON_ERROR_STOP 1

\c :TEST_DBNAME_2 :ROLE_SUPERUSER
--can drop extension now. Since drop first command.
DROP EXTENSION timescaledb;
\dx

--broken version and update to fixed
\c :TEST_DBNAME_2 :ROLE_SUPERUSER
CREATE EXTENSION timescaledb VERSION 'mock-broken';
\set ON_ERROR_STOP 0
--intentional broken version
SELECT 1;
--cannot update extension; already loaded bad version
ALTER EXTENSION timescaledb UPDATE TO 'mock-5';
\set ON_ERROR_STOP 1

\c :TEST_DBNAME_2 :ROLE_SUPERUSER
--can update extension now.
ALTER EXTENSION timescaledb UPDATE TO 'mock-5';
SELECT 1;
SELECT mock_function();

\c :TEST_DBNAME_2 :ROLE_SUPERUSER
ALTER EXTENSION timescaledb UPDATE TO 'mock-6';
--The mock-5->mock_6 upgrade is intentionally broken.
--The mock_function was never changed to point to mock-6 in the update script.
--Thus mock_function is defined incorrectly to point to the mock-5.so
--This will now be a FATAL error.
SELECT format($$\! utils/test_fatal_command.sh %1$s "SELECT mock_function()"$$, :'TEST_DBNAME_2') as command_to_run \gset
:command_to_run
\dx

--TEST: create extension when old .so already loaded
\c :TEST_DBNAME :ROLE_SUPERUSER
--force load of extension with (\dx)
\dx
DROP EXTENSION timescaledb;
\dx

\set ON_ERROR_STOP 0
CREATE EXTENSION timescaledb VERSION 'mock-2';
\set ON_ERROR_STOP 1
\dx
--can create in a new session.
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE EXTENSION timescaledb VERSION 'mock-2';
\dx

--make sure parallel workers started after a 'DISCARD ALL' work
CREATE TABLE test (i int, j double precision);
INSERT INTO test SELECT x, x+0.1 FROM generate_series(1,100) AS x;

DISCARD ALL;
SET force_parallel_mode = 'on';
SET max_parallel_workers_per_gather = 1;
SELECT count(*) FROM test;

-- clean up additional database
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE :TEST_DBNAME_2;

