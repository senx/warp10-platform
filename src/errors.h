/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
/* Defines error codes used
-- PREFIX TS
*/

/*
-- TS000 - GROUP: query errors
-- TS001 - hypertable does not exist
-- TS002 - column does not exist
*/
#define ERRCODE_TS_QUERY_ERRORS MAKE_SQLSTATE('T', 'S', '0', '0', '0')
#define ERRCODE_TS_HYPERTABLE_NOT_EXIST MAKE_SQLSTATE('T', 'S', '0', '0', '1')
#define ERRCODE_TS_DIMENSION_NOT_EXIST MAKE_SQLSTATE('T', 'S', '0', '0', '2')

/*
--TS100 - GROUP: DDL errors
--TS101 - operation not supported
--TS102 - bad hypertable definition
--TS103 - bad hypertable index definition
--TS110 - hypertable already exists
--TS120 - node already exists
--TS130 - user already exists
*/
#define ERRCODE_TS_DDL_ERRORS MAKE_SQLSTATE('T', 'S', '1', '0', '0')
#define ERRCODE_TS_OPERATION_NOT_SUPPORTED MAKE_SQLSTATE('T', 'S', '1', '0', '1')
#define ERRCODE_TS_BAD_HYPERTABLE_DEFINITION MAKE_SQLSTATE('T', 'S', '1', '0', '2')
#define ERRCODE_TS_BAD_HYPERTABLE_INDEX_DEFINITION MAKE_SQLSTATE('T', 'S', '1', '0', '3')
#define ERRCODE_TS_HYPERTABLE_EXISTS MAKE_SQLSTATE('T', 'S', '1', '1', '0')
#define ERRCODE_TS_NODE_EXISTS MAKE_SQLSTATE('T', 'S', '1', '2', '0')
#define ERRCODE_TS_USER_EXISTS MAKE_SQLSTATE('T', 'S', '1', '3', '0')
#define ERRCODE_TS_TABLESPACE_ALREADY_ATTACHED MAKE_SQLSTATE('T', 'S', '1', '4', '0')
#define ERRCODE_TS_TABLESPACE_NOT_ATTACHED MAKE_SQLSTATE('T', 'S', '1', '5', '0')
#define ERRCODE_TS_DUPLICATE_DIMENSION MAKE_SQLSTATE('T', 'S', '1', '6', '0')

/*
--IO500 - GROUP: internal error
--IO501 - unexpected state/event
--IO502 - communication/remote error
*/
#define ERRCODE_TS_INTERNAL_ERROR MAKE_SQLSTATE('T', 'S', '5', '0', '0')
#define ERRCODE_TS_UNEXPECTED MAKE_SQLSTATE('T', 'S', '5', '0', '1')
#define ERRCODE_TS_COMMUNICATION_ERROR MAKE_SQLSTATE('T', 'S', '5', '0', '2')
