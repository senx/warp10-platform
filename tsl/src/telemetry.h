/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_TELEMETRY_H
#define TIMESCALEDB_TSL_TELEMETRY_H

#include <postgres.h>
#include <utils/jsonb.h>
void tsl_telemetry_add_license_info(JsonbParseState *parseState);

#endif /* TIMESCALEDB_TSL_TELEMETRY_H */
