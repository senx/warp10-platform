//
//Copyright 2016  Cityzen Data
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//

package io.warp10.script.aggregator;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptException;

import com.geoxp.GeoXPLib;

/**
 * Return the operation or of the values on the interval.
 * The returned location will be the centroid of all locations.
 * The returned elevation will be the average of all elevations.
 */
public class Or extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {

  private final boolean ignoreNulls;

  public Or(String name, boolean ignoreNulls) {
    super(name);
    this.ignoreNulls = ignoreNulls;
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (0 == ticks.length) {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }

    TYPE orType = TYPE.UNDEFINED;
    boolean or = false;
    long tickor = 0L;
    long latitudes = 0L;
    long longitudes = 0L;
    int locationcount = 0;
    long elev = 0L;
    int elevationcount = 0;

    int nulls = 0;

    for (int i = 0; i < values.length; i++) {
      Object value = values[i];

      if (null == value) {
        nulls++;
        continue;
        //return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
      } else {

        tickor += ticks[i] - ticks[0];

        if (GeoTimeSerie.NO_LOCATION != locations[i]) {
          long[] xy = GeoXPLib.xyFromGeoXPPoint(locations[i]);
          latitudes += xy[0];
          longitudes += xy[1];
          locationcount++;
        }

        if (GeoTimeSerie.NO_ELEVATION != elevations[i]) {
          elev += elevations[i];
          elevationcount++;
        }

        or = or || Boolean.TRUE.equals(values[i]);
      }
    }

    long meanlocation = GeoTimeSerie.NO_LOCATION;
    long meanelevation = GeoTimeSerie.NO_ELEVATION;

    if (locationcount > 0) {
      latitudes = latitudes / locationcount;
      longitudes = longitudes / locationcount;
      meanlocation = GeoXPLib.toGeoXPPoint(latitudes, longitudes);
    }

    if (elevationcount > 0) {
      meanelevation = elev / elevationcount;
    }

    //
    // If we should not ignore nulls and there were some nulls, return null
    //

    if (!ignoreNulls && nulls > 0) {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }

    return new Object[] { ticks[0] + (tickor / ticks.length), meanlocation, meanelevation, or };
  }
}