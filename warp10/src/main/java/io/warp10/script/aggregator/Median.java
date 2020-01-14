//
//   Copyright 2020  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.script.aggregator;

import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.binary.EQ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

/**
 * Return the median of the values on the interval.
 * The returned location will be the median of all locations.
 * The returned elevation will be the median of all elevations.
 */
public class Median extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {

  public Median(String name) {
    super(name);
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    //
    // Remove NO_LOCATION, keep valid latitude and longitude
    // Compute median of latitude, median of longitude
    //
    ArrayList<Double> validLat = new ArrayList<Double>(locations.length);
    ArrayList<Double> validLong = new ArrayList<Double>(locations.length);

    for (int i = 0; i < locations.length; i++) {
      if (GeoTimeSerie.NO_LOCATION != locations[i]) {
        validLat.add(GeoXPLib.fromGeoXPPoint(locations[i])[0]);
        validLong.add(GeoXPLib.fromGeoXPPoint(locations[i])[1]);
      }
    }

    long location;

    if (0 == validLat.size()) {
      location = GeoTimeSerie.NO_LOCATION;
    } else if (1 == validLat.size()) {
      location = GeoXPLib.toGeoXPPoint(validLat.get(0), validLong.get(0));
    } else {
      Collections.sort(validLat);
      Collections.sort(validLong);
      Double medianLatitude;
      Double medianLongitude;
      int len = validLat.size();
      if (0 == len % 2) {
        medianLatitude = (validLat.get(len / 2) + validLat.get(len / 2 - 1)) / 2.0D;
        medianLongitude = (validLong.get(len / 2) + validLong.get(len / 2 - 1)) / 2.0D;
      } else {
        medianLatitude = validLat.get(len / 2);
        medianLongitude = validLong.get(len / 2);
      }
      location = GeoXPLib.toGeoXPPoint(medianLatitude, medianLongitude);
    }

    //
    // Sort elevations.
    // As NO_ELEVATION == Long.MIN_VALUE, sorting elevations is a way to exclude NO_ELEVATION efficiently.
    // Compute elevation median
    //
    long elevation;
    Arrays.sort(elevations);

    if (elevations[0] == elevations[elevations.length - 1]) {
      elevation = elevations[0];
    } else {
      // set offset to the first valid elevation
      int offset = 0;
      while (offset < elevations.length && GeoTimeSerie.NO_ELEVATION == elevations[offset]) {
        offset++;
      }
      if (offset == elevations.length) {
        // there is no valid elevation
        elevation = GeoTimeSerie.NO_ELEVATION;
      } else if (offset == elevations.length - 1) {
        // there is only one valid elevation
        elevation = elevations[offset];
      } else {
        // compute elevation median in the end of the array
        int len = elevations.length - offset;
        if (0 == len % 2) {
          elevation = (elevations[offset + (len / 2)] + elevations[offset + ((len / 2) - 1)]) / 2L;
        } else {
          elevation = elevations[offset + (len / 2)];
        }
      }
    }

    //
    // Remove nulls, NaN
    // Fail on non numeric values.
    //
    ArrayList<Number> validValues = new ArrayList<Number>(values.length);
    for (int i = 0; i < values.length; i++) {
      if (null != values[i]) {
        if (values[i] instanceof Number) {
          if (!(values[i] instanceof Double && Double.isNaN((Double) values[i]))) {
            validValues.add((Number) values[i]);
          }
        } else {
          throw new WarpScriptException(this.getName() + " cannot compute median of non numeric values.");
        }
      }
    }

    //
    // Sort values
    // Could be Long or Double, or a mix, if used as a reducer.
    //
    Collections.sort(validValues, new Comparator<Number>() {
      @Override
      public int compare(Number o1, Number o2) {
        return EQ.compare(o1, o2);
      }
    });


    Object median = null;

    if (0 != validValues.size()) {
      if (validValues.get(0).equals(validValues.get(validValues.size() - 1))) {
        // If extrema are identical, use this as the median
        median = validValues.get(0);
      } else {
        int len = validValues.size();
        if (0 == len % 2) {
          Object low = validValues.get((len / 2) - 1);
          Object high = validValues.get(len / 2);
          if (low instanceof Long && high instanceof Long) {
            median = ((long) low + (long) high) / 2L;
          } else {
            median = ((double) low + (double) high) / 2.0D;
          }
        } else {
          median = validValues.get(len / 2);
        }
      }
    }

    return new Object[]{tick, location, elevation, median};
  }
}
