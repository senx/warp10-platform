//
//   Copyright 2021  SenX S.A.S.
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

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Aggregator to compute the variance on a list of datapoints.
 * This implements Welford's algorithm for numerical stability, see
 * https://www.johndcook.com/blog/2008/09/26/comparing-three-methods-of-computing-standard-deviation/
 */
public class VarianceWelford extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptReducerFunction, WarpScriptBucketizerFunction {

  private final boolean useBessel;
  private final boolean forbidNulls;

  public VarianceWelford(String name, boolean useBessel, boolean forbidNulls) {
    super(name);
    this.useBessel = useBessel;
    this.forbidNulls = forbidNulls;
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

    double m = 0.0D;
    double s = 0.0D;

    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    long timestamp = Long.MIN_VALUE;

    int nticks = 0;

    for (int i = 0; i < values.length; i++) {
      Object value = values[i];

      if (null == value) {
        if (this.forbidNulls) {
          return new Object[] {Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
        } else {
          continue;
        }
      }

      nticks++;

      if (ticks[i] > timestamp) {
        location = locations[i];
        elevation = elevations[i];
        timestamp = ticks[i];
      }

      if (value instanceof Number) {
        double x = ((Number) value).doubleValue();
        if (0 == i) {
          m = x;
          s = 0.0D;
        } else {
          double mnew = m + (x - m) / (i + 1);
          s = s + (x - m) * (x - mnew);
          m = mnew;
        }
      } else {
        //
        // Mean of String or Boolean has no meaning
        //
        return new Object[] {Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
      }
    }

    //
    // Compute variance
    //

    double variance;

    if (nticks > 1 && useBessel) {
      // Apply Bessel's correction
      // @see <a href="http://en.wikipedia.org/wiki/Bessel's_correction">http://en.wikipedia.org/wiki/Bessel's_correction</a>
      variance = s / (nticks - 1);
    } else {
      variance = s / nticks;
    }

    return new Object[] { 0L, location, elevation, variance };
  }

  @Override
  public String toString() {
    return Boolean.toString(this.forbidNulls) + " " + this.getName();
  }
}
