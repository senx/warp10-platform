//
//   Copyright 2018-2020  SenX S.A.S.
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
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.Comparator;
import java.util.Arrays;

import com.geoxp.GeoXPLib;
import io.warp10.script.binary.EQ;

/**
 * Return the Nth percentile of the values on the interval.
 * The returned location will be that of the chosen value
 * The returned elevation will be that of the chosen value
 */
public class Percentile extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {

  /**
   * Should we use linear interpolation?
   */
  private final boolean interpolate;

  private final double percentile;
  private final boolean forbidNulls;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    private final boolean forbidNulls;

    public Builder(String name, boolean forbidNulls) {
      super(name);
      this.forbidNulls = forbidNulls;
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object top = stack.pop();

      boolean interpolate = false;

      if(top instanceof Boolean) {
        interpolate = (Boolean) top;
        top = stack.pop();
      }

      if (!(top instanceof Number)) {
        throw new WarpScriptException("Invalid parameter for " + getName());
      }

      double percentile = ((Number) top).doubleValue();

      if (percentile < 0.0D || percentile > 100.0D) {
        throw new WarpScriptException("Invalid percentile for " + getName() + ", MUST be between 0 and 100.");
      }

      stack.push(new Percentile(getName(), percentile, interpolate, this.forbidNulls));
      return stack;
    }
  }

  public Percentile(String name, double percentile, boolean interpolate, boolean forbidNulls) {
    super(name);
    this.percentile = Math.min(100.0D, Math.max(0.0D, percentile));
    this.interpolate = interpolate;
    this.forbidNulls = forbidNulls;
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    final Object[] values = (Object[]) args[6];

    if (0 == ticks.length) {
      return new Object[] {Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
    }


    //
    // count null value
    //
    int nullCounter = 0;
    for (Object v: values) {
      if (null == v) {
        nullCounter++;
      }
    }

    if (nullCounter != 0 && this.forbidNulls) {
      throw new WarpScriptException(this.getName() + " cannot compute median of null values.");
    }

    // Safeguard against all null values. Should not happen but just in case return null value.
    if (values.length == nullCounter) {
      return new Object[] {0, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
    }

    //
    // Sort the array indices by increasing value
    // FIXME(hbs): find something less memory hungry...
    //

    Integer[] indices = new Integer[values.length];
    for (int i = 0; i < indices.length; i++) {
      indices[i] = i;
    }

    final String functionName = this.getName();
    try {
      Arrays.sort(indices, new Comparator<Integer>() {
        @Override
        public int compare(Integer idx1, Integer idx2) {
          if (null == values[idx1] && null == values[idx2]) {
            return 0;
          } else if (null == values[idx1] || null == values[idx2]) {
            return null == values[idx1] ? 1 : -1;
          } else if (values[idx1] instanceof Number && values[idx2] instanceof Number) {
            return EQ.compare((Number) values[idx1], (Number) values[idx2]);
          } else {
            throw new RuntimeException(functionName + " can only operate on numeric Geo Time Series.");
          }
        }
      });
    } catch (RuntimeException re) {
      throw new WarpScriptException(re);
    }

    int nonNullLength = values.length - nullCounter;

    //
    // Compute rank
    //

    double p = this.percentile / 100.0D;

    double pn = p * nonNullLength;

    if (!this.interpolate) {
      // Type 1 in Hyndman, Rob & Fan, Yanan. (1996). Sample Quantiles in Statistical Packages. The American Statistician.
      int j = (int) Math.floor(pn);
      if (pn > j) {
        j++;
      }

      // Formula is 1-indexed, switch to 0-indexed.
      if (j > 0) {
        j--;
      }

      return new Object[] {ticks[indices[j]], locations[indices[j]], elevations[indices[j]], values[indices[j]]};
    } else {
      // Type 8 in Hyndman, Rob & Fan, Yanan. (1996). Sample Quantiles in Statistical Packages. The American Statistician.
      double m = (p + 1) / 3;
      int j = (int) Math.floor(pn + m);
      double gamma = pn + m - j;

      // Formula is 1-indexed, switch to 0-indexed.
      if (j > 0) {
        j--;
      } else {
        // j should be negative, directly return lowest value.
        return new Object[] {ticks[indices[j]], locations[indices[j]], elevations[indices[j]], values[indices[j]]};
      }

      if (j + 1 >= nonNullLength) {
        // j+1 should be over the number of valid values, directly return highest value.
        return new Object[] {ticks[indices[j]], locations[indices[j]], elevations[indices[j]], values[indices[j]]};
      }

      // Check values are numeric
      if (!(values[0] instanceof Number)) {
        throw new WarpScriptException(getName() + " can only interpolate on numeric values");
      }

      // Tick
      long tick = Math.round((1 - gamma) * ticks[indices[j]] + gamma * ticks[indices[j + 1]]);

      // Location
      long location;
      if (GeoTimeSerie.NO_LOCATION == locations[indices[j]]) {
        if (GeoTimeSerie.NO_LOCATION == locations[indices[j + 1]]) {
          location = GeoTimeSerie.NO_LOCATION;
        } else {
          location = locations[indices[j + 1]];
        }
      } else {
        if (GeoTimeSerie.NO_LOCATION == locations[indices[j + 1]]) {
          location = locations[indices[j]];
        } else {
          // Both locations are valid, interpolate.
          double[] latlonj = GeoXPLib.fromGeoXPPoint(locations[indices[j]]);
          double[] latlonjp1 = GeoXPLib.fromGeoXPPoint(locations[indices[j + 1]]);

          double lat = (1 - gamma) * latlonj[0] + gamma * latlonjp1[0];
          double lon = (1 - gamma) * latlonj[1] + gamma * latlonjp1[1];

          location = GeoXPLib.toGeoXPPoint(lat, lon);
        }
      }

      // Elevation
      long elevation;
      if (GeoTimeSerie.NO_ELEVATION == elevations[indices[j]]) {
        if (GeoTimeSerie.NO_ELEVATION == elevations[indices[j + 1]]) {
          elevation = GeoTimeSerie.NO_ELEVATION;
        } else {
          elevation = elevations[indices[j + 1]];
        }
      } else {
        if (GeoTimeSerie.NO_ELEVATION == elevations[indices[j + 1]]) {
          elevation = elevations[indices[j]];
        } else {
          // Both elevations are valid, interpolate.
          elevation = Math.round((1 - gamma) * elevations[indices[j]] + gamma * elevations[indices[j + 1]]);
        }
      }

      // Value
      double value = (1 - gamma) * ((Number) values[indices[j]]).doubleValue() + gamma * ((Number) values[indices[j + 1]]).doubleValue();

      return new Object[] {tick, location, elevation, value};
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.percentile));
    sb.append(" ");
    sb.append(StackUtils.toString(this.interpolate));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
