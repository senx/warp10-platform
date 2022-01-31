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

import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.binary.EQ;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Return the percentile, given its rank and type, for the given GTS datapoints.
 * It implements the percentiles as defined in Hyndman, Rob & Fan, Yanan. (1996). Sample Quantiles in Statistical Packages. The American Statistician.
 */
public class Percentile extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {

  /**
   * For computing double "almost equal".
   */
  private static final double EPSILON = 0.000001;

  /**
   * Percentile type, see Hyndman, Rob & Fan, Yanan. (1996). Sample Quantiles in Statistical Packages. The American Statistician.
   */
  private final int type;

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

      int type = 1;

      // Parameter is a String here not to interfere with the percentile Number.
      if (top instanceof String) {
        String typeStr = ((String) top).toLowerCase();
        if (5 != typeStr.length() || !typeStr.startsWith("type")) {
          throw new WarpScriptException(getName() + " expect percentile type to be 'typeX' where 0<X<10.");
        }
        type = typeStr.charAt(4) - '0';

        if (type < 1 || 9 < type) {
          throw new WarpScriptException(getName() + " expect percentile type to be 'typeX' where X is a integer and 0<X<10.");
        }

        top = stack.pop();
      }

      if (!(top instanceof Number)) {
        throw new WarpScriptException("Invalid parameter for " + getName());
      }

      double percentile = ((Number) top).doubleValue();

      if (percentile < 0.0D || percentile > 100.0D) {
        throw new WarpScriptException("Invalid percentile for " + getName() + ", MUST be between 0 and 100.");
      }

      stack.push(new Percentile(getName(), percentile, type, this.forbidNulls));
      return stack;
    }
  }

  public Percentile(String name, double percentile, int type, boolean forbidNulls) {
    super(name);
    this.percentile = Math.min(100.0D, Math.max(0.0D, percentile));
    this.type = type;
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
      throw new WarpScriptException(this.getName() + " cannot compute percentile of null values.");
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

    //
    // Return the percentile depending on type.
    // As in the Hyndman paper we handle differently discontinuous types 1 to 3 and continuous types 4 to 9.
    // We first test type 1 because it is the default and can be optimized. It does not interpolate, gamma is either 0 or 1.
    // Types 2 and 3 must be handled separately because type 2 interpolates, gamma can take the value 0.5, while 3 does not.
    // Types 4 to 9 interpolate and only the m values changes so they are handled together.
    // Types 1 and 3, which do not interpolate, return the same type as the original GTS, either LONG or DOUBLE.
    // Other types return a DOUBLE GTS because they may interpolate between the original GTS datapoints.
    //

    if (1 == type) {
      // If we strictly follow the formula we would code the following, but it can be simplified using ceil.
      // int j = (int) Math.floor(pn);
      // if (pn > j) {
      //   j++;
      // }
      int j = (int) Math.ceil(pn);

      // Formula is 1-indexed, switch to 0-indexed.
      // If j==0 then the lowest value is taken, so keep j at 0.
      if (j > 0) {
        j--;
      }

      return new Object[] {ticks[indices[j]], locations[indices[j]], elevations[indices[j]], values[indices[j]]};
    } else if (2 >= type) {
      // Type 2

      int j = (int) Math.floor(pn);
      double g = pn - j;

      double gamma = 1;
      if (Math.abs(g) < EPSILON) {
        // g ~= 0
        gamma = 0.5;
      }

      // Formula is 1-indexed, switch to 0-indexed.
      j--;

      return interpolate(nonNullLength, j, gamma, indices, ticks, locations, elevations, values);
    } else if (3 >= type) {
      // Type 3
      double m = -0.5D;

      int j = (int) Math.floor(pn + m);
      double g = pn + m - j;

      // A little simplification here. gamma can only be 0 or 1 which will respectively return X(j) or X(j+1).
      // So we make gamma an integer and return X(j+gamma).
      int gamma = 1;
      if (Math.abs(g) < EPSILON && 0 == (j % 2)) {
        // g ~= 0 and j even
        gamma = 0;
      }

      // Formula is 1-indexed, switch to 0-indexed.
      // If (j + gamma)==0 then the lowest value is taken, so keep j + gamma at 0.
      if (j + gamma > 0) {
        j--;
      }

      return new Object[] {ticks[indices[j + gamma]], locations[indices[j + gamma]], elevations[indices[j + gamma]], values[indices[j + gamma]]};
    } else {
      // Type 4 to 9
      // Type 7 is used by defaut in R.
      // Type 8 is the recommended by Hyndman and Fan.
      // Except on small samples, the difference between types is negligible.

      double m;
      switch (type) {
        case 4:
          m = 0D;
          break;
        case 5:
          m = 0.5D;
          break;
        case 6:
          m = p;
          break;
        case 7:
          m = 1 - p;
          break;
        case 8:
          m = (p + 1) / 3D;
          break;
        case 9:
          m = p / 4D + 3.0D / 8.0D;
          break;
        default:
          throw new WarpScriptException(getName() + " given invalid type.");
      }

      int j = (int) Math.floor(pn + m);
      double gamma = pn + m - j;

      // Formula is 1-indexed, switch to 0-indexed.
      j--;

      return interpolate(nonNullLength, j, gamma, indices, ticks, locations, elevations, values);
    }
  }

  private Object[] interpolate(int nonNullLength, int j, double gamma, Integer[] indices, long[] ticks, long[] locations, long[] elevations, Object[] values) throws WarpScriptException {
    if (j < 0) {
      // j is -1, directly return the lowest value
      return new Object[] {ticks[indices[0]], locations[indices[0]], elevations[indices[0]], ((Number) values[indices[0]]).doubleValue()};
    }
    if (j >= nonNullLength - 1) {
      // j+1 is OOB, directly return highest value.
      return new Object[] {ticks[indices[nonNullLength - 1]], locations[indices[nonNullLength - 1]], elevations[indices[nonNullLength - 1]], ((Number) values[indices[nonNullLength - 1]]).doubleValue()};
    }

    if (j + 1 == nonNullLength || Math.abs(gamma) < EPSILON) {
      // gamma ~= 0, in that case, do not interpolate and return value at j
      return new Object[] {ticks[indices[j]], locations[indices[j]], elevations[indices[j]], ((Number) values[indices[j]]).doubleValue()};
    }

    if (Math.abs(gamma - 1) < EPSILON) {
      // gamma ~= 1, in that case, do not interpolate and return value at j+1
      return new Object[] {ticks[indices[j + 1]], locations[indices[j + 1]], elevations[indices[j + 1]], ((Number) values[indices[j + 1]]).doubleValue()};
    }

    // Check values are numeric
    if (!(values[indices[0]] instanceof Number)) {
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.percentile));
    sb.append(" ");
    sb.append("'type" + type + "'");
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
