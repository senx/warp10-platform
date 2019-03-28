//
//   Copyright 2018  SenX S.A.S.
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

package io.warp10.script.mapper;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Wrap a Mapper so we return no value when the number of values in the
 * window is outside a given range
 */
public class STRICTMAPPER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public STRICTMAPPER(String name) {
    super(name);
  }


  private static final class StringentMapper extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

    private final long min;
    private final long max;
    private final WarpScriptMapperFunction mapper;
    private final boolean isOnCount;

    public StringentMapper(String name, long min, long max, WarpScriptMapperFunction mapper, boolean isOnCount) {
      super(name);
      this.min = min;
      this.max = max;
      this.mapper = mapper;
      this.isOnCount = isOnCount;
    }

    @Override
    public Object apply(Object[] args) throws WarpScriptException {

      long[] ticks = (long[]) args[3];

      long size;

      if (isOnCount) {
        size = ticks.length;
      } else {
        if (0 == ticks.length) {
          size = 0;
        } else {
          size = ticks[ticks.length - 1] - ticks[0] + 1;
        }
      }

      if (size < min || size > max) {
        return new Object[]{args[0], GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
      }

      return mapper.apply(args);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(mapper.toString());
      sb.append(" ");
      sb.append(min * (isOnCount ? 1 : -1));
      sb.append(" ");
      sb.append(max * (isOnCount ? 1 : -1));
      sb.append(" ");
      sb.append(getName());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      return sb.toString();
    }
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop(); // maxpoints or -maxspan

    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a maximum (inclusive) number of values on top of the stack.");
    }


    long max = ((Number) o).longValue();

    o = stack.pop(); // minpoints or -minspan

    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a minimum (inclusive) number of values below the top of the stack.");
    }

    long min = ((Number) o).longValue();

    boolean isOnCount = true;
    if (min < 0 || max < 0) { // Either min or max is strictly negative -> timespan definition
      if(min > 0 || max > 0) { // Either min or max is strictly positive -> error
        throw new WarpScriptException(getName() + " expects min and max to be of the same sign.");
      }

      isOnCount = false;

      // Safeguard over long overflow when negating
      if (Long.MIN_VALUE == min) {
        min++;
      }
      if (Long.MIN_VALUE == max) {
        max++;
      }

      min = -min;
      max = -max;
    }

    if (min > max) {
      throw new WarpScriptException(getName() + " expects abs(min) <= abs(max).");
    }

    o = stack.pop(); // mapper

    if (!(o instanceof WarpScriptMapperFunction)) {
      throw new WarpScriptException(getName() + " expects a mapper below the extrema defining the value count range.");
    }

    WarpScriptMapperFunction mapper = (WarpScriptMapperFunction) o;

    stack.push(new StringentMapper(getName(), min, max, mapper, isOnCount));

    return stack;
  }
}
