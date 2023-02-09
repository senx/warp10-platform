//
//   Copyright 2018-2021  SenX S.A.S.
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

import io.warp10.continuum.gts.Aggregate;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapper;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.SNAPSHOT;

import java.util.ArrayList;
import java.util.List;

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

    public StringentMapper(String name, long min, long max, WarpScriptMapperFunction mapper) {
      super(name);
      this.min = min;
      this.max = max;
      this.mapper = mapper;
    }

    @Override
    public Object apply(Object[] args) throws WarpScriptException {

      long[] ticks = (long[]) args[3];

      long timespan;
      if (0 == ticks.length) {
        timespan = 0;
      } else {
        timespan = ticks[ticks.length - 1] - ticks[0] + 1;
      }

      if (min > 0 && ticks.length < min || max > 0 && ticks.length > max || min < 0 && timespan < -min || max < 0 && timespan > -max) {
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
      sb.append(min);
      sb.append(" ");
      sb.append(max);
      sb.append(" ");
      sb.append(getName());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      return sb.toString();
    }
  }

  private static final class StringentMapper2 extends NamedWarpScriptFunction implements WarpScriptMapper, SNAPSHOT.Snapshotable {
    private final long min;
    private final long max;
    private final WarpScriptMapper mapper;

    public StringentMapper2(String name, long min, long max, WarpScriptMapper mapper) {
      super(name);
      this.min = min;
      this.max = max;
      this.mapper = mapper;
    }

    @Override
    public Object apply(Aggregate aggregate) throws WarpScriptException {

      List<Long> ticks = aggregate.getTicks();
      long timespan;
      if (0 == ticks.size()) {
        timespan = 0;
      } else {
        timespan = ticks.get(ticks.size() - 1) - ticks.get(0) + 1;
      }

      if (min > 0 && ticks.size() < min || max > 0 && ticks.size() > max || min < 0 && timespan < -min || max < 0 && timespan > -max) {
        return new Object[]{aggregate.getReferenceTick(), GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
      }

      return mapper.apply(aggregate);
    }

    @Override
    public String snapshot() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(mapper.toString());
      sb.append(" ");
      sb.append(min);
      sb.append(" ");
      sb.append(max);
      sb.append(" ");
      sb.append(getName());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      return sb.toString();
    }
  }

  private static final class StringentMacroAsMapperFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    private final long min;
    private final long max;
    private final WarpScriptStack.Macro macro;

    public StringentMacroAsMapperFunction(String name, long min, long max, WarpScriptStack.Macro macro) {
      super(name);
      this.min = min;
      this.max = max;
      this.macro = macro;
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {

      Object o = stack.peek();
      if (!(o instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + "'s resulting function can only be applied on a GTS.");
      }

      GeoTimeSerie gts = (GeoTimeSerie) o;
      int size = gts.size();
      long timespan = 0;
      if (0 < size) {
        timespan = GTSHelper.lasttick(gts) - GTSHelper.firsttick(gts) + 1;
      }

      if (min > 0 && size < min || max > 0 && size > max || min < 0 && timespan < -min || max < 0 && timespan > -max) {
        stack.pop();

        List<Object> list = new ArrayList<Object>(1);
        list.add(null);
        stack.push(list);
        return stack;
      }

      stack.exec(macro);

      return stack;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(macro.snapshot());
      sb.append(" ");
      sb.append(min);
      sb.append(" ");
      sb.append(max);
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
      throw new WarpScriptException(getName() + " expects a maximum (inclusive) number of values or a minimum timespan on top of the stack.");
    }

    long max = ((Number) o).longValue();

    o = stack.pop(); // minpoints or -minspan

    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a minimum (inclusive) number of values or a minimum timespan below the top of the stack.");
    }

    long min = ((Number) o).longValue();

    // Safeguard over long overflow when negating
    if (Long.MIN_VALUE == min) {
      min++;
    }
    if (Long.MIN_VALUE == max) {
      max++;
    }

    if(min > 0 && max >= 0 || min < 0 && max <= 0) {
      if(Math.abs(min) > Math.abs(max)){
        throw new WarpScriptException(getName() + " expects abs(min) <= abs(max) when min and max both express a count or both express a duration");
      }
    }

    o = stack.pop(); // mapper

    if (o instanceof WarpScriptMapperFunction) {
      WarpScriptMapperFunction mapper = (WarpScriptMapperFunction) o;
      stack.push(new StringentMapper(getName(), min, max, mapper));

    } else if (o instanceof WarpScriptMapper) {
      WarpScriptMapper mapper = (WarpScriptMapper) o;
      stack.push(new StringentMapper2(getName(), min, max, mapper));

    } else if (o instanceof WarpScriptStack.Macro) {
      WarpScriptStack.Macro macro = new WarpScriptStack.Macro();
      macro.add(new StringentMacroAsMapperFunction(getName(), min, max, (WarpScriptStack.Macro) o));
      stack.push(macro);

    } else {
      throw new WarpScriptException(getName() + " expects a mapper or a macro below the extrema defining the value count range or timespan.");
    }

    return stack;
  }
}
