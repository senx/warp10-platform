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

package io.warp10.script.mapper;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Wrap a Mapper so empty buckets are filled by the mapper and non-empty buckets are left untouched
 */
public class FILLMAPPER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public FILLMAPPER(String name) {
    super(name);
  }

  public static final class FillerMapper extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

    private final WarpScriptMapperFunction mapper;

    public FillerMapper(String name, WarpScriptMapperFunction mapper) {
      super(name);
      this.mapper = mapper;
    }

    @Override
    public Object apply(Object[] args) throws WarpScriptException {

      long[] ticks = (long[]) args[3];
      long tick = (long) args[0];
      int tick_index = (int) ((long[]) args[7])[4];

      // if the tick is missing in the window, apply mapper to fill it
      if (-1 == tick_index) {
        return mapper.apply(args);
      }

      // if tick is present, set back the previous point
      return new Object[]{args[0], ((long[]) args[4])[tick_index], ((long[]) args[5])[tick_index], ((Object[]) args[6])[tick_index]};
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(mapper.toString());
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

    Object o = stack.pop(); // mapper

    if (!(o instanceof WarpScriptMapperFunction)) {
      throw new WarpScriptException(getName() + " expects a mapper.");
    }

    WarpScriptMapperFunction mapper = (WarpScriptMapperFunction) o;

    stack.push(new FillerMapper(getName(), mapper));

    return stack;
  }
}
