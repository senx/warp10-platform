//
//   Copyright 2020-2021  SenX S.A.S.
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

package io.warp10.script.filter;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterBySize extends NamedWarpScriptFunction implements WarpScriptFilterFunction {

  private final int min;
  private final int max;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    public Builder(String name) {
      super((name));
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {

      Object o2 = stack.pop();
      Object o1 = stack.pop();

      if (!(o1 instanceof Long) || !(o2 instanceof Long)) {
        throw new WarpScriptException(getName() + " expects two LONG objects as arguments.");
      }

      stack.push(new FilterBySize(getName(), ((Long) o1).intValue(), ((Long) o2).intValue()));

      return stack;
    }
  }

  public FilterBySize(String name, int min, int max) throws WarpScriptException {
    super(name);
    this.min = min;
    this.max = max;
  }

  @Override
  public List<GeoTimeSerie> filter(Map<String, String> labels, List<GeoTimeSerie>... series) throws WarpScriptException {

    List<GeoTimeSerie> retained = new ArrayList<GeoTimeSerie>();

    for (List<GeoTimeSerie> serie: series) {
      for (GeoTimeSerie gts: serie) {
        if (gts.size() >= this.min && gts.size() <= this.max) {
          retained.add(gts);
        }
      }
    }

    return retained;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.min));
    sb.append(" ");
    sb.append(StackUtils.toString(this.max));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
