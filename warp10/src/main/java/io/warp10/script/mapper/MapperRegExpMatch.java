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

package io.warp10.script.mapper;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class MapperRegExpMatch extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

  private final Pattern pattern;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    public Builder(String name) {
      super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object value = stack.pop();

      if (!(value instanceof String)) {
        throw new WarpScriptException(getName() + " expects a regular expression STRING.");
      }

      Pattern pattern;
      try {
        pattern = Pattern.compile((String) value);
      } catch (PatternSyntaxException pse) {
        throw new WarpScriptException(getName() + " expects a valid regular expression.", pse);
      }

      stack.push(new MapperRegExpMatch(getName(), pattern));

      return stack;
    }
  }

  public MapperRegExpMatch(String name, Pattern pattern) {
    super(name);
    this.pattern = pattern;
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    Matcher matcher = null;
    for (int i = 0; i < values.length; i++) {
      if (null == matcher) {
        // Initialize Matcher
        matcher = pattern.matcher((String) values[i]);
      } else {
        // Reuse Matcher
        matcher.reset((String) values[i]);
      }
      if (values[i] instanceof String && matcher.matches()) {
        return new Object[] {tick, locations[i], elevations[i], values[i]};
      }
    }

    return new Object[] {tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
  }
}
