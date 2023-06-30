//
//   Copyright 2021-2022  SenX S.A.S.
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

public class MapperRegExpReplace extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

  private final Pattern pattern;
  private final String replacement;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    public Builder(String name) {
      super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object top = stack.pop();

      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a replacement STRING.");
      }

      String replacement = (String) top;

      top = stack.pop();

      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a regular expression STRING.");
      }

      Pattern pattern;
      try {
        pattern = Pattern.compile((String) top);
      } catch (PatternSyntaxException pse) {
        throw new WarpScriptException(getName() + " expects a valid regular expression.", pse);
      }

      stack.push(new MapperRegExpReplace(getName(), pattern, replacement));

      return stack;
    }
  }

  public MapperRegExpReplace(String name, Pattern pattern, String replacement) {
    super(name);
    this.pattern = pattern;
    this.replacement = replacement;
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (0 == values.length) {
      return new Object[] {0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
    }
    
    if (values.length > 1) {
      throw new WarpScriptException(getName() + " can only be applied to a single value.");
    }

    Matcher matcher = pattern.matcher((String) values[0]);
    return new Object[] {tick, locations[0], elevations[0], matcher.replaceAll(replacement)};
  }
}
