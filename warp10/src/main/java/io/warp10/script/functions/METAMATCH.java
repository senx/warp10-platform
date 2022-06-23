//
//   Copyright 2022  SenX S.A.S.
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

package io.warp10.script.functions;

import java.util.ArrayList;
import java.util.List;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.MetadataSelectorMatcher;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class METAMATCH extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public METAMATCH(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a STRING selector.");
    }

    MetadataSelectorMatcher matcher = new MetadataSelectorMatcher((String) top);

    top = stack.pop();

    if (top instanceof GeoTimeSerie) {
      stack.push(matcher.matches(((GeoTimeSerie) top).getMetadata()));
    } else if (top instanceof GTSEncoder) {
      stack.push(matcher.matches(((GTSEncoder) top).getRawMetadata()));
    } else if (top instanceof List) {
      List<Object> results = new ArrayList<Object>(((List) top).size());
      for (Object o: (List) top) {
        if (o instanceof GeoTimeSerie) {
          results.add(matcher.matches(((GeoTimeSerie) o).getMetadata()));
        } else if (o instanceof GTSEncoder) {
          results.add(matcher.matches(((GTSEncoder) o).getRawMetadata()));
        } else {
          throw new WarpScriptException(getName() + " can only be applied to a LIST containing GTS or ENCODER elements.");
        }
      }
      stack.push(results);
    } else {
      throw new WarpScriptException(getName() + " operates on a GTS or ENCODER or a list thereof.");
    }

    return stack;
  }
}
