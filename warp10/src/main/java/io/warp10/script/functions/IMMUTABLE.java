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

package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Makes a map/list/set immutable
 */
public class IMMUTABLE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public IMMUTABLE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (top instanceof List) {
      stack.push(Collections.unmodifiableList((List) top));
    } else if (top instanceof Set) {
      stack.push(Collections.unmodifiableSet((Set) top));
    } else if (top instanceof Map) {
      stack.push(Collections.unmodifiableMap((Map) top));
    } else {
      throw new WarpScriptException(getName() + " operates on a " + TYPEOF.TYPE_LIST + ", " + TYPEOF.TYPE_SET + " or " + TYPEOF.TYPE_MAP + ".");
    }

    return stack;
  }

}
