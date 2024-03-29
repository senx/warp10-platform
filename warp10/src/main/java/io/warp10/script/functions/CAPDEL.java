//
//   Copyright 2020-2022  SenX S.A.S.
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

import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

public class CAPDEL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public CAPDEL(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    Capabilities capabilities = Capabilities.get(stack);

    if (top instanceof String) {
      if (null != capabilities) {
        capabilities.remove((String) top);
      }
    } else if (top instanceof List) {
      if (null != capabilities) {
        for (Object elt: (List) top) {
          if (elt instanceof String) {
            capabilities.remove((String) elt);
          }
        }
      }
    } else if (null == top) {
      if (null != capabilities) {
        capabilities.clear();
      }
    } else {
      throw new WarpScriptException(getName() + " expects a capability name (STRING), a LIST thereof or NULL.");
    }

    return stack;
  }
}
