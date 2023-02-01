//
//   Copyright 2022 SenX S.A.S.
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
import java.util.Set;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class CAPEXPORT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public CAPEXPORT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    Set<String> exported = GUARD.getExportedCapabilities(stack);

    if (null == exported) {
      throw new WarpScriptException(getName() + " can only be called from a GUARDed macro.");
    }

    // Calling CAPEXPORT on NULL will have the side effect of propagating the exported capabilities exported by any enclosed GUARD call
    if (top instanceof String || null == top) {
      exported.add((String) top);
    } else if (top instanceof List) {
      for (Object cap: (List) top) {
        if (cap instanceof String || null == cap) {
          exported.add((String) cap);
        } else {
          throw new WarpScriptException(getName() + " expects NULL, a capability name (STRING) or a LIST thereof.");
        }
      }
    } else {
      throw new WarpScriptException(getName() + " expects NULL, a capability name (STRING) or a LIST thereof.");
    }

    return stack;
  }
}
