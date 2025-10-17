//
//   Copyright 2025  SenX S.A.S.
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

public class ATTRDELTA extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ATTRDELTA(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (top instanceof Boolean) {
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_ATTRIBUTES_DELTA, Boolean.TRUE.equals(top));
    } else if (null == top) {
      stack.push(Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_ATTRIBUTES_DELTA)));
    } else {
      throw new WarpScriptException(getName() + " invalid parameter, expected a BOOLEAN or NULL.");
    }

    return stack;
  }

}
