//
//   Copyright 2018-2022  SenX S.A.S.
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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Configure the maximum number of ops for the stack
 */
public class MAXOPS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public MAXOPS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LIMITS) && null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_MAXOPS)) {
      throw new WarpScriptException(getName() + " missing capability '" + WarpScriptStack.CAPABILITY_MAXOPS + "' or '" + WarpScriptStack.CAPABILITY_LIMITS + "'.");
    }

    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a numeric (long) limit.");
    }

    long limit = ((Number) top).longValue();

    Long max = Capabilities.getLong(stack, WarpScriptStack.CAPABILITY_MAXOPS, (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS_HARD));

    if (limit > max) {
      throw new WarpScriptException(getName() + " cannot extend limit past " + max);
    }

    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, limit);

    return stack;
  }
}
