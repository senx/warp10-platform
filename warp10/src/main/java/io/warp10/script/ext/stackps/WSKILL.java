//
//   Copyright 2020-2023  SenX S.A.S.
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

package io.warp10.script.ext.stackps;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Signal;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.warp.sdk.Capabilities;

public class WSKILL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final Signal signal;

  public WSKILL(String name, Signal signal) {
    super(name);
    this.signal = signal;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (null == Capabilities.get(stack, StackPSWarpScriptExtension.CAPABILITY)) {
      throw new WarpScriptException(getName() + " missing capability '" + StackPSWarpScriptExtension.CAPABILITY + "'.");
    }

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a UUID.");
    }

    stack.push(WarpScriptStackRegistry.signalByUuid(top.toString(), this.signal));

    return stack;
  }
}
