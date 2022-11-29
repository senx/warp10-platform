//
//   Copyright 2019-2022  SenX S.A.S.
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

import io.warp10.WarpManager;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class MANAGERONOFF extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean on;
  private final String attr;

  public MANAGERONOFF(String name, String attr, boolean on) {
    super(name);
    this.on = on;
    this.attr = attr;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_MANAGER)) {
      throw new WarpScriptException(getName() + " missing manager capability.");
    }

    String msg = null;

    if (!on) {
      Object top = stack.pop();
      msg = String.valueOf(top);
    }

    WarpManager.setAttribute(attr, msg);

    return stack;
  }
}
