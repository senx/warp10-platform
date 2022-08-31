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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class MSEC extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean recursive;

  public MSEC(String name, boolean recursive) {
    super(name);
    this.recursive = recursive;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a Macro.");
    }

    Macro m = (Macro) top;

    if (recursive) {
      m.setSecureRecursive(true);
    } else {
      m.setSecure(true);
    }

    stack.push(m);

    return stack;
  }
}
