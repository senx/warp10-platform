//
//   Copyright 2020  SenX S.A.S.
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

public class RUNR extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final int regno;

  public RUNR(String name, int regno) {
    super(name);
    this.regno = regno;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object reg = stack.load(this.regno);

    if (reg instanceof WarpScriptStack.Macro) {
      stack.exec((WarpScriptStack.Macro) reg);
    } else {
      throw new WarpScriptException(getName() + " expects register number " + this.regno + " to contain a macro.");
    }

    return stack;
  }

  public int getRegister() {
    return this.regno;
  }
}
