//
//   Copyright 2021  SenX S.A.S.
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

public class MSIG extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String SIGALG = "SHA256WITHECDSA";

  public MSIG(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (top instanceof Macro) {
      Macro macro = (Macro) top;
      stack.push(macro);
      stack.push(getSignature(macro));
    } else {
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a hex encoded signature.");
      }
      top = stack.pop();
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a hex encoded ECC public key.");
      }
      top = stack.pop();
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects an ECC curve name.");
      }
    }

    return stack;
  }

  public static Macro getSignature(Macro macro) {
    int size = macro.size();

    Macro sigmacro = new Macro();

    if (size < 4
        || !(macro.get(size - 1) instanceof MSIG)
        || !(macro.get(size - 2) instanceof String)
        || !(macro.get(size - 3) instanceof String)
        || !(macro.get(size - 4) instanceof String)) {
    } else {
      sigmacro.add(macro.get(size - 4));
      sigmacro.add(macro.get(size - 3));
      sigmacro.add(macro.get(size - 2));
      sigmacro.add(macro.get(size - 1));
    }

    return sigmacro;
  }
}
