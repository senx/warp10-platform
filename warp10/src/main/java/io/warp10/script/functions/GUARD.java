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

import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

public class GUARD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public GUARD(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    List<Object> symbols = null;

    if (top instanceof List) {
      symbols = (List<Object>) top;
      top = stack.pop();
    }

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a MACRO.");
    }

    Macro macro = (Macro) top;

    try {
      stack.exec(macro);
    } catch (Throwable t) {
      //
      // If any exception was raised during the execution of the macro,
      // clear the stack and the specified symbols so no information leak
      // occurs
      //
      stack.clear();
      if (null != symbols) {
        for (Object symbol: symbols) {
          if (symbol instanceof String) {
            stack.forget((String) symbol);
          } else if (symbol instanceof Long) {
            stack.store(((Long) symbol).intValue(), null);
          }
        }
      }
      throw t;
    }

    return stack;
  }
}
