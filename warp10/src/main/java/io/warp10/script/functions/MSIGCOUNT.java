//
//   Copyright 2021-2024  SenX S.A.S.
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
import io.warp10.script.WrappedStatementUtils;

public class MSIGCOUNT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public MSIGCOUNT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a Macro.");
    }

    Macro macro = (Macro) top;

    int size = macro.size();

    long sigcount = 0;

    while (size >= 4) {
     if (WrappedStatementUtils.unwrapAll(macro.get(size - 1)) instanceof MSIG
        && WrappedStatementUtils.unwrapAll(macro.get(size - 2)) instanceof String
        && WrappedStatementUtils.unwrapAll(macro.get(size - 3)) instanceof String
        && WrappedStatementUtils.unwrapAll(macro.get(size - 4)) instanceof String) {
       sigcount++;
       size -= 4;
     } else {
       break;
     }
    }

    stack.push(sigcount);

    return stack;
  }
}
