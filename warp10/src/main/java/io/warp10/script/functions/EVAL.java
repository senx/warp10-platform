//
//   Copyright 2018-2023  SenX S.A.S.
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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

public class EVAL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean xeval;

  public EVAL(String name) {
    super(name);
    this.xeval = false;
  }

  public EVAL(String name, boolean xeval) {
    super(name);
    this.xeval = xeval;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    boolean xevalSet = Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_IN_XEVAL));

    try {
      Object o = stack.pop();

      if (o instanceof String) {
        // We only set the xeval attribute to true for the XEVAL call on a String
        // which is the only supported param, otherwise calling XEVAL from within a
        // secure macro on a Macro which calls EVAL on a STRING would lead to a non
        // secure macro which would be rather hard to track as a source of potential anomaly
        if (xeval) {
          stack.setAttribute(WarpScriptStack.ATTRIBUTE_IN_XEVAL, true);
        }
        // Execute single statement which may span multiple lines
        stack.execMulti((String) o);
      } else if (o instanceof Macro) {
        if (xeval) {
          throw new WarpScriptException(getName() + " can only be applied to a STRING.");
        }
        stack.exec((Macro) o);
      } else if (o instanceof WarpScriptStackFunction) {
        if (xeval) {
          throw new WarpScriptException(getName() + " can only be applied to a STRING.");
        }
        ((WarpScriptStackFunction) o).apply(stack);
      } else {
        throw new WarpScriptException(getName() + " expects a Macro, a function or a String.");
      }
    } finally {
      // Clear the XEVAL attribute if it was not set prior to this call.
      if (xeval && !xevalSet) {
        stack.setAttribute(WarpScriptStack.ATTRIBUTE_IN_XEVAL, null);
      }
    }

    return stack;
  }
}
