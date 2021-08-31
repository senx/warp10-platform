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

import java.util.ArrayList;
import java.util.List;

public class LFILTER extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public LFILTER(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    boolean pushIndex = true;
    if (top instanceof Boolean) {
      pushIndex = (Boolean) top;
      top = stack.pop();
    }

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a MACRO.");
    }

    Object list = stack.pop();

    if (!(list instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST.");
    }

    int n = ((List) list).size();
    ArrayList<Object> result = new ArrayList<Object>();

    for (int i = 0; i < n; i++) {
      Object element = ((List) list).get(i);
      stack.push(element);
      if (pushIndex) {
        stack.push((long) i);
      }
      stack.exec((Macro) top);

      Object o = stack.pop();

      if (!(o instanceof Boolean)) {
        throw new WarpScriptException(getName() + " expects the filter macro to return a BOOLEAN for each element.");
      }

      if (Boolean.TRUE.equals(o)) {
        result.add(element);
      }
    }

    stack.push(result);

    return stack;
  }
}
