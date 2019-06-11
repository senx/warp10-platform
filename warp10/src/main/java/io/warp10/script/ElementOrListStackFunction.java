//
//   Copyright 2019  SenX S.A.S.
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

package io.warp10.script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ElementOrListStackFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  @FunctionalInterface
  public interface ElementStackFunction {
    Object applyOnElement(Object element) throws WarpScriptException;
  }

  public abstract ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException;

  public ElementOrListStackFunction(String name) {
    super(name);
  }

  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    ElementStackFunction function = generateFunction(stack);

    Object o = stack.pop();

    if (o instanceof List) {
      List list = (List) o;
      ArrayList<Object> result = new ArrayList<Object>(list.size());

      for (Object element: list) {
        result.add(function.applyOnElement(element));
      }

      stack.push(result);
    } else {
      stack.push(function.applyOnElement(o));
    }

    return stack;
  }
}

