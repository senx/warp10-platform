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

import java.util.LinkedHashMap;
import java.util.Map;

public class MFILTER extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public MFILTER(String name) {
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

    Macro macro = (Macro) top;
    
    top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a MAP.");
    }

    Map<?, ?> map = (Map) top;

    LinkedHashMap<Object, Object> result = new LinkedHashMap<Object, Object>();

    int i = 0;
    for (Map.Entry entry: map.entrySet()) {
      Object key = entry.getKey();
      Object value = entry.getValue();

      stack.push(key);
      stack.push(value);

      if (pushIndex) {
        stack.push((long) i);
      }

      stack.exec(macro);

      Object o = stack.pop();

      if(!(o instanceof Boolean)) {
        throw new WarpScriptException(getName() + " expects the filter macro to return a BOOLEAN for each element.");
      }

      if (Boolean.TRUE.equals(o)) {
        result.put(key, value);
      }

      i++;
    }
    
    stack.push(result);
    
    return stack;
  }
}
