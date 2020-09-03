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
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Produces a map which is the result of the application of 'macro' on each entry of 'map'
 * <p>
 * 2: map
 * 1: macro
 * MMAP
 * <p>
 * The macro and the map are popped out of the stack.
 * The macro is called for each entry of 'map' with the index of the current element, by default, the value and the key on the stack.
 *
 */
public class MMAP extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public MMAP(String name) {
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
      throw new WarpScriptException(getName() + " expects a macro on top of the stack.");
    }

    Macro macro = (Macro) top;
    
    top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a MAP below the macro on top of the stack.");
    }

    Map<?, ?> map = (Map) top;

    LinkedHashMap<Object, Object> result = new LinkedHashMap<Object, Object>(map.size());

    int i = 0;
    for (Map.Entry entry: map.entrySet()) {
      stack.push(entry.getKey());
      stack.push(entry.getValue());

      if (pushIndex) {
        stack.push((long) i);
      }

      stack.exec(macro);

      Object val = stack.pop();
      Object key = stack.pop();

      result.put(key, val);

      i++;
    }
    
    stack.push(result);
    
    return stack;
  }
}
