//
//   Copyright 2018  SenX S.A.S.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pushes a value into a map or list. Modifies the map or list on the stack.
 */
public class PUT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PUT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object key = stack.pop();
    Object value = stack.pop();    
    
    Object maporlist = stack.peek();

    if (maporlist instanceof Map) {
      ((Map) maporlist).put(key, value);      
    } else if (maporlist instanceof List) {
      List list = (List) maporlist;

      if (key instanceof Number) {
        list.set(((Number) key).intValue(), value);

      } else if (!(key instanceof List)) {
        throw new WarpScriptException(getName() + " expects the key to be an integer or a list of integers when operating on a List.");

      } else {
        for (Object o: (List) key) {
          if (!(o instanceof Number)) {
            throw new WarpScriptException(getName() + " expects the key to be an integer or a list of integers when operating on a List.");
          }
        }

        List<Number> copyIndices = new ArrayList<>((List<Number>) key);
        int lastIdx = copyIndices.remove(copyIndices.size() - 1).intValue();
        Object container;

        try {
          container = GET.nestedGet(list, copyIndices);
        } catch (WarpScriptException wse) {
          throw new WarpScriptException("Tried to set an element at a nested path that does not exist in the input list.");
        }

        if (container instanceof List) {
          ((List) container).set(lastIdx, value);

        } else {
          throw new WarpScriptException("Tried to set an element at a nested path that does not exist in the input list.");
        }

      }
    } else {
      throw new WarpScriptException(getName() + " operates on a map or list.");
    }

    return stack;
  }
}
