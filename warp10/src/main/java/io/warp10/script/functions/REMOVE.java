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

import java.util.List;
import java.util.Map;

/**
 * Remove an entry from a map or from a list
 */
public class REMOVE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public REMOVE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object key = stack.pop();
    Object maporlist = stack.pop();

    if (maporlist instanceof Map) {
      if (!(key instanceof String)) {
        throw new WarpScriptException(getName() + " expects a string as key.");
      }      
      
      Object o = null;
      
      o = ((Map) maporlist).remove(key);

      stack.push(maporlist);
      stack.push(o);
    } else if (maporlist instanceof List) {
      if (!(key instanceof Long) && !(key instanceof Integer)) {
        throw new WarpScriptException(getName() + " expects a positive integer as key.");
      }      
      int idx = ((Number) key).intValue();
      
      stack.push(maporlist);
      
      Object o = null;
      
      if (idx >= 0 && idx <= ((List) maporlist).size()) {
        o = ((List) maporlist).remove(idx);
      }

      stack.push(o);
    } else {
      throw new WarpScriptException(getName() + " operates on a map or a list.");      
    }
    
    return stack;
  }
}
