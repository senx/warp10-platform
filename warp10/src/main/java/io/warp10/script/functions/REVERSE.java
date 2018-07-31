//
//   Copyright 2016  Cityzen Data
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

import io.warp10.continuum.gts.UnsafeString;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reverse the order of the elements in a list, either by copying the list or reversing it in place 
 */
public class REVERSE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean stable;
  
  public REVERSE(String name, boolean stable) {
    super(name);
    this.stable = stable;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof List) && !(top instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a list or String.");
    }

    if (!this.stable) {      
      if (top instanceof List) {
        List l = new ArrayList<Object>();
        l.addAll((List) top);
        Collections.reverse(l);
        top = l;        
      } else {
        top = top.toString();
      }
    } else if (top instanceof List) {
      Collections.reverse((List) top); 
    }

    if (top instanceof String) {
      char[] chars = top.toString();
      int i = 0;
      int j = chars.length - 1;
      
      while (i < j) {
        char tmp = chars[i];
        chars[i] = chars[j];
        chars[j] = tmp;
        i++;
        j--;
      }
      
      top = new String(chars);
    }
    
    stack.push(top);
    
    return stack;
  }
}
