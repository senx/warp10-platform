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
    
    Object list = stack.pop();
    
    if (!(list instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list.");
    }

    if (!this.stable) {      
      List l = new ArrayList<Object>();
      l.addAll((List) list);
      Collections.reverse(l);
      list = l;
    } else {
      Collections.reverse((List) list); 
    }
    
    stack.push(list);
    
    return stack;
  }
}
