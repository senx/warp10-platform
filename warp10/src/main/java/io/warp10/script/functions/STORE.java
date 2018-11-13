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

public class STORE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public STORE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object var = stack.pop();

    if (!(var instanceof String) && !(var instanceof List)) {
      throw new WarpScriptException(getName() + " expects a variable name or a list thereof on top of the stack.");
    }
    
    int count = 1;
    
    // Check that each element of the list is a STRING
    if (var instanceof List) {
      count = ((List) var).size();
      for (Object elt: (List) var) {
        if (null != elt && !(elt instanceof String)) {
          throw new WarpScriptException(getName() + " expects a variable name or a list thereof on top of the stack.");
        }
      }
    }
    
    if (stack.depth() < count) {
      throw new WarpScriptException(getName() + " expects " + count + " elements on the stack, only found " + stack.depth());
    }
    
    if (count > 1) {
      for (int i = count - 1; i >= 0; i--) {
        Object symbol = ((List) var).get(i);
        
        Object o = stack.pop();
        
        if (null != symbol) {
          stack.store(symbol.toString(), o);
        }
      }      
    } else {
      Object o = stack.pop();
        
      stack.store(var.toString(), o);
    }

    return stack;
  }
}
