//
//   Copyright 2017  Cityzen Data
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

import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStopException;

/**
 * Consumes the two lists on the stack or leave them there and stop the script
 * if the stack is currently in signature mode
 */
public class SIG extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SIG(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of pairs of lists on top of the stack.");
    }

    List<Object> sig = (List<Object>) top;
    
    for (Object elt: sig) {
      if (!(elt instanceof List) || (2 != ((List) elt).size())) {
        throw new WarpScriptException(getName() + " expects a list of pairs of lists on top of the stack.");        
      }
      
      for (Object o: ((List) elt)) {
        if (!(o instanceof List)) {
          throw new WarpScriptException(getName() + " expects a list of pairs of lists on top of the stack.");     
        }
      }
    }

    if (Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_SIGMODE))) {
      // Push the signatures back on the stack
      stack.push(sig);
      // Stop the script
      throw new WarpScriptStopException("");
    }
    
    return stack;
  }
}
