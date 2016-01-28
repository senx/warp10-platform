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

/**
 * Evaluate a secure script
 */
public class EVALSECURE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public EVALSECURE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.peek();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " can only evaluate a secure script stored as a string.");
    }
    
    //
    // Clear debug depth so you can't peek into a Secure Script
    //
    
    int debugDepth = (int) stack.getAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH, 0);
    
    new UNSECURE("", false).apply(stack);
    
    o = stack.pop();
    
    stack.execMulti(o.toString());      

    //
    // Set debug depth back to its original value
    //
    
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH, debugDepth);
    
    return stack;
  }
}
