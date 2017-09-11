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

import java.util.HashMap;
import java.util.Map;

import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.sensision.Sensision;

/**
 * Evaluate a function based on its name
 */
public class EVALFUNC extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public EVALFUNC(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a function name on top of the stack.");
    }

    String stmt = o.toString();
    
    Object func = null;
    
    func = null != func ? func : stack.getDefined().get(stmt);
    
    if (null != func && Boolean.FALSE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_ALLOW_REDEFINED))) {
      throw new WarpScriptException("Disallowed redefined function '" + stmt + "'.");
    }
    
    func = null != func ? func : WarpScriptLib.getFunction(stmt);

    if (null == func) {
      throw new WarpScriptException("Unknown function '" + stmt + "'");
    }

    if (func instanceof WarpScriptStackFunction) {
      //
      // Function is an EinsteinStackFunction, call it on this stack
      //
      
      WarpScriptStackFunction esf = (WarpScriptStackFunction) func;

      esf.apply(stack);
    } else {
      //
      // Push any other type of function onto the stack
      //
      stack.push(func);
    }          

    return stack;
  }
}
