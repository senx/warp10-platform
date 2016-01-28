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
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

import java.util.List;

/**
 * Implements 'If-Then-Else' conditional
 * 
 * 3: IF-macro 
 * 2: THEN-macro
 * 1: ELSE-macro
 * IFTE
 * 
 * Macros are popped out of the stack.
 * If-macro is evaluated, it MUST leave a boolean on top of the stack
 * Boolean is consumed, if true, THEN-macro is evaluated, otherwise ELSE-macro is
 */
public class IFTE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public IFTE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object[] macros = new Object[3];
    macros[0] = stack.pop(); // ELSE-macro
    macros[1] = stack.pop(); // THEN-macro
    macros[2] = stack.pop(); // IF-macro
    
    //
    // Check that what we popped are macros
    //
    
    for (Object macro: macros) {
      if (!WarpScriptLib.isMacro(macro)) {
        throw new WarpScriptException(getName() + " expects three macros on top of the stack.");
      }
    }
    
    //
    // Execute IF-macro
    //
    
    stack.exec((Macro) macros[2]);
    
    //
    // Check that the top of the stack is a boolean
    //
    
    Object top = stack.pop();
    
    if (! (top instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects its 'IF' macro to leave a boolean on top of the stack.");
    }
    
    //
    // If IF-macro left 'true' on top of the stack, execute the THEN-macro, otherwise execute the ELSE-macro
    //
    
    if (Boolean.TRUE.equals(top)) {
      stack.exec((Macro) macros[1]);
    } else {
      stack.exec((Macro) macros[0]);
    }

    return stack;
  }
}
