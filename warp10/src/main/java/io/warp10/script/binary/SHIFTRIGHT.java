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

package io.warp10.script.binary;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Shift right the long below the top of the stack by the number of bits on top of the stack
 */
public class SHIFTRIGHT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean signed;
  public SHIFTRIGHT(String name, boolean signed) {
    super(name);
    this.signed = signed;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of bits on top of the stack.");
    }
    
    int nbits = ((Number) top).intValue();
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " operates on a LONG.");
    }
    
    long v = ((Number) top).longValue();
    
    if (this.signed) {
      v = v >> nbits;
    } else {
      v = v >>> nbits;
    }
    
    stack.push(v);
    
    return stack;
  }
}
