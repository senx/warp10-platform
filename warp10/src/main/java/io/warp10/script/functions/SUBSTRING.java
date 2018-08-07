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

import org.python.bouncycastle.util.Arrays;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class SUBSTRING extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SUBSTRING(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a numeric length or 0-based start index on top of the stack.");
    }
    
    int n = ((Number) top).intValue();
    
    top = stack.pop();

    if (top instanceof String) {
      stack.push(top.toString().substring(n));
      return stack;
    } else if (top instanceof byte[]) {
      stack.push(Arrays.copyOfRange((byte[]) top, n, ((byte[]) top).length));
      return stack;
    }
        
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a numeric (0 based) start index below the length.");
    }
    
    int idx = ((Number) top).intValue();
    
    top = stack.pop();
    
    if (top instanceof String) {
      stack.push(top.toString().substring(idx, Math.min(n + idx, top.toString().length())));      
    } else if (top instanceof byte[]) {
      stack.push(Arrays.copyOfRange((byte[]) top, idx, Math.min(n + idx, ((byte[]) top).length)));
    } else {
      throw new WarpScriptException(getName() + " can only operate on strings or byte arrays.");
    }
        
    return stack;
  }
}
