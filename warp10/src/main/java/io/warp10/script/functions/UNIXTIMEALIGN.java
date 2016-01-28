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

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Replaces the time on top of the stack with a time on a Constants.DEFAULT_MODULUS boundary
 * after the time on top of the stack (except if the time on top of the stack if on the right boundary)
 */
public class UNIXTIMEALIGN extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public UNIXTIMEALIGN(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a Unix timestamp on top of the stack.");
    }
    
    long ts = (long) top;
    
    if (0 == (ts % Constants.DEFAULT_MODULUS)) {
      stack.push(ts);
    } else {
      stack.push(ts - (ts % Constants.DEFAULT_MODULUS) + Constants.DEFAULT_MODULUS);
    }
    
    return stack;
  }
}
