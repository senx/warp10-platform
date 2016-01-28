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

import java.util.List;

/**
 * Pushes a value into a list. Modifies the list on the stack.
 */
public class SET extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SET(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object key = stack.pop();
    Object value = stack.pop();    
    
    Object maporlist = stack.peek();

    if (!(maporlist instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list.");
    }

    if (!(key instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a key which is an integer when operating on a list.");
    }

    ((List) maporlist).set(((Number) key).intValue(), value);
    
    return stack;
  }
}
