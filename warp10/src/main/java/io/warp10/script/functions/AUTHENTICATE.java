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

import io.warp10.continuum.Tokens;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Check and record the provided ReadToken as the current 'owner' of the stack
 */
public class AUTHENTICATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public AUTHENTICATE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    if (null != stack.getAttribute(WarpScriptStack.ATTRIBUTE_TOKEN)) {
      throw new WarpScriptException("Stack is already authenticated.");
    }
    
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a string on top of the stack.");
    }
    
    ReadToken rtoken = Tokens.extractReadToken(o.toString());
    
    //
    // TODO(hbs): check that the provided token can indeed be used for AUTHENTICATION
    // or simply to set specific values of various thresholds
    //
    
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_TOKEN, o.toString());
    
    return stack;
  }
}
