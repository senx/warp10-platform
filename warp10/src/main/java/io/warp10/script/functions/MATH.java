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

import java.lang.reflect.Method;

public class MATH extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  //TODO(tce): LambdaMetafactory could be used to make calls a bit faster, but it is only available on Java 8. See https://stackoverflow.com/q/19557829
  
  private Method method;
  
  public MATH(String name, String methodName) throws WarpScriptException {
    super(name);
    
    try {
      this.method = Math.class.getMethod(methodName, double.class);
    } catch (Exception e) {
      throw new WarpScriptException(e);
    }
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " operates on a numeric argument.");
    }
    
    try {
      stack.push(this.method.invoke(null, ((Number) o).doubleValue()));
    } catch (Exception e) {
      throw new WarpScriptException(e);
    }
    
    return stack;
  }
}
