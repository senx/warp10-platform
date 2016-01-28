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

public class MATH2 extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private Method method;

  public MATH2(String name, String methodName) throws WarpScriptException {
    super(name);

    try {
      this.method = Math.class.getMethod(methodName, double.class, double.class);
    } catch (Exception e) {
      throw new WarpScriptException(e);
    }
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();

    if (!(op1 instanceof Number) || !(op2 instanceof Number)) {
      throw new WarpScriptException(getName() + " operates on two numeric arguments.");
    }

    try {
      stack.push(this.method.invoke(null, ((Number) op1).doubleValue(), ((Number) op2).doubleValue()));
    } catch (Exception e) {
      throw new WarpScriptException(e);
    }

    return stack;
  }
}
