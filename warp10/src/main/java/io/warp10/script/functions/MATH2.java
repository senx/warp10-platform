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
import java.math.BigDecimal;

public class MATH2 extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  //TODO(tce): LambdaMetafactory could be used to make calls a bit faster, but it is only available on Java 8. See https://stackoverflow.com/q/19557829

  private Method methodLong;
  private Method methodDouble;

  public MATH2(String name, String methodName) throws WarpScriptException {
    super(name);

    WarpScriptException warpExceptionLong = null;

    try {
      this.methodLong = Math.class.getMethod(methodName, long.class, long.class);
    } catch (Exception e) {
      warpExceptionLong = new WarpScriptException(e);
    }

    WarpScriptException warpExceptionDouble = null;

    try {
      this.methodDouble = Math.class.getMethod(methodName, double.class, double.class);
    } catch (Exception e) {
      warpExceptionDouble = new WarpScriptException(e);
    }

    // No method suitable, throw
    if (null != warpExceptionLong && null != warpExceptionDouble) {
      throw new WarpScriptException("Could not find a method "+methodName+" in Math accepting (Long, Long) or (Double, Double).");
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
      // If any argument is double/float/bigdecimal
      if (op1 instanceof Double || op1 instanceof Float || op1 instanceof BigDecimal || op2 instanceof Double || op2 instanceof Float || op2 instanceof BigDecimal) {
        // Check method accepting doubles first
        if (null != this.methodDouble) {
          stack.push(this.methodDouble.invoke(null, ((Number) op1).doubleValue(), ((Number) op2).doubleValue()));
        } else {
          stack.push(this.methodLong.invoke(null, ((Number) op1).longValue(), ((Number) op2).longValue()));
        }
      } else { // If all arguments are ints/longs
        // Check method accepting longs first
        if (null != this.methodLong) {
          stack.push(this.methodLong.invoke(null, ((Number) op1).longValue(), ((Number) op2).longValue()));
        } else {
          stack.push(this.methodDouble.invoke(null, ((Number) op1).doubleValue(), ((Number) op2).doubleValue()));
        }
      }
    } catch (Exception e) {
      throw new WarpScriptException(e);
    }

    return stack;
  }
}
