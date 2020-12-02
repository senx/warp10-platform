//
//   Copyright 2018-2020  SenX S.A.S.
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

/**
 * Checks the two operands on top of the stack for equality, accepting a difference under a configurable lambda value
 *
 * ALMOSTEQ expects the following parameters on the stack:
 * 3: lambda The tolerance of the comparison
 * 2: op2
 * 1: op1
 * lambda, op1 and op2 need to be instances of Number.
 */
public class ALMOSTEQ extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ALMOSTEQ(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object lambdaParam = stack.pop();
    Object op2Param = stack.pop();
    Object op1Param = stack.pop();

    // All operands need to be numeric.
    if  (!(lambdaParam instanceof Number) || !(op2Param instanceof Number)  || !(op1Param instanceof Number)) {
      throw new WarpScriptException(getName() + " only accepts numeric parameters.");
    }

    double lambda = Math.abs(((Number) lambdaParam).doubleValue());
    double op1 = ((Number) op1Param).doubleValue();
    double op2 = ((Number) op2Param).doubleValue();

    if (Double.isNaN(op1) || Double.isNaN(op2)) {
      stack.push(Double.isNaN(op1) && Double.isNaN(op2));
    } else {
      stack.push(lambda >= Math.abs(op1 - op2));
    }

    return stack;
  }
}
