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

package io.warp10.script.binary;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

/**
 * Checks the two operands on top of the stack for inequality
 */
public class NE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public NE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();

    if (op2 instanceof Double && op1 instanceof Double) {
      // Special case if the 2 parameters are NaN value : we want 'NaN NaN !=' to be true
      // NaN is not convertible to BigDecimal, so we cannot use the compare method
      if (Double.isNaN((Double) op1) || Double.isNaN((Double) op2)) {
        stack.push(!(Double.isNaN((Double) op1) && Double.isNaN((Double) op2)));
      } else {
        stack.push(0 != EQ.compare((Number) op1, (Number) op2));
      }
    } else if (op1 instanceof Double && Double.isNaN((Double) op1)) { // Do we have only one NaN ?
      stack.push(true);
    } else if (op2 instanceof Double && Double.isNaN((Double) op2)) { // Do we have only one NaN ?
      stack.push(true);
    } else if (op2 instanceof Number && op1 instanceof Number) {
      stack.push(0 != EQ.compare((Number) op1, (Number) op2));
    } else if (op1 instanceof Boolean || op1 instanceof String
        || op1 instanceof RealVector || op1 instanceof RealMatrix) {
      stack.push(!op1.equals(op2));
    } else {
      throw new WarpScriptException(getName()
          + " can only operate on homogeneous numeric, string, boolean, vector or matrix types.");
    }

    return stack;
  }
}
