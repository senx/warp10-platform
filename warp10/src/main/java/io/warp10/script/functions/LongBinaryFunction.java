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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.LongBinaryOperator;

/**
 * Apply a long binary operator to 2 values or a list of values, converting all values to long.
 */
public class LongBinaryFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  final LongBinaryOperator opL;

  public LongBinaryFunction(String name, LongBinaryOperator longBinOp) {
    super(name);
    opL = longBinOp;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op0 = stack.pop();

    if (op0 instanceof List) {
      Number result = null;

      for (Object element: (List) op0) {
        if (!(element instanceof Number)) {
          throw new WarpScriptException(getName() + " can only operate on numerical values.");
        }

        if (null == result) {
          if (element instanceof Double || element instanceof BigDecimal) {
            result = ((Number) element).doubleValue();
          } else {
            result = ((Number) element).longValue();
          }
        } else {
          result = opL.applyAsLong(((Number) element).longValue(), result.longValue());
        }
      }

      stack.push(result);
    } else {

      if (!(op0 instanceof Number)) {
        throw new WarpScriptException(getName() + " can only operate on 2 numerical values or a list on numerical values.");
      }

      Object op1 = stack.pop();

      if (!(op1 instanceof Number)) {
        throw new WarpScriptException(getName() + " can only operate on 2 numerical values or a list on numerical values.");
      }

      stack.push(opL.applyAsLong(((Number) op1).longValue(), ((Number) op0).longValue()));
    }

    return stack;
  }
}
