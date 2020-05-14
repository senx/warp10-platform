//
//   Copyright 2020  SenX S.A.S.
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

import io.warp10.script.ListRecursiveStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.math.BigDecimal;
import java.util.function.DoubleUnaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * Apply a double or long unary operator to one value, a list of values or nested lists of values.
 * If the only the long operator is defined, all numbers are converted to long.
 * If the only the double operator is defined, all numbers are converted to double.
 * If the both long and double operator are defined, all numbers are converted to long until a Double or BigDecimal is found,
 * then they are all converted to double.
 */
public class NumericalUnaryFunction extends ListRecursiveStackFunction {

  final ListRecursiveStackFunction.ElementStackFunction func;

  public NumericalUnaryFunction(String name, LongUnaryOperator opL, DoubleUnaryOperator opD) {
    super(name);

    func = new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof Number) {
          if (null != opD && (null == opL || element instanceof Double || element instanceof BigDecimal)) {
            return opD.applyAsDouble(((Number) element).doubleValue());
          } else {
            return opL.applyAsLong(((Number) element).longValue());
          }
        } else {
          return UNHANDLED;
        }
      }
    };
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    return func;
  }

  @Override
  public String getUnhandledErrorMessage() {
    return getName() + " can only operate on numerical values.";
  }
}
