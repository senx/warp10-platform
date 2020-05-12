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

import java.util.function.DoubleUnaryOperator;

/**
 * Apply a double unary operator to 1 value or a list of values, converting all numbers to doubles.
 */
public class DoubleUnaryFunction extends ListRecursiveStackFunction {

  final ElementStackFunction func;

  public DoubleUnaryFunction(String name, DoubleUnaryOperator doubleUnOp) {
    super(name);
    func = new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof Number) {
          return doubleUnOp.applyAsDouble(((Number) element).doubleValue());
        } else {
          return unhandled(element);
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
