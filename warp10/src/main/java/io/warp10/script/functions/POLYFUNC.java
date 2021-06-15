//
//   Copyright 2018-2021  SenX S.A.S.
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
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;

/**
 * Build a function which will compute a polynomial function
 */
public class POLYFUNC extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final PolynomialFunction func;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    public Builder(String name) {
      super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object top = stack.pop();

      if (!(top instanceof List)) {
        throw new WarpScriptException(getName() + " expects a list of polynomial coefficients on top of the stack.");
      }

      List<Object> l = (List<Object>) top;

      double[] coeffs = new double[l.size()];

      int i = 0;

      for (Object o: l) {
        if (!(o instanceof Number)) {
          throw new WarpScriptException(getName() + " expects polynomial coefficients to be numerical.");
        }
        coeffs[i++] = ((Number) o).doubleValue();
      }

      stack.push(new POLYFUNC(getName(), coeffs));

      return stack;
    }
  }

  public POLYFUNC(String name, double[] coeffs) {
    super(name);
    this.func = new PolynomialFunction(coeffs);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " can only be applied to numerical values.");
    }

    double value = this.func.value(((Number) top).doubleValue());

    stack.push(value);

    return stack;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    try {
      SNAPSHOT.addElement(sb, Arrays.asList(this.func.getCoefficients()));
    } catch (WarpScriptException wse) {
      throw new RuntimeException("Error building coefficient snapshot", wse);
    }
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }

}
