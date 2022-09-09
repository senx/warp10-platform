//
//   Copyright 2022  SenX S.A.S.
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

package io.warp10.ext.interpolation;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.apache.commons.math3.analysis.interpolation.AkimaSplineInterpolator;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Function that implements the bicubic spline interpolation
 */
public class LINEARFIT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static class LINEAR extends NamedWarpScriptFunction implements WarpScriptStackFunction, WarpScriptMapperFunction {

    private final PolynomialSplineFunction func;
    private final String generatedFrom;
    private ArrayList xval;
    private ArrayList fval;

    private LINEAR(PolynomialSplineFunction function, String interpolatorName, ArrayList xval, ArrayList fval) {
      super(interpolatorName);
      func = function;
      this.xval = xval;
      this.fval = fval;
      generatedFrom = interpolatorName;
    }

    private double value(double x) {
      if (!func.isValidPoint(x)) {
        return Double.NaN;
      } else {
        return func.value(x);
      }
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object o = stack.pop();
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " expects a DOUBLE or a LONG");
      }

      double x = ((Number) o).doubleValue();
      stack.push(value(x));

      return stack;
    }

    @Override
    public Object apply(Object[] args) throws WarpScriptException {
      long tick = (long) args[0];
      long[] locations = (long[]) args[4];
      long[] elevations = (long[]) args[5];
      Object[] values = (Object[]) args[6];

      if (1 != values.length) {
        throw new WarpScriptException(getName() + " expects 1 element but got " + values.length);
      }

      double x = ((Number) values[0]).doubleValue();
      double res = value(x);

      return new Object[] {tick, locations[0], elevations[0], res};
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();


      try {
        sb.append(WarpScriptLib.LIST_START);
        sb.append(" ");
        for (int i = 0; i < xval.size(); i++) {
          sb.append(xval.get(i));
          sb.append(" ");
        }
        sb.append(WarpScriptLib.LIST_END);
        sb.append(" ");

        sb.append(WarpScriptLib.LIST_START);
        sb.append(" ");
        for (int i = 0; i < fval.size(); i++) {
          sb.append(fval.get(i));
          sb.append(" ");
        }
        sb.append(WarpScriptLib.LIST_END);
        sb.append(" ");

      } catch (Exception e) {
        throw new RuntimeException("Error building argument snapshot", e);
      }

      sb.append(generatedFrom);

      return sb.toString();
    }
  }

  public LINEARFIT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    double xval[];
    double fval[];

    Object o = stack.pop();

    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST as 2nd argument");
    }
    List o2 = (List) o;

    o = stack.pop();
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST as 1st argument");
    }
    List o1 = (List) o;

    // fill x
    int d1 = o1.size();
    if (getName().equals("AKIMASPLINEFIT") && d1 < 5) {
      throw new WarpScriptException(getName() + " expects at least 5 interpolation points");
    }
    xval = new double[d1];
    for (int i = 0; i < d1; i++) {
      xval[i] = ((Number) o1.get(i)).doubleValue();
    }

    // fill f
    if (o2.size() != d1) {
      throw new WarpScriptException(getName() + ": incoherent argument sizes");
    }
    fval = new double[d1];

    for (int i = 0; i < d1; i++) {
      if (!(o2.get(i) instanceof Number)) {
        throw new WarpScriptException(getName() + " expects the last argument to be a numeric LIST of numbers");
      }
      fval[i] = ((Number) o2.get(i)).doubleValue();
    }

    PolynomialSplineFunction function = null;
    if (getName().equals("LINEARFIT")) {
      function = (new LinearInterpolator()).interpolate(xval, fval);
    } else if (getName().equals("SPLINELINEARFIT")) {
      function = (new SplineInterpolator()).interpolate(xval, fval);
    } else if (getName().equals("AKIMASPLINEFIT")) {
      function = (new AkimaSplineInterpolator()).interpolate(xval, fval);
    }

    // clone the inputs for snapshot.
    LINEAR warpscriptFunction = new LINEAR(function, getName(), new ArrayList<Number>(o1), new ArrayList<Number>(o2));
    stack.push(warpscriptFunction);

    return stack;
  }
}
