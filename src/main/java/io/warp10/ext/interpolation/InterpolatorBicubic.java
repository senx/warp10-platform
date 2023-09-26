//
//   Copyright 2022 - 2023 SenX S.A.S.
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
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.math3.analysis.interpolation.BicubicInterpolatingFunction;

import java.util.List;

/**
 * Function that implements the bicubic spline interpolation
 */
public class InterpolatorBicubic extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static class BICUBE extends NamedWarpScriptFunction implements WarpScriptStackFunction, WarpScriptReducerFunction {

    private final BicubicInterpolatingFunction func;
    private final String generatedFrom;

    private BICUBE(BicubicInterpolatingFunction function, String interpolatorName) {
      super("BICUBE");
      func = function;
      generatedFrom = interpolatorName;
    }

    private double value(double x, double y) {
      if (!func.isValidPoint(x, y)) {
        return Double.NaN;
      } else {
        return func.value(x, y);
      }
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object o = stack.pop();
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " expects a LIST");
      }
      List l = (List) o;

      if (2 != l.size()) {
        throw new WarpScriptException(getName() + " expects a LIST with 2 components " + l.size());
      }

      double x = ((Number) l.get(0)).doubleValue();
      double y = ((Number) l.get(1)).doubleValue();
      stack.push(value(x, y));

      return stack;
    }

    @Override
    public Object apply(Object[] args) throws WarpScriptException {
      long tick = (long) args[0];
      long[] locations = (long[]) args[4];
      long[] elevations = (long[]) args[5];
      Object[] values = (Object[]) args[6];

      if (2 != values.length) {
        throw new WarpScriptException(getName() + " expects 2 components but only got " + values.length);
      }

      double x = ((Number) values[0]).doubleValue();
      double y = ((Number) values[1]).doubleValue();
      double res = value(x, y);

      return new Object[] {tick, locations[0], elevations[0], res};
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();

      try {
        double[] xval = (double[]) FieldUtils.readField(func, "xval", true);
        sb.append(WarpScriptLib.LIST_START);
        sb.append(" ");
        for (int i = 0; i < xval.length; i++) {
          sb.append(xval[i]);
          sb.append(" ");
        }
        sb.append(WarpScriptLib.LIST_END);
        sb.append(" ");

        double[] yval = (double[]) FieldUtils.readField(func, "yval", true);
        sb.append(WarpScriptLib.LIST_START);
        sb.append(" ");
        for (int i = 0; i < yval.length; i++) {
          sb.append(yval[i]);
          sb.append(" ");
        }
        sb.append(WarpScriptLib.LIST_END);
        sb.append(" ");

        // fval
        sb.append(WarpScriptLib.LIST_START);
        sb.append(" ");
        for (int i = 0; i < xval.length; i++) {
          sb.append(WarpScriptLib.LIST_START);
          sb.append(" ");
          for (int j = 0; j < yval.length; j++) {
            sb.append(func.value(xval[i], yval[j]));
            sb.append(" ");
          }
          sb.append(WarpScriptLib.LIST_END);
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

  public InterpolatorBicubic(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    double xval[];
    double yval[];
    double[][] fval;

    Object o = stack.pop();
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST as 3rd argument");
    }
    List o3 = (List) o;

    o = stack.pop();
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
    xval = new double[d1];
    for (int i = 0; i < d1; i++) {
      xval[i] = ((Number) o1.get(i)).doubleValue();
    }

    // fill y
    int d2 = o2.size();
    yval = new double[d2];
    for (int i = 0; i < d2; i++) {
      yval[i] = ((Number) o2.get(i)).doubleValue();
    }

    // fill f
    if (o3.size() != d1) {
      throw new WarpScriptException(getName() + ": incoherent argument sizes");
    }
    fval = new double[d1][d2];

    for (int i = 0; i < d1; i++) {
      if (!(o3.get(i) instanceof List)) {
        throw new WarpScriptException(getName() + " expects the last argument to be a LIST tensor of dimension 2");
      }

      List row = (List) o3.get(i);
      if (row.size() != d2) {
        throw new WarpScriptException(getName() + ": incoherent argument sizes");
      }

      for (int j = 0; j < d2; j++) {
        if (!(row.get(j) instanceof Number)) {
          throw new WarpScriptException(getName() + " expects the last argument to be a numeric LIST tensor of dimension 2");
        }

        fval[i][j] = ((Number) row.get(j)).doubleValue();
      }
    }

    BicubicInterpolatingFunction function = (new BicubicInterpolator()).interpolate(xval, yval, fval);
    BICUBE warpscriptFunction = new BICUBE(function, getName());
    stack.push(warpscriptFunction);

    return stack;
  }
}
