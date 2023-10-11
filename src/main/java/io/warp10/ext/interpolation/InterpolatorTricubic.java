//
//   Copyright 2022 - 2023  SenX S.A.S.
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

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.math3.analysis.interpolation.TricubicInterpolatingFunction;
import org.apache.commons.math3.exception.OutOfRangeException;

import java.util.List;

/**
 * Function that implements the tricubic spline interpolation, as proposed in
 * Tricubic interpolation in three dimensions, F. Lekien and J. Marsden, Int. J. Numer. Meth. Eng 2005; 63:455-471
 */
public class InterpolatorTricubic extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static class TRICUBE extends NamedWarpScriptFunction implements WarpScriptStackFunction, WarpScriptReducerFunction {

    private final TricubicInterpolatingFunction func;
    private final String generatedFrom;

    private TRICUBE(TricubicInterpolatingFunction function, String interpolatorName) {
      super("TRICUBE");
      func = function;
      generatedFrom = interpolatorName;
    }

    private double value(double x, double y, double z) {
      try {
        if (!func.isValidPoint(x, y, z)) {
          return Double.NaN;
        } else {
          return func.value(x, y, z);
        }
      } catch (OutOfRangeException e) {
        return Double.NaN; // It can happen, even with the isValidPoint check.
      }
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object o = stack.pop();
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " expects a LIST");
      }
      List l = (List) o;

      if (3 != l.size()) {
        throw new WarpScriptException(getName() + " expects a LIST with 3 components " + l.size());
      }

      double x = ((Number) l.get(0)).doubleValue();
      double y = ((Number) l.get(1)).doubleValue();
      double z = ((Number) l.get(2)).doubleValue();
      stack.push(value(x, y, z));

      return stack;
    }

    @Override
    public Object apply(Object[] args) throws WarpScriptException {
      long tick = (long) args[0];
      long[] locations = (long[]) args[4];
      long[] elevations = (long[]) args[5];
      Object[] values = (Object[]) args[6];

      if (3 != values.length) {
        throw new WarpScriptException(getName() + " expects 3 components but got " + values.length);
      }

      if (null == values[0] || null == values[1] || null == values[2]) {
        return new Object[] {tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
      }

      double x = ((Number) values[0]).doubleValue();
      double y = ((Number) values[1]).doubleValue();
      double z = ((Number) values[2]).doubleValue();
      double res = value(x, y, z);

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

        double[] zval = (double[]) FieldUtils.readField(func, "zval", true);
        sb.append(WarpScriptLib.LIST_START);
        sb.append(" ");
        for (int i = 0; i < zval.length; i++) {
          sb.append(zval[i]);
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
            sb.append(WarpScriptLib.LIST_START);
            sb.append(" ");
            for (int k = 0; k < zval.length; k++) {
              sb.append(func.value(xval[i], yval[j], zval[k]));
              sb.append(" ");
            }
            sb.append(WarpScriptLib.LIST_END);
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

  public InterpolatorTricubic(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    double xval[];
    double yval[];
    double zval[];
    double[][][] fval;

    Object o = stack.pop();
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST as 4th argument");
    }
    List o4 = (List) o;

    o = stack.pop();
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

    // fill z
    int d3 = o3.size();
    zval = new double[d3];
    for (int i = 0; i < d3; i++) {
      zval[i] = ((Number) o3.get(i)).doubleValue();
    }

    // fill f
    if (o4.size() != d1) {
      throw new WarpScriptException(getName() + ": incoherent argument sizes");
    }
    fval = new double[d1][d2][d3];

    for (int i = 0; i < d1; i++) {
      if (!(o4.get(i) instanceof List)) {
        throw new WarpScriptException(getName() + " expects the last argument to be a LIST tensor of dimension 3");
      }

      List row = (List) o4.get(i);
      if (row.size() != d2) {
        throw new WarpScriptException(getName() + ": incoherent argument sizes");
      }

      for (int j = 0; j < d2; j++) {
        if (!(row.get(j) instanceof List)) {
          throw new WarpScriptException(getName() + " expects the last argument to be a LIST tensor of dimension 3");
        }

        List col = (List) row.get(j);
        if (col.size() != d3) {
          throw new WarpScriptException(getName() + ": incoherent argument sizes");
        }

        for (int k = 0; k < d3; k++) {
          if (!((col.get(k)) instanceof Number)) {
            throw new WarpScriptException(getName() + " expects the last argument to be a numeric LIST tensor of dimension 3");
          }

          fval[i][j][k] = ((Number) col.get(k)).doubleValue();
        }
      }
    }

    TricubicInterpolatingFunction function = (new TricubicInterpolator()).interpolate(xval, yval, zval, fval);
    TRICUBE warpscriptFunction = new TRICUBE(function, getName());
    stack.push(warpscriptFunction);

    return stack;
  }
}
