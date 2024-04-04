//
//   Copyright 2024  SenX S.A.S.
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

package io.warp10.script.filler;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptSingleValueFillerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.TYPEOF;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.interpolation.LoessInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

public class FillerRlowess extends NamedWarpScriptFunction implements WarpScriptSingleValueFillerFunction.Precomputable {

  private long bandwidth;
  private int robustness;
  private double accuracy;

  private FillerRlowess(String name, long bandwidth, int robustness, double accuracy) {
    super(name);
    this.bandwidth = bandwidth;
    this.robustness = robustness;
    this.accuracy = accuracy;
  }

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    public Builder(String name) {
      super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {

      // accuracy
      double a = LoessInterpolator.DEFAULT_ACCURACY;
      Object o = stack.pop();
      if (o instanceof Double) {
        a = (double) o;
      } else {
        stack.pop();
      }

      // robustness iterations
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a number of robustifying iterations (LONG), but instead got a " + TYPEOF.typeof(o));
      }
      int r = (int) o;

      // bandwidth
      o = stack.pop();
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects an bandwidth as a number of data points (LONG), but instead got a " + TYPEOF.typeof(o));
      }

      stack.push(new FillerRlowess(getName(), (long) o, r, a));

      return stack;
    }
  }

  @Override
  public WarpScriptSingleValueFillerFunction compute(GeoTimeSerie gts) throws WarpScriptException {
    double[] xval = GTSHelper.getTicksAsDouble(gts);
    double[] fval = GTSHelper.getValuesAsDouble(gts);

    int size = gts.size();
    final PolynomialSplineFunction function;
    if (size > 2) {
      function = (new LoessInterpolator((double) bandwidth / size + 1e-12, robustness, accuracy)).interpolate(xval, fval);
    } else if (size > 1) {
      function = (new LinearInterpolator()).interpolate(xval, fval);
    } else {
      function = null;
    }

    return new WarpScriptSingleValueFillerFunction() {
      @Override
      public Object evaluate(long tick) throws WarpScriptException {
        if (null == function || !function.isValidPoint(tick)) {
          return null;
        } else {
          return function.value(tick);
        }
      }
    };
  }
}
