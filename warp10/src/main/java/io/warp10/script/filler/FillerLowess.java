//
//   Copyright 2024-2025  SenX S.A.S.
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
import io.warp10.script.functions.SNAPSHOT;
import io.warp10.script.functions.TYPEOF;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.interpolation.LoessInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

public class FillerLowess extends NamedWarpScriptFunction implements WarpScriptSingleValueFillerFunction.Precomputable, SNAPSHOT.Snapshotable {

  private long bandwidth;
  private double accuracy;

  private FillerLowess(String name, long bandwidth, double accuracy) throws WarpScriptException {
    super(name);
    this.bandwidth = bandwidth;
    this.accuracy = accuracy;

    if (bandwidth <= 1) {
      throw new WarpScriptException(getName() + " expects a bandwidth > 1, instead got " + String.valueOf(bandwidth));
    }

    if (accuracy <= 0) {
      throw new WarpScriptException(getName() + " expects a positive accuracy, instead got " + String.valueOf(accuracy));
    }
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
        o = stack.pop();
      }

      // bandwidth
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a bandwidth as a number of data points (LONG), but instead got a " + TYPEOF.typeof(o));
      }

      stack.push(new FillerLowess(getName(), (long) o, a));

      return stack;
    }
  }

  @Override
  public WarpScriptSingleValueFillerFunction compute(GeoTimeSerie gts) throws WarpScriptException {
    final PolynomialSplineFunction function;
    if (gts.size() == 0) {
      function = null;
    } else {
      if (GeoTimeSerie.TYPE.DOUBLE != gts.getType() && GeoTimeSerie.TYPE.LONG != gts.getType()) {
        throw new WarpScriptException(getName() + " expects a GTS of type DOUBLE or LONG, but instead got a GTS of type " + gts.getType().name());
      }

      double[] xval = GTSHelper.getTicksAsDouble(gts);
      double[] fval = GTSHelper.getValuesAsDouble(gts);

      int size = gts.size();
      if (size > 2) {
        double bandwidthRatio = Math.min(1.0, (double) bandwidth / size + 1e-12);
        function = (new LoessInterpolator(bandwidthRatio, 0, accuracy)).interpolate(xval, fval);
      } else if (size > 1) {
        function = (new LinearInterpolator()).interpolate(xval, fval);
      } else {
        function = null;
      }
    }
    

    return new WarpScriptSingleValueFillerFunction() {
      @Override
      public void fillTick(long tick, GeoTimeSerie filled, Object invalidValue) throws WarpScriptException {
        if (null != function && function.isValidPoint(tick)) {
          GTSHelper.setValue(filled, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, function.value(tick), false);
        } else if (null != invalidValue) {
          GTSHelper.setValue(filled, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, invalidValue, false);
        }
      }

    };
  }

  @Override
  public String snapshot() {
    StringBuilder sb = new StringBuilder();
    try {
      SNAPSHOT.addElement(sb, bandwidth);
      SNAPSHOT.addElement(sb, accuracy);
    } catch (WarpScriptException wse) {
      sb.append(WarpScriptStack.COMMENT_START);
      sb.append(" Error while snapshoting function " + getName());
      sb.append(" ");
      sb.append(WarpScriptStack.COMMENT_END);
    }
    sb.append(" ");
    sb.append(getName());
    return sb.toString();
  }
}
