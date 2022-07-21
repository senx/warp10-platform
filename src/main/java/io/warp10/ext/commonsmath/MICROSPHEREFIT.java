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

package io.warp10.ext.commonsmath;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.PRNG;
import org.apache.commons.math3.analysis.MultivariateFunction;
import org.apache.commons.math3.analysis.interpolation.InterpolatingMicrosphere;
import org.apache.commons.math3.analysis.interpolation.MicrosphereProjectionInterpolator;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.UnitSphereRandomVectorGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Fit an interpolating micro sphere as described in http://www.dudziak.com/microsphere.pdf
 * Used for multivariate interpolation
 */
public class MICROSPHEREFIT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String MAXDARKFRACTION = "max.dark.fraction";
  public static final String DARKTHRESHOLD = "dark.threshold";
  public static final String BACKGROUND = "background";
  public static final String EXPONENT = "exponent";
  public static final String NOINTERPOLATIONTOLERANCE = "no.interpolation.tolerance";

  private static final Map<String, Object> defaultInterpolationParams;
  static {
    defaultInterpolationParams = new HashMap<String, Object>();
    defaultInterpolationParams.put(MAXDARKFRACTION, 0.5);
    defaultInterpolationParams.put(DARKTHRESHOLD, 1e-2);
    defaultInterpolationParams.put(BACKGROUND, Double.NaN);
    defaultInterpolationParams.put(EXPONENT, 1.1);
    defaultInterpolationParams.put(NOINTERPOLATIONTOLERANCE, Math.ulp(1d));
  }

  private static class MICROSPHERE extends NamedWarpScriptFunction implements WarpScriptStackFunction, WarpScriptReducerFunction {
    private final MultivariateFunction func;

    private MICROSPHERE(MultivariateFunction function) {
      super("MICROSPHERE");
      func = function;
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object o = stack.pop();
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " expects a LIST");
      }
      List l = (List) o;

      double[] point = new double[l.size()];
      for (int i = 0; i < l.size(); i++) {
        point[i] = ((Number) l.get(i)).doubleValue();
      }

      double res = func.value(point);
      stack.push(res);

      return stack;
    }

    @Override
    public Object apply(Object[] args) throws WarpScriptException {
      long tick = (long) args[0];
      long[] locations = (long[]) args[4];
      long[] elevations = (long[]) args[5];
      Object[] values = (Object[]) args[6];

      double[] point = new double[values.length];
      for (int i = 0; i < values.length; i++) {
        point[i] = ((Number) values[i]).doubleValue();
      }
      double res = func.value(point);

      return new Object[] { tick, locations[0], elevations[0], res };
    }

    @Override
    public String toString() {
      throw new RuntimeException(getName() + " snapshotability not implemented yet");
    }
  }

  final private boolean seeded;
  public MICROSPHEREFIT(String name, boolean seeded) {
    super(name);
    this.seeded = seeded;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object o = stack.pop();

    Map<String, Object> interpolationParams = defaultInterpolationParams;
    if (o instanceof Map) {
      interpolationParams.putAll((Map<String, Object>) o);
      o = stack.pop();
    }

    double[][] xval;
    double[] yval;

    if (o instanceof GeoTimeSerie) {
      GeoTimeSerie yGts = (GeoTimeSerie) o;

      if (GeoTimeSerie.TYPE.DOUBLE != yGts.getType()) {
        throw new WarpScriptException(getName() + " expects GTS wth values of type DOUBLE.");
      }

      o = stack.pop();
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " expects a List of GTS as first argument.");
      }
      for (Object g : (List) o) {
        if (!(g instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " expects a List of GTS as first argument.");
        }
        GeoTimeSerie gts = (GeoTimeSerie) g;

        if (GeoTimeSerie.TYPE.DOUBLE != gts.getType()) {
          throw new WarpScriptException(getName() + " expects GTS wth values of type DOUBLE.");
        }

        // check synchronicity
        if (gts.size() != yGts.size()) {
          throw new WarpScriptException(getName() + ": incoherent GTS sizes.");
        }
        for (int i = 0; i < gts.size(); i++) {
          if (GTSHelper.tickAtIndex(gts, i) != GTSHelper.tickAtIndex(yGts, i)) {
            throw new WarpScriptException(getName() + ": GTS are not synchronized.");
          }
        }
      }
      List<GeoTimeSerie> lgts = (List<GeoTimeSerie>) o;

      yval = GTSHelper.doubleValues((GeoTimeSerie) o);
      xval = new double[yval.length][lgts.size()];
      for (int i = 0; i < yval.length; i++) {
        for (int j = 0; j < lgts.size(); j++) {
          xval[i][j] = (double) GTSHelper.valueAtIndex(lgts.get(j), i);
        }
      }

    } else if (o instanceof List) {

      for (Object it: (List) o) {
        if (!(it instanceof Double)) {
          throw new WarpScriptException(getName() + " expects values of type DOUBLE.");
        }
      }
      List<Double> yList = (List<Double>) o;

      o = stack.pop();
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " expects a List of List as first argument.");
      }
      List xList = (List) o;
      if (xList.size() != yList.size()) {
        throw new WarpScriptException(getName() + ": incoherent input list sizes.");
      }

      int d = -1;
      for (Object l : xList) {
        if (!(l instanceof List)) {
          throw new WarpScriptException(getName() + " expects a List of List as first argument.");
        }

        List point = (List) l;

        if (-1 == d) {
          d = point.size();
        } else if (point.size() != d) {
          throw new WarpScriptException(getName() + ": incoherent input list sizes.");
        }

        for (Object it: point) {
          if (!(it instanceof Double)) {
            throw new WarpScriptException(getName() + " expects values of type DOUBLE.");
          }
        }
      }

      xval = new double[xList.size()][d];
      yval = new double[yList.size()];
      for (int i = 0; i < xList.size(); i++) {
        yval[i] = yList.get(i);
        for (int j = 0; j < d; j++) {
          xval[i][j] = ((List<Double>) xList.get(i)).get(j);
        }
      }

    } else {
      throw new WarpScriptException(getName() + ": wrong argument type");
    }

    double maxDarkFraction = ((Number) interpolationParams.get(MAXDARKFRACTION)).doubleValue();
    double darkThreshold = ((Number) interpolationParams.get(DARKTHRESHOLD)).doubleValue();
    double background = ((Number) interpolationParams.get(BACKGROUND)).doubleValue();
    double exponent = ((Number) interpolationParams.get(EXPONENT)).doubleValue();
    double noInterpolationTolerance = ((Number) interpolationParams.get(NOINTERPOLATIONTOLERANCE)).doubleValue();


    MicrosphereProjectionInterpolator interpolator;

    if (!seeded) {
      interpolator = new MicrosphereProjectionInterpolator(xval[0].length, xval.length, maxDarkFraction, darkThreshold, background, exponent, false, noInterpolationTolerance);

    } else {
      Random prng = (Random) stack.getAttribute(PRNG.ATTRIBUTE_SEEDED_PRNG);

      if (null == prng) {
        throw new WarpScriptException(getName() + " seeded PRNG was not initialized.");
      }

      RandomGenerator prng2 = new JDKRandomGenerator(prng.nextInt());
      UnitSphereRandomVectorGenerator rvg = new UnitSphereRandomVectorGenerator(xval[0].length, prng2);
      InterpolatingMicrosphere microsphere = new InterpolatingMicrosphere(xval[0].length, xval.length, maxDarkFraction, darkThreshold, background, rvg);

      interpolator = new MicrosphereProjectionInterpolator(microsphere, exponent, false, noInterpolationTolerance);
    }

    MultivariateFunction func = interpolator.interpolate(xval, yval);

    stack.push(new MICROSPHERE(func));

    return stack;
  }
}
