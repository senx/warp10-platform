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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Fit a polynomial curve to a GTS
 */
public class POLYFIT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public POLYFIT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    boolean useBucketId = false;

    if (top instanceof Boolean) {
      useBucketId = Boolean.TRUE.equals(top);
      top = stack.pop();
    }

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a polynomial degree on top if the stack.");
    }

    PolynomialCurveFitter fitter = PolynomialCurveFitter.create(((Number) top).intValue());

    top = stack.pop();

    if (!(top instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects a Geo Time Series below the degree of the polynomial.");
    }

    GeoTimeSerie gts = (GeoTimeSerie) top;

    int n = GTSHelper.nvalues(gts);

    if (0 == n) {
      throw new WarpScriptException(getName() + " operates on a non empty Geo Time Series.");
    }

    if (useBucketId && !GTSHelper.isBucketized(gts)) {
      throw new WarpScriptException(getName() + " use of bucket id is only possible for bucketized Geo Time Series.");
    }

    if (TYPE.LONG != gts.getType() && TYPE.DOUBLE != gts.getType()) {
      throw new WarpScriptException(getName() + " can only operate on numerical Geo Time Series.");
    }

    //
    // Build the list of observations
    //

    long firsttick = GTSHelper.firsttick(gts);
    long bucketspan = GTSHelper.getBucketSpan(gts);

    WeightedObservedPoints points = new WeightedObservedPoints();

    for (int i = 0; i < n; i++) {
      Object value = GTSHelper.valueAtIndex(gts, i);

      long tick = GTSHelper.tickAtIndex(gts, i);

      if (useBucketId) {
        //
        // Compute bucket id
        //

        int bucket = (int) ((tick - firsttick) / bucketspan);

        points.add(bucket, ((Number) value).doubleValue());
      } else {
        points.add(tick, ((Number) value).doubleValue());
      }
    }

    //
    // Limit the number of iterations to twice the number of datapoints we attempt to fit
    //

    fitter.withMaxIterations(n * 2);

    double[] coeffs = fitter.fit(points.toList());

    List<Object> lcoeffs = new ArrayList<Object>(coeffs.length);

    for (Object coeff: coeffs) {
      lcoeffs.add(coeff);
    }

    stack.push(lcoeffs);

    return stack;
  }
}
