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
import org.apache.commons.math3.analysis.interpolation.AkimaSplineInterpolator;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

public class FillerAkima extends NamedWarpScriptFunction implements WarpScriptSingleValueFillerFunction.Precomputable {
  public FillerAkima(String name) {
    super(name);
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
      if (size > 4) {
        function = (new AkimaSplineInterpolator()).interpolate(xval, fval);
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
}
