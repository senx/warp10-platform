//
//   Copyright 2018-2020  SenX S.A.S.
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

import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Converts a HHCode to lat/lon.
 */
public class HHCODETO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean useGtsConvention;

  public HHCODETO(String name) {
    this(name, false);
  }

  public HHCODETO(String name, boolean useGtsConvention) {
    super(name);
    this.useGtsConvention = useGtsConvention;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    long[] hhAndRes;
    try {
      hhAndRes = HHCODEFUNC.hhAndRes(top);
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " was given unexpected arguments.", wse);
    }
    long hh = hhAndRes[0];

    if (useGtsConvention && GeoTimeSerie.NO_LOCATION == hh) {
      stack.push(Double.NaN);
      stack.push(Double.NaN);
    } else {
      // Lat/Lon of the SW coordinates of the HHCode
      double[] latlon = GeoXPLib.fromGeoXPPoint(hh);
      stack.push(latlon[0]);
      stack.push(latlon[1]);
    }

    return stack;
  }
}
