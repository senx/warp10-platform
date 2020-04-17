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
import com.geoxp.geo.GeoHashHelper;
import com.geoxp.geo.HHCodeHelper;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Convert a HHCode lat/lon or GeoHash, or a list of HHCodes to a GeoXPShape
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

    if (top instanceof List) {
      List cellList = (List) top;
      ArrayList<Long> cells = new ArrayList<Long>();

      long[] hhAndRes;
      for(Object cell: cellList) {
        // Only support String or byte[] HHCode. Long HHCode are of resolution 32 and cannot be converted to a GeoXPShape.
        // Longs may be Geo Cells which must not be mixed with pure HHCodes.
        if (!(cell instanceof String || cell instanceof byte[])) {
          throw new WarpScriptException(getName() + " expects the given list to contain STRING or BYTES HHCodes.");
        }

        try {
          hhAndRes = HHCODEFUNC.hhAndRes(cell);
        } catch (WarpScriptException wse) {
          throw new WarpScriptException(getName() + " expects the given list to contain valid STRING or BYTES HHCodes.", wse);
        }
        long hh = hhAndRes[0];
        int res = (int) hhAndRes[1]; // We know hhAndRes returns an int here.

        cells.add(HHCodeHelper.toGeoCell(hh, res));
      }

      stack.push(GeoXPLib.fromCells(cells));
    } else {
      // Optional: boolean to select GeoHash/Lat-Lon output
      boolean geohashOutput = false;
      if (top instanceof Boolean) {
        geohashOutput = (Boolean) top;
        top = stack.pop();
      }

      long[] hhAndRes;
      try {
        hhAndRes = HHCODEFUNC.hhAndRes(top);
      } catch (WarpScriptException wse) {
        throw new WarpScriptException(getName() + " was given unexpected arguments.", wse);
      }
      long hh = hhAndRes[0];
      int res = (int) hhAndRes[1]; // We know hhAndRes returns an int here.

      if(geohashOutput) {
        // This is an approximation of the HHCode with a GeoHash
        stack.push(GeoHashHelper.fromHHCode(hh, res));
      } else {
        if (useGtsConvention && GeoTimeSerie.NO_LOCATION == hh) {
          stack.push(Double.NaN);
          stack.push(Double.NaN);
        } else {
          // Lat/Lon of the SW coordinates of the HHCode
          double[] latlon = GeoXPLib.fromGeoXPPoint(hh);
          stack.push(latlon[0]);
          stack.push(latlon[1]);
        }
      }
    }

    return stack;
  }
}
