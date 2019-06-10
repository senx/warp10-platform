//
//   Copyright 2018  SenX S.A.S.
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
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extract the value/location/elevation at 'index' of the GTS on top of the stack
 */
public class ATINDEX extends GTSStackFunction {

  private static final String INDEX = "INDEX";

  public ATINDEX(String name) {
    super(name);
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    int idx = (Integer) params.get(INDEX);

    idx = GET.computeAndCheckIndex(this, idx, GTSHelper.nvalues(gts));

    return getTupleAtIndex(gts, idx);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Map<String, Object> params = new HashMap<String, Object>();

    Object o = stack.pop();

    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects an index on top of the stack.");
    }

    int idx = ((Number) o).intValue();

    params.put(INDEX, idx);

    return params;
  }

  public static List<Object> getTupleAtIndex(GeoTimeSerie gts, int idx) {
    List<Object> result = new ArrayList<Object>();

    if (idx < 0 || idx >= GTSHelper.nvalues(gts)) {
      result.add(Double.NaN);
      result.add(Double.NaN);
      result.add(Double.NaN);
      result.add(Double.NaN);
      result.add(null);
    } else {

      Object value = GTSHelper.valueAtIndex(gts, idx);
      long elevation = GTSHelper.elevationAtIndex(gts, idx);
      long location = GTSHelper.locationAtIndex(gts, idx);


      result.add(GTSHelper.tickAtIndex(gts, idx));

      if (GeoTimeSerie.NO_LOCATION != location) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(location);
        result.add(latlon[0]);
        result.add(latlon[1]);
      } else {
        result.add(Double.NaN);
        result.add(Double.NaN);
      }

      if (GeoTimeSerie.NO_ELEVATION != elevation) {
        result.add(elevation);
      } else {
        result.add(Double.NaN);
      }

      result.add(value);
    }

    return result;
  }
}
