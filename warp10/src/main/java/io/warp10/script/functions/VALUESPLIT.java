//
//   Copyright 2018-2023  SenX S.A.S.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Split a GTS into N distinct GTS, one for each distinct value
 *
 * @param name Name of label to use for storing the value
 */
public class VALUESPLIT  extends GTSStackFunction {

  private static final String PARAM_LABEL = "label";

  public VALUESPLIT(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a label name on top of the stack.");
    }

    Map<String,Object> params = new HashMap<String, Object>();
    params.put(PARAM_LABEL, top.toString());

    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {

    String label = params.get(PARAM_LABEL).toString();

    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

    //
    // Handle boolean GTS in a specific way
    //

    if (GeoTimeSerie.TYPE.BOOLEAN == gts.getType()) {
      GeoTimeSerie tgts = gts.cloneEmpty(gts.size() / 2);
      tgts.getMetadata().putToLabels(label, "true");
      GeoTimeSerie fgts = gts.cloneEmpty(gts.size() / 2);
      fgts.getMetadata().putToLabels(label, "false");

      for (int i = 0; i < gts.size(); i++) {
        long tick = GTSHelper.tickAtIndex(gts, i);
        long location = GTSHelper.locationAtIndex(gts, i);
        long elevation = GTSHelper.elevationAtIndex(gts, i);
        Object value = GTSHelper.valueAtIndex(gts, i);

        if (Boolean.TRUE.equals(value)) {
          GTSHelper.setValue(tgts, tick, location, elevation, value, false);
        } else {
          GTSHelper.setValue(fgts, tick, location, elevation, value, false);
        }
      }

      GTSHelper.shrink(fgts);
      GTSHelper.shrink(tgts);

      if (fgts.size() > 0) {
        series.add(fgts);
      }
      if (tgts.size() > 0) {
        series.add(tgts);
      }
      return series;
    }

    //
    // Sort gts by values
    //

    GTSHelper.valueSort(gts);


    GeoTimeSerie split = null;
    Object lastvalue = null;

    for (int i = 0; i < gts.size(); i++) {
      long tick = GTSHelper.tickAtIndex(gts, i);
      long location = GTSHelper.locationAtIndex(gts, i);
      long elevation = GTSHelper.elevationAtIndex(gts, i);
      Object value = GTSHelper.valueAtIndex(gts, i);

      if (!value.equals(lastvalue)) {
        split = gts.cloneEmpty();
        split.getMetadata().putToLabels(label, value.toString());
        series.add(split);
      }

      GTSHelper.setValue(split, tick, location, elevation, value, false);

      lastvalue = value;
    }

    return series;
  }
}
