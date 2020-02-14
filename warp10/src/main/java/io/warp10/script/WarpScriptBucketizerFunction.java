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

package io.warp10.script;


//
// Aggregators for bucketization
//
// Object[] args contains in this order:
//
// bucket: end timestamp of bucket in us since epoch
// names: array of class names of GTS being aggregated (identical when used as an aggregator)
// labels: array of labels of GTS being aggregated (ditto)
// ticks: array of ticks being aggregated
// locations: array of locations
// elevations: array of elevations
// values: array of values
// window: 4 parameters of the map window (prewindow, postwindow, starttimestamp, stoptimestamp)
//
// They return an array of:
//
// tick, location, elevation, value
//

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;

import java.util.Arrays;
import java.util.Map;

public interface WarpScriptBucketizerFunction {
  public Object apply(Object[] args) throws WarpScriptException;

  default Object apply(GeoTimeSerie subgts, long bucketend) throws WarpScriptException {

    Object[] parms =  new Object[8];

    parms[0] = bucketend;
    parms[1] = new String[]{subgts.getName()};
    parms[2] = new Map[]{subgts.getLabels()};
    parms[3] = GTSHelper.getTicks(subgts);
    if (subgts.hasLocations()) {
      parms[4] = GTSHelper.getLocations(subgts);
    } else {
      parms[4] = new long[subgts.size()];
      Arrays.fill((long[]) parms[4], GeoTimeSerie.NO_LOCATION);
    }
    if (subgts.hasElevations()) {
      parms[5] = GTSHelper.getElevations(subgts);
    } else {
      parms[5] = new long[subgts.size()];
      Arrays.fill((long[]) parms[5], GeoTimeSerie.NO_ELEVATION);
    }
    parms[6] = new Object[subgts.size()];
    parms[7] = new long[] {0, -1, bucketend, bucketend};

    for (int j = 0; j < subgts.size(); j++) {
      ((Object[]) parms[6])[j] = GTSHelper.valueAtIndex(subgts, j);
    }

    return apply(parms);
  }
}
