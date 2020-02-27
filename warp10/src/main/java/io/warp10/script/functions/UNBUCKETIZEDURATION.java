//
//   Copyright 2020  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import org.joda.time.DateTimeZone;

import java.util.Map;

/**
 * Unbucketizes GTS that were previously duration-bucketized.
 * Restore timestamps instead bucket indices.
 * See DURATIONBUCKETIZE.
 */
public class UNBUCKETIZEDURATION extends GTSStackFunction {

  public UNBUCKETIZEDURATION(String name) {
    super(name);
  }

  public UNBUCKETIZEDURATION() {
    super(getDefaultName());
  }

  public static String getDefaultName() {
    return WarpScriptLib.UNBUCKETIZE_DURATION;
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    return null;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {

    if (!GTSHelper.isBucketized(gts)) {
      throw new WarpScriptException(getName() + " expects input GTS to be bucketized.");
    }

    if (!BUCKETIZEDURATION.isDurationBucketized(gts)) {
      throw new WarpScriptException(getName() + " expects input GTS to be duration-bucketized. This information is stored in attributes.");
    }

    ADDDURATION.ReadWritablePeriodWithSubSecondOffset bucketperiod = ADDDURATION.durationToPeriod(gts.getMetadata().getAttributes().get(BUCKETIZEDURATION.DURATION_ATTRIBUTE_KEY));
    long bucketoffset = Long.parseLong(gts.getMetadata().getAttributes().get(BUCKETIZEDURATION.OFFSET_ATTRIBUTE_KEY));
    DateTimeZone dtz = DateTimeZone.forID(gts.getMetadata().getAttributes().get(BUCKETIZEDURATION.TIMEZONE_ATTRIBUTE_KEY));

    GeoTimeSerie result = gts.cloneEmpty();
    GTSHelper.unbucketize(result);
    result.getMetadata().getAttributes().remove(BUCKETIZEDURATION.DURATION_ATTRIBUTE_KEY);
    result.getMetadata().getAttributes().remove(BUCKETIZEDURATION.OFFSET_ATTRIBUTE_KEY);
    result.getMetadata().getAttributes().remove(BUCKETIZEDURATION.TIMEZONE_ATTRIBUTE_KEY);

    for (int i = 0; i < gts.size(); i++) {

      long tick = ADDDURATION.addPeriod(0, bucketperiod, dtz, GTSHelper.tickAtIndex(gts, i) + 1) - 1 - bucketoffset;
      GTSHelper.setValue(result, tick, GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), GTSHelper.valueAtIndex(gts, i), false);
    }

    return result;
  }
}
