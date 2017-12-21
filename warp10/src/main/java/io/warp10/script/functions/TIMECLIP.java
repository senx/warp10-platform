//
//   Copyright 2016  Cityzen Data
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

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;

/**
 * Only retain ticks within the given timerange
 */
public class TIMECLIP extends GTSStackFunction  {

  private static final String START = "start";
  private static final String END = "end";


  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  public TIMECLIP(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {


    Object top = stack.pop();

    long start;

    boolean iso8601 = false;

    if (top instanceof String) {
      iso8601 = true;
      if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_1_8)) {
        start = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(top.toString());
      } else {
        start = fmt.parseDateTime(top.toString()).getMillis() * Constants.TIME_UNITS_PER_MS;
      }
    } else if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects either an ISO8601 timestamp as the origin timestamp or a duration.");
    } else {
      start = (long) top;
    }

    long end;

    top = stack.pop();

    if (top instanceof String) {      
      if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_1_8)) {
        end = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(top.toString());
      } else {
        end = fmt.parseDateTime(top.toString()).getMillis() * Constants.TIME_UNITS_PER_MS;
      }
    } else if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects either an ISO8601 timestamp or a delta since Unix Epoch as 'now' parameter.");
    } else {
      end = (long) top;
    }

    if (!iso8601) {
      start = end - start + 1;
    }

    Map<String,Object> params = new HashMap<String, Object>();

    params.put(START, start);
    params.put(END, end);

    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    long start = (long) params.get(START);
    long end = (long) params.get(END);

    GeoTimeSerie result = GTSHelper.timeclip(gts, start, end);

    return result;
  }
}
