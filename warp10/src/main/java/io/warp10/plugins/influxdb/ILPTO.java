//
//   Copyright 2023  SenX S.A.S.
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

package io.warp10.plugins.influxdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class ILPTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ILPTO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects the precision as a STRING.");
    }

    long ilpTimeUnitMultiplier = 0L;

    String precision = (String) top;

    if (null != precision) {
      if ("n".equals(precision)) {
        ilpTimeUnitMultiplier = 1;
      } else if ("u".equals(precision)) {
        ilpTimeUnitMultiplier = 1000L;
      } else if ("ms".equals(precision)) {
        ilpTimeUnitMultiplier = 1000000L;
      } else if ("s".equals(precision)) {
        ilpTimeUnitMultiplier = 1000000000L;
      } else if ("m".equals(precision)) {
        ilpTimeUnitMultiplier = 60 * 1000000000L;
      } else if ("h".equals(precision)) {
        ilpTimeUnitMultiplier = 3600 * 1000000000L;
      } else {
        throw new WarpScriptException(getName() + " Invalid precision, must be one of 'n', 'u', 'ms', 's', 'm' or 'h'.");
      }
    }

    //
    // Adjust the ratio and time unit factor
    //

    long ratio = 1L;

    //
    // If the platform's time unit is not set to ns then adapt ratio and ilpTimeUnitMultiplier
    //

    if (1000000000L != Constants.TIME_UNITS_PER_S) {
      ratio = 1000000000L / Constants.TIME_UNITS_PER_S;

      // Divide each by 1000 until one of the ratios is 1
      while(ratio > 1L && ilpTimeUnitMultiplier > 1L) {
        ratio /= 1000L;
        ilpTimeUnitMultiplier /= 1000L;
      }
    }

    top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a STRING.");
    }

    BufferedReader br = new BufferedReader(new StringReader((String) top));

    Map<UUID, GTSEncoder> currentEncoders = new HashMap<UUID, GTSEncoder>();
    AtomicReference<String> lastLabels = new AtomicReference(null);
    AtomicReference<String> lastMeasurement = new AtomicReference(null);
    AtomicReference<Map<String,String>> curLabels = new AtomicReference(null);

    Map<String,Long> classIds = new LinkedHashMap<String,Long>() {
      @Override
      protected boolean removeEldestEntry(java.util.Map.Entry<String, Long> eldest) {
        return this.size() > 100;
      }
    };

    long threshold = Long.MAX_VALUE;
    String mlabel = ".measurement";

    try {
      InfluxDBHandler.parseMulti(br, mlabel, ilpTimeUnitMultiplier, ratio, threshold, classIds, currentEncoders, lastLabels, lastMeasurement, curLabels);
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " error while parsing InfluxDB Line Protocol.", ioe);
    }

    List<GTSEncoder> encoders = new ArrayList<GTSEncoder>(currentEncoders.size());

    encoders.addAll(currentEncoders.values());

    stack.push(encoders);

    return stack;
  }
}
