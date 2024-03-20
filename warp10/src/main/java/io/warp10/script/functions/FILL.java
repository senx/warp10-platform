//
//   Copyright 2018-2024  SenX S.A.S.
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

import java.util.List;
import java.util.Map;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class FILL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static String PARAM_OCCURRENCES = MAP.PARAM_OCCURRENCES;
  public static String PARAM_FILLER = "filler";
  public static String PARAM_FILLER_ELEV = "filler.elev";
  public static String PARAM_FILLER_LOC = "filler.loc";
  public static String PARAM_INVALID_TICK_VALUE = "invalid.tick.val";
  public static String PARAM_INVALID_TICK_ELEV = "invalid.tick.elev";
  public static String PARAM_INVALID_TICK_LAT = "invalid.tick.lat";
  public static String PARAM_INVALID_TICK_LON = "invalid.tick.lon";

  public FILL(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    if (stack.peek() instanceof WarpScriptFillerFunction) {
      return crossFillApply(stack);
    } else {
      return univariateFillApply(stack);
    }
  }

  private Object crossFillApply(WarpScriptStack stack) throws WarpScriptException {
    WarpScriptFillerFunction filler = (WarpScriptFillerFunction) stack.pop();

    Object top = stack.pop();

    if (!(top instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expected a " + TYPEOF.typeof(GeoTimeSerie.class) + ", but instead got a " + TYPEOF.typeof(top));
    }

    GeoTimeSerie gtsb = (GeoTimeSerie) top;

    top = stack.pop();

    if (!(top instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expected a " + TYPEOF.typeof(GeoTimeSerie.class) + ", but instead got a " + TYPEOF.typeof(top));
    }

    GeoTimeSerie gtsa = (GeoTimeSerie) top;

    List<GeoTimeSerie> gts = GTSHelper.fill(gtsa, gtsb, filler);

    stack.push(gts.get(0));
    stack.push(gts.get(1));

    return stack;
  }

  private Object univariateFillApply(WarpScriptStack stack) throws WarpScriptException {
    if (stack.peek() instanceof Map) {
      return applyFromMap(stack, (Map) stack.pop());
    } else if (stack.peek() instanceof List) {
      return applyFromList(stack, (List) stack.pop());
    } else {
     throw new WarpScriptException(getName() + "'s argument types do not match the expected ones.");
    }
  }

  private Object applyFromMap(WarpScriptStack stack, Map params) throws WarpScriptException {

    //todo

    return stack;
  }

  private Object applyFromList(WarpScriptStack stack, List params) throws WarpScriptException {

    //todo

    return stack;
  }

  private GeoTimeSerie fill(GeoTimeSerie gts, List<Long> occurrences, Object filler, Object fillerElev, Object fillerLoc, Object invalidTickValue, long invalidTickElev, double invalidTickLat, double invalidTickLon) throws WarpScriptException {
    GeoTimeSerie res = gts.clone();

    return res;
  }
}
