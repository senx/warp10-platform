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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptUnivariateFillerFunction;

public class FILL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static String PARAM_OCCURRENCES = MAP.PARAM_OCCURRENCES;
  public static String PARAM_FILLER = "filler";
  public static String PARAM_FILLER_ELEV = "filler.elev";
  public static String PARAM_FILLER_LOC = "filler.loc";
  public static String PARAM_INVALID_VALUE = "invalid.val";

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

  /**
   * Expected signature:
   * a:GTS b:GTS c:Filler FILL d:GTS e:GTS
   *
   * @param stack
   * @return
   * @throws WarpScriptException
   */
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

  /**
   * Expected signatures:
   * a:GTS b:MAP FILL res:List<GTS>
   * a:List<GTS> b:MAP FILL res:List<GTS>
   *
   * @param stack
   * @param params
   * @return
   * @throws WarpScriptException
   */
  private Object applyFromMap(WarpScriptStack stack, Map params) throws WarpScriptException {
    Object occurrences = params.get(PARAM_OCCURRENCES);
    if (null != occurrences) {
      if (!(occurrences instanceof List)) {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_OCCURRENCES + " to be a LIST, but instead got a " + TYPEOF.typeof(occurrences));
      }
      for (Object o: (List) occurrences) {
        if (!(o instanceof Long)) {
          throw new WarpScriptException(getName() + " expects parameter " + PARAM_OCCURRENCES + " to be a LIST of LONG, but the LIST contains a " + TYPEOF.typeof(o));
        }
      }
    }

    Object filler = params.get(PARAM_FILLER);
    if (!(filler instanceof WarpScriptUnivariateFillerFunction)) {
      if (filler instanceof WarpScriptFillerFunction) {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER + " to be an univariate filler, but instead got a filler used to cross fill two GTS.");
      } else {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER + " to be a FILLER, but instead got a " + TYPEOF.typeof(filler));
      }
    }

    Object fillerElev = params.get(PARAM_FILLER_ELEV);
    if (null != fillerElev && !(fillerElev instanceof WarpScriptUnivariateFillerFunction)) {
      if (fillerElev instanceof WarpScriptFillerFunction) {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER_ELEV + " to be an univariate filler, but instead got a filler used to cross fill two GTS.");
      } else {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER_ELEV + " to be a filler, but instead got a " + TYPEOF.typeof(fillerElev));
      }
    }

    Object fillerLoc = params.get(PARAM_FILLER_LOC);
    if (null != fillerLoc && !(fillerLoc instanceof WarpScriptUnivariateFillerFunction)) {
      if (fillerLoc instanceof WarpScriptFillerFunction) {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER_LOC + " to be an univariate filler, but instead got a filler used to cross fill two GTS.");
      } else {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER_LOC + " to be a filler, but instead got a " + TYPEOF.typeof(fillerLoc));
      }
    }

    Object invalidValue = params.get(PARAM_INVALID_VALUE);

    List res = new ArrayList<GeoTimeSerie>();
    if (stack.peek() instanceof GeoTimeSerie) {
      res.add(fill((GeoTimeSerie) stack.pop(), (List<Long>) occurrences, (WarpScriptUnivariateFillerFunction) filler, (WarpScriptUnivariateFillerFunction) fillerElev, (WarpScriptUnivariateFillerFunction) fillerLoc, invalidValue));
    } else if (stack.peek() instanceof List) {
      for (Object o: (List) stack.pop()) {
        if (!(o instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " expects a LIST of GTS, but instead the list contains a " + TYPEOF.typeof(o));
        }
        res.add(fill((GeoTimeSerie) o, (List<Long>) occurrences, (WarpScriptUnivariateFillerFunction) filler, (WarpScriptUnivariateFillerFunction) fillerElev, (WarpScriptUnivariateFillerFunction) fillerLoc, invalidValue));
      }
    } else {
      throw new WarpScriptException(getName() + "expects a GTS or a LIST of GTS before the parameter MAP.");
    }

    stack.push(res);

    return stack;
  }

  /**
   * Expected signatures:
   * [ a:GTS b:Filler ] FILL res:List<GTS>
   * [ a:GTS b:Filler c:List<Long> ] FILL res:List<GTS>
   * [ a:List<GTS> b:Filler ] FILL res:List<GTS>
   * [ a:List<GTS> b:Filler c:List<Long> ] FILL res:List<GTS>
   *
   * @param stack
   * @param params
   * @return
   * @throws WarpScriptException
   */
  private Object applyFromList(WarpScriptStack stack, List params) throws WarpScriptException {

    //todo

    return stack;
  }

  private GeoTimeSerie fill(GeoTimeSerie gts, List<Long> occurrences, WarpScriptUnivariateFillerFunction filler, WarpScriptUnivariateFillerFunction fillerElev, WarpScriptUnivariateFillerFunction fillerLoc, Object invalidValue) throws WarpScriptException {
    GeoTimeSerie res = gts.clone();

    //todo

    return res;
  }
}
