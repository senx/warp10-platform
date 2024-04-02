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
import io.warp10.script.WarpScriptSingleValueFillerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class FILL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static String PARAM_TICKS = "ticks";
  public static String PARAM_FILLER = "filler";
  public static String PARAM_INVALID_VALUE = "invalid.value";

  public FILL(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    if (stack.peek() instanceof WarpScriptFillerFunction || stack.peek() instanceof WarpScriptSingleValueFillerFunction) {
      return crossFillApply(stack);
    } else {
      return univariateFillApply(stack);
    }
  }

  /**
   * crossFillApply aligns two gts by ensuring they end up with the same ticks.
   * If a tick is present in one gts but not in the other, it is added to the later and the value is computed using the filler function.
   *
   * Expected signature:
   * a:GTS b:GTS c:Filler FILL d:GTS e:GTS
   *
   * @param stack
   * @return stack
   * @throws WarpScriptException
   */
  private Object crossFillApply(WarpScriptStack stack) throws WarpScriptException {
    Object filler = stack.pop();

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

    List<GeoTimeSerie> gts;
    if (filler instanceof WarpScriptFillerFunction) {
      gts = GTSHelper.fill(gtsa, gtsb, (WarpScriptFillerFunction) filler);
    } else {
      gts = GTSHelper.fill(gtsa, gtsb, (WarpScriptSingleValueFillerFunction) filler);
    }

    stack.push(gts.get(0));
    stack.push(gts.get(1));

    return stack;
  }

  /**
   * univariateFillApply fills missing ticks in each input gts independently, either by filling on specified gaps or on empty buckets.
   *
   * @param stack
   * @return stack
   * @throws WarpScriptException
   */
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
    Object ticks = params.get(PARAM_TICKS);
    if (null != ticks) {
      if (!(ticks instanceof List)) {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_TICKS + " to be a LIST, but instead got a " + TYPEOF.typeof(ticks));
      }
      for (Object o: (List) ticks) {
        if (!(o instanceof Long)) {
          throw new WarpScriptException(getName() + " expects parameter " + PARAM_TICKS + " to be a LIST of LONG, but it contains a " + TYPEOF.typeof(o));
        }
      }
    }

    Object filler = params.get(PARAM_FILLER);
    if (!(filler instanceof WarpScriptFillerFunction) || !(filler instanceof WarpScriptSingleValueFillerFunction)) {
      throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER + " to be a filler, but instead got a " + TYPEOF.typeof(filler));
    }

    Object invalidValue = params.get(PARAM_INVALID_VALUE);

    List res = new ArrayList<GeoTimeSerie>();
    if (stack.peek() instanceof GeoTimeSerie) {
      if (filler instanceof WarpScriptFillerFunction) {
        res.add(GTSHelper.fill((GeoTimeSerie) stack.pop(), (List<Long>) ticks, (WarpScriptFillerFunction) filler, invalidValue));

      } else {
        res.add(GTSHelper.fill((GeoTimeSerie) stack.pop(), (List<Long>) ticks, (WarpScriptSingleValueFillerFunction) filler, invalidValue));
      }

    } else if (stack.peek() instanceof List) {
      for (Object o: (List) stack.pop()) {
        if (!(o instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " expects a LIST of GTS, but instead the list contains a " + TYPEOF.typeof(o));
        }
        if (filler instanceof WarpScriptFillerFunction) {
          res.add(GTSHelper.fill((GeoTimeSerie) o, (List<Long>) ticks, (WarpScriptFillerFunction) filler, invalidValue));

        } else {
          res.add(GTSHelper.fill((GeoTimeSerie) o, (List<Long>) ticks, (WarpScriptSingleValueFillerFunction) filler, invalidValue));
        }
      }

    } else {
      throw new WarpScriptException(getName() + "expects a GTS or a LIST of GTS before the map of parameters");
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
    if (params.size() < 2) {
      throw new WarpScriptException(getName() + " expects an input LIST containing at least two parameters, but got only " +params.size());
    }

    if (params.size() > 3) {
      throw new WarpScriptException(getName() + " expects an input LIST containing at most three parameters, but got " +params.size());
    }

    if (!(params.get(1) instanceof WarpScriptUnivariateFillerFunction)) {
      if (params.get(1) instanceof WarpScriptFillerFunction) {
        throw new WarpScriptException(getName() + " expects the second parameter of the input LIST to be an univariate filler, but instead got a filler used to cross fill two GTS.");
      } else {
        throw new WarpScriptException(getName() + " expects parameter the second parameter of the input LIST to be a filler, but instead got a " + TYPEOF.typeof(params.get(1)));
      }
    }
    WarpScriptUnivariateFillerFunction filler = (WarpScriptUnivariateFillerFunction) params.get(1);

    List<Long> occurrences = null;
    if (3 == params.size()) {
      if (!(params.get(2) instanceof List)) {
        throw new WarpScriptException(getName() + "expects the last parameter of the input LIST to be a LIST");
      }
      for (Object o: (List) params.get(2)) {
        if (!(o instanceof Long)) {
          throw new WarpScriptException(getName() + " expects the last parameter of the input LIST to be a LIST of LONG, but it contains a " + TYPEOF.typeof(o));
        }
      }
      occurrences = (List<Long>) params.get(2);
    }

    List res = new ArrayList<GeoTimeSerie>();
    if (params.get(0) instanceof GeoTimeSerie) {
      res.add(GTSHelper.fill((GeoTimeSerie) params.get(0), occurrences, filler, null));

    } else if (params.get(0) instanceof List) {
      for (Object o: (List) params.get(0)) {
        if (!(o instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " expects a LIST of GTS as first parameter in the input LIST, but instead the list contains a " + TYPEOF.typeof(o));
        }
        res.add(GTSHelper.fill((GeoTimeSerie) o, occurrences, filler, null));
      }

    } else {
      throw new WarpScriptException(getName() + "expects the first parameter of the input LIST to be a GTS or a LIST of GTS");
    }

    stack.push(res);

    return stack;
  }
}
