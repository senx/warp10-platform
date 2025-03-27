//
//   Copyright 2018-2025  SenX S.A.S.
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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptSingleValueFillerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class FILL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  /**
   * Parameters of the WarpScript function
   */
  public static String PARAM_FILLER = "filler";
  public static String PARAM_TICKS = "ticks";
  public static String PARAM_VERIFY = "verify";
  public static String PARAM_INVALID_VALUE = "invalid.value";
  public static String DEPRECATED_PARAM_FORCE_OLD_FILLER_FLAG = "force.old.filler"; // undocumented flag to cover an edge case of filler.interpolate with geo See PR#1397.
  
  /**
   * If a macro is passed as the filler parameter, FILL will use this method to wrap it as the filler function.
   * The macro expects a gts and a tick on the stack and it outputs an object (the value to be filled).
   *
   * @param stack
   * @param macro
   * @return The filler
   * @throws WarpScriptException
   */
  public static WarpScriptSingleValueFillerFunction.Precomputable fillerFromMacro(WarpScriptStack stack, Macro macro) throws WarpScriptException {
    return new WarpScriptSingleValueFillerFunction.Precomputable() {
      @Override
      public WarpScriptSingleValueFillerFunction compute(GeoTimeSerie gts) throws WarpScriptException {
        return new WarpScriptSingleValueFillerFunction() {
          @Override
          public void fillTick(long tick, GeoTimeSerie gtsFilled, Object invalidValue) throws WarpScriptException {
            stack.push(gts);
            stack.push(tick);
            stack.exec(macro);
            Object out = stack.pop();
            if (null != out) {
              GTSHelper.setValue(gts, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, out, false);
            } else if (null != invalidValue) {
              GTSHelper.setValue(gts, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, invalidValue, false);
            }
          }
        };
      }
    };
  }

  public FILL(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    if (stack.peek() instanceof WarpScriptFillerFunction || stack.peek() instanceof WarpScriptSingleValueFillerFunction || stack.peek() instanceof Macro) {
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

    } else if (filler instanceof WarpScriptSingleValueFillerFunction) {
      gts = GTSHelper.fill(gtsa, gtsb, (WarpScriptSingleValueFillerFunction) filler);

    } else if (filler instanceof Macro) {
      gts = GTSHelper.fill(gtsa, gtsb, fillerFromMacro(stack, (Macro) filler));

    } else {
      throw new WarpScriptException(getName() + " expects a filler or a macro as last parameter, but instead got a " + TYPEOF.typeof(filler));
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
    boolean verify = true;
    Object v = params.get(PARAM_VERIFY);
    if (null != v) {
      if (!(v instanceof Boolean)) {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_VERIFY + " to be a BOOLEAN, but instead got a " + TYPEOF.typeof(v));
      }
      verify = (boolean) v;
    }

    Object ticks = params.get(PARAM_TICKS);
    if (null != ticks) {
      if (!(ticks instanceof List)) {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_TICKS + " to be a LIST, but instead got a " + TYPEOF.typeof(ticks));
      }
      if(((List)ticks).size() > 0) {
        for (Object o: (List) ticks) {
          if (!(o instanceof Long)) {
            throw new WarpScriptException(getName() + " expects parameter " + PARAM_TICKS + " to be a LIST of LONG, but it contains a " + TYPEOF.typeof(o));
          }
        }
        if (verify) {
          ticks = sortDedupTicks((List<Long>) ticks);
        }
      } else {
        ticks = null;
      }
    }

    Object filler = params.get(PARAM_FILLER);
    if (filler instanceof Macro) {
      Macro macro = (Macro) filler;
      filler = fillerFromMacro(stack, macro);
    }
    if (!(filler instanceof WarpScriptFillerFunction) && !(filler instanceof WarpScriptSingleValueFillerFunction)) {
      throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER + " to be a filler or a macro, but instead got a " + TYPEOF.typeof(filler));
    }

    Object invalidValue = params.get(PARAM_INVALID_VALUE);

    boolean forceOldFiller = false; // this is an undocumented flag. The new filler.interpolate add more geo than the previous one. Use it to force the previous (slow) behavior 
    Object f = params.get(DEPRECATED_PARAM_FORCE_OLD_FILLER_FLAG);
    if (f instanceof Boolean) {
      forceOldFiller = (boolean) f;
    }
    
    List res = new ArrayList<GeoTimeSerie>();
    if (stack.peek() instanceof GeoTimeSerie) {
      if (!forceOldFiller && filler instanceof WarpScriptSingleValueFillerFunction) {
        res.add(GTSHelper.fill((GeoTimeSerie) stack.pop(), (WarpScriptSingleValueFillerFunction) filler, (List<Long>) ticks, verify, invalidValue));

      } else if (filler instanceof WarpScriptFillerFunction) {
        res.add(GTSHelper.fill((GeoTimeSerie) stack.pop(), (WarpScriptFillerFunction) filler, (List<Long>) ticks, verify, invalidValue));

      } else {
        throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER + " to be a filler, but instead got a " + TYPEOF.typeof(filler));
      }

    } else if (stack.peek() instanceof List) {
      for (Object o: (List) stack.pop()) {
        if (!(o instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " expects a LIST of GTS, but instead the list contains a " + TYPEOF.typeof(o));
        }
        if (!forceOldFiller && filler instanceof WarpScriptSingleValueFillerFunction) {
          res.add(GTSHelper.fill((GeoTimeSerie) o, (WarpScriptSingleValueFillerFunction) filler, (List<Long>) ticks, verify, invalidValue));
        } else if (filler instanceof WarpScriptFillerFunction) {
          res.add(GTSHelper.fill((GeoTimeSerie) o, (WarpScriptFillerFunction) filler, (List<Long>) ticks, verify, invalidValue));

        } else {
          throw new WarpScriptException(getName() + " expects parameter " + PARAM_FILLER + " to be a filler, but instead got a " + TYPEOF.typeof(filler));
        }
      }

    } else {
      throw new WarpScriptException(getName() + "expects a GTS or a LIST of GTS before the MAP of parameters");
    }

    stack.push(res);

    return stack;
  }

  private List<Long> sortDedupTicks(List<Long> ticks) {
    if (ticks.size() < 2) {
      return ticks;
    }

    // check that the list is sorted, sort if needed
    long previousTick = ticks.get(0);
    for (int i = 1; i < ticks.size(); i++) {
      long t = ticks.get(i);
      if (t < previousTick) {
        Collections.sort(ticks);
        break;
      }
      previousTick = t;
    }

    // deduplicate if needed
    long[] deduplicatedTicks = null;
    int idx2 = 0;

    Long lasttick = null;

    int idx = 0;

    int n = ticks.size();

    while (idx < n) {
      Long tick = ticks.get(idx);
      if (tick.equals(lasttick)) { // Duplicate tick
        if (null == deduplicatedTicks) { // First duplicate tick
          deduplicatedTicks = new long[ticks.size() - 1];
          idx2 = 0;
          // Copy the first idx -1 values
          for (int i = 0; i < idx - 1; i++) {
            deduplicatedTicks[idx2++] = ticks.get(i);
          }
        }
      } else if (null != deduplicatedTicks) { // Already encountered a duplicate tick, store tick
        deduplicatedTicks[idx2++] = tick;
      }
      lasttick = tick;
      idx++;
    }
    if (null != deduplicatedTicks) {
      ticks = new ArrayList<Long>(idx2);
      for (int i = 0; i < idx2; i++) {
        ticks.add(deduplicatedTicks[i]);
      }
    }

    return ticks;
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

    Object filler = params.get(1);
    if (filler instanceof Macro) {
      Macro macro = (Macro) filler;
      filler = fillerFromMacro(stack, macro);
    }
    if (!(filler instanceof WarpScriptFillerFunction) && !(filler instanceof WarpScriptSingleValueFillerFunction)) {
      throw new WarpScriptException(getName() + " expects the second parameter of the input LIST to be a filler or a macro, but instead got a " + TYPEOF.typeof(filler));
    }

    List<Long> ticks = null;
    if (3 == params.size() && null != params.get(2)) {
      if (!(params.get(2) instanceof List)) {
        throw new WarpScriptException(getName() + "expects the last parameter of the input LIST to be a LIST");
      }
      if (((List) params.get(2)).size() > 0) {
        ticks = new ArrayList<Long>();
        for (Object o: (List) params.get(2)) {
          if (!(o instanceof Long)) {
            throw new WarpScriptException(getName() + " expects the last parameter of the input LIST to be a LIST of LONG, but it contains a " + TYPEOF.typeof(o));
          }
        }
        ticks = sortDedupTicks((List<Long>) (params.get(2)));
      }
    }

    List res = new ArrayList<GeoTimeSerie>();
    if (params.get(0) instanceof GeoTimeSerie) {
      if (filler instanceof WarpScriptSingleValueFillerFunction) {
        res.add(GTSHelper.fill((GeoTimeSerie) params.get(0), (WarpScriptSingleValueFillerFunction) filler, ticks, true, null));

      } else if (filler instanceof WarpScriptFillerFunction) {
        res.add(GTSHelper.fill((GeoTimeSerie) params.get(0), (WarpScriptFillerFunction) filler, ticks, true, null));

      } else {
        throw new WarpScriptException(getName() + " expects a filler, but instead got a " + TYPEOF.typeof(filler));
      }

    } else if (params.get(0) instanceof List) {
      for (Object o: (List) params.get(0)) {
        if (!(o instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " expects a LIST of GTS as first parameter in the input LIST, but instead the list contains a " + TYPEOF.typeof(o));
        }
        if (filler instanceof WarpScriptSingleValueFillerFunction) {
          res.add(GTSHelper.fill((GeoTimeSerie) o, (WarpScriptSingleValueFillerFunction) filler, ticks, true, null));

        } else if (filler instanceof WarpScriptFillerFunction) {
          res.add(GTSHelper.fill((GeoTimeSerie) o, (WarpScriptFillerFunction) filler, ticks, true, null));

        } else {
          throw new WarpScriptException(getName() + " expects a filler, but instead got a " + TYPEOF.typeof(filler));
        }
      }

    } else {
      throw new WarpScriptException(getName() + "expects the first parameter of the input LIST to be a GTS or a LIST of GTS");
    }

    stack.push(res);

    return stack;
  }
}
