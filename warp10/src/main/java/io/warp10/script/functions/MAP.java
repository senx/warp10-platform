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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptATCException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Apply a mapper on some GTS instances
 */
public class MAP extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String PARAM_MAPPER = "mapper";
  private static final String PARAM_PREWINDOW = "pre";
  private static final String PARAM_POSTWINDOW = "post";
  @Deprecated
  // Use PARAM_OCCURRENCES which fixes typo.
  private static final String PARAM_OCCURENCES = "occurences";
  private static final String PARAM_OCCURRENCES = "occurrences";
  private static final String PARAM_STEP = "step";
  private static final String PARAM_OVERRIDE = "override";
  private static final String PARAM_OUTPUTTICKS = "ticks";

  public MAP(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (top instanceof Map) {

      //This Case handle the new (20150826) parameter passing mechanism      
      return applyWithParamsFromMap(stack, (Map) top);
    }

    //This Case handle the original parameter passing mechanism

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list as input or a map of parameters on top of a list of GTS.");
    }

    List<Object> params = (List<Object>) top;

    int nseries = 0;

    for (Object param: params) {
      if (!(param instanceof GeoTimeSerie) && !(param instanceof List)) {
        break;
      }
      nseries++;
    }

    if (0 == nseries) {
      throw new WarpScriptException(getName() + " expects Geo Time Series or lists thereof as first parameters.");
    }

    if (nseries == params.size() || !(params.get(nseries) instanceof WarpScriptMapperFunction) && !(params.get(nseries) instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a mapper function or a macro after Geo Time Series.");
    }

    for (int i = params.size(); i <= nseries + 3; i++) {
      params.add(0L);
    }

    if (!(params.get(nseries + 1) instanceof Long) || !(params.get(nseries + 2) instanceof Long) || !(params.get(nseries + 3) instanceof Long)) {
      throw new WarpScriptException(getName() + " expects prewindow, postwindow and occurrences as 3 parameters following the mapper function.");
    }

    int step = 1;

    if (params.size() > nseries + 4) {
      if (!(params.get(nseries + 4) instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a step parameter that is an integer number.");
      } else {
        step = ((Number) params.get(nseries + 4)).intValue();

        if (step <= 0) {
          throw new WarpScriptException(getName() + " expects a step parameter which is strictly positive.");
        }
      }
    }

    boolean overrideTick = false;

    if (params.size() > nseries + 5) {
      if (!(params.get(nseries + 5) instanceof Boolean)) {
        throw new WarpScriptException(getName() + " expects a boolean as 'override tick' parameter.");
      } else {
        overrideTick = Boolean.TRUE.equals(params.get(nseries + 5));
      }
    }

    List<Object> series = new ArrayList<Object>(nseries);

    for (int i = 0; i < nseries; i++) {
      series.add(params.get(i));
    }
    stack.push(series);

    Map<String,Object> mapParams = new HashMap<String, Object>();

    mapParams.put(PARAM_MAPPER, params.get(nseries));
    mapParams.put(PARAM_PREWINDOW, params.get(nseries + 1));
    mapParams.put(PARAM_POSTWINDOW, params.get(nseries + 2));
    mapParams.put(PARAM_OCCURRENCES, params.get(nseries + 3));
    mapParams.put(PARAM_STEP, (long) step);
    mapParams.put(PARAM_OVERRIDE, overrideTick);

    return applyWithParamsFromMap(stack, mapParams);
  }

  private Object applyWithParamsFromMap(WarpScriptStack stack, Map params) throws WarpScriptException {
    //
    // Get and check parameters
    //

    // Initialize to default value
    long prewindow = 0L;
    long postwindow = 0L;
    long occurrences = 0L;
    int step = 1;
    boolean overrideTick = false;

    if (!params.containsKey(PARAM_MAPPER)) {
      throw new WarpScriptException(getName() + " Missing '" + PARAM_MAPPER + "' parameter.");
    }

    Object mapper = params.get(PARAM_MAPPER);

    Object prewindowParam = params.get(PARAM_PREWINDOW);
    if (prewindowParam instanceof Long) {
      prewindow = ((Long) prewindowParam).longValue();
    } else if (params.containsKey(PARAM_PREWINDOW)) {
      throw new WarpScriptException(getName() + " expects the " + PARAM_PREWINDOW + " parameter to be a LONG.");
    }

    Object postwindowParam = params.get(PARAM_POSTWINDOW);
    if (postwindowParam instanceof Long) {
      postwindow = ((Long) postwindowParam).longValue();
    } else if (params.containsKey(PARAM_POSTWINDOW)) {
      throw new WarpScriptException(getName() + " expects the " + PARAM_POSTWINDOW + " parameter to be a LONG.");
    }

    // Backward compatibility, accept deprecated PARAM_OCCURENCES parameter.
    Object occurencesParam = params.get(PARAM_OCCURENCES);
    if (occurencesParam instanceof Long) {
      occurrences = ((Long) occurencesParam).longValue();
    } else if (params.containsKey(PARAM_OCCURENCES)) {
      throw new WarpScriptException(getName() + " expects the " + PARAM_OCCURENCES + " parameter to be a LONG.");
    }

    Object occurrencesParam = params.get(PARAM_OCCURRENCES);
    if (occurrencesParam instanceof Long) {
      occurrences = ((Long) occurrencesParam).longValue();
    } else if (params.containsKey(PARAM_OCCURRENCES)) {
      throw new WarpScriptException(getName() + " expects the " + PARAM_OCCURRENCES + " parameter to be a LONG.");
    }

    // Make sure Math.abs(occurrences) will return a positive value.
    if (Long.MIN_VALUE == occurrences) {
      occurrences = Long.MIN_VALUE + 1;
    }
    final boolean reversed = occurrences < 0;

    Object stepParam = params.get(PARAM_STEP);
    if (stepParam instanceof Long) {
      // Cap step to Integer.MAX_VALUE, which means the mapper will be run once.
      step = (int) Math.min(((Long) stepParam).longValue(), Integer.MAX_VALUE);
    } else if (params.containsKey(PARAM_STEP)) {
      throw new WarpScriptException(getName() + " expects the " + PARAM_STEP + " parameter to be a LONG.");
    }

    Object overrideParam = params.get(PARAM_OVERRIDE);
    if (overrideParam instanceof Boolean) {
      overrideTick = ((Boolean) overrideParam).booleanValue();
    } else if (params.containsKey(PARAM_OVERRIDE)) {
      throw new WarpScriptException(getName() + " expects the " + PARAM_OVERRIDE + " parameter to be a BOOLEAN.");
    }

    Object outputTicks = params.get(PARAM_OUTPUTTICKS);
    // Make sure outputTicks is a List<Long>, and that the list is sorted accordingly.
    if (null != outputTicks) {
      if (!(outputTicks instanceof List)) {
        throw new WarpScriptException(getName() + " expects '" + PARAM_OUTPUTTICKS + "' to be list of LONG values.");
      }
      for (Object tick: (List) outputTicks) {
        if (!(tick instanceof Long)) {
          throw new WarpScriptException(getName() + " expects '" + PARAM_OUTPUTTICKS + "' to be list of LONG values.");
        }
      }

      // Check if outputTicks is correctly sorted
      // In case of a concurrent execution, sorting outputTicks here would lead to a ConcurrentModificationException
      if (((List<Long>) outputTicks).size() > 1) {
        if (reversed) { // reversed
          boolean descending = true;
          int i = 1;
          long lastElt = ((List<Long>) outputTicks).get(0);
          long elt;
          while (i < ((List<Long>) outputTicks).size() && descending) {
            elt = ((List<Long>) outputTicks).get(i);
            descending = elt <= lastElt;
            lastElt = elt;
            i++;
          }
          if (!descending) {
            throw new WarpScriptException(getName() + " expects a reverse sorted list for " + PARAM_OUTPUTTICKS + " parameter.");
          }
        } else {
          boolean ascending = true;
          int i = 1;
          long lastElt = ((List<Long>) outputTicks).get(0);
          long elt;
          while (i < ((List<Long>) outputTicks).size() && ascending) {
            elt = ((List<Long>) outputTicks).get(i);
            ascending = elt >= lastElt;
            lastElt = elt;
            i++;
          }
          if (!ascending) {
            throw new WarpScriptException(getName() + " expects a sorted list for " + PARAM_OUTPUTTICKS + " parameter.");
          }
        }
      }
    }

    //
    // Handle gts and nested list of gts
    //

    Object top = stack.pop();

    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

    // top is expected to be a GTS, a list of GTS or a list of list of GTS
    if (top instanceof List) {
      for (Object o : (List) top) {
        if (o instanceof List) {
          // top is a list of list, o must be a list of gts
          for (Object oo : (List) o) {
            // o must be a gts
            if (!(oo instanceof GeoTimeSerie)) {
              throw new WarpScriptException(getName() + " operates on lists of Geo Time Series.");
            } else {
              series.add((GeoTimeSerie) oo);
            }
          }
        } else {
          // top is a list of gts, o must be a gts
          if (!(o instanceof GeoTimeSerie)) {
            throw new WarpScriptException(getName() + " operates on lists of Geo Time Series.");
          } else {
            series.add((GeoTimeSerie) o);
          }
        }
      }
    } else {
      // top must be a gts
      if (!(top instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " operates on lists of Geo Time Series.");
      } else {
        series.add((GeoTimeSerie) top);
      }
    }

    //
    // Call GTSHelper#map
    //

    List<Object> mapped = new ArrayList<Object>();

    for (GeoTimeSerie gts: series) {
      List<GeoTimeSerie> res;
      try {
        res = GTSHelper.map(gts, mapper, prewindow, postwindow, Math.abs(occurrences), reversed, step, overrideTick, mapper instanceof Macro ? stack : null, (List<Long>) outputTicks);
      } catch (WarpScriptATCException wsatce) {
        // Do not handle WarpScriptATCException (STOP in MACROMAPPER for instance)
        throw wsatce;
      } catch (WarpScriptException wse) {
        throw new WarpScriptException(getName() + " was given invalid parameters.", wse);
      }

      if (res.size() < 2) {
        mapped.addAll(res);
      } else {
        mapped.add(res);
      }
    }

    //
    // Push to stack and return
    //

    stack.push(mapped);

    return stack;
  }
}
