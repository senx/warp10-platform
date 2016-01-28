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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generate a quantified version of GTS
 * 
 * QUANTIZE expects the following parameters on the stack:
 * 
 * 2: bounds a list of N bounds definining N + 1 intervals for quantification
 * 1: values a list of N+1 values or an empty list
 */
public class QUANTIZE extends GTSStackFunction  {

  private static final String BOUNDS = "bounds";
  private static final String VALUES = "values";
  

  public QUANTIZE(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();
  
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of target values on top of the stack.");
    }

    List<Object> rankToValue = (List) top;
    
    top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of bounds under the top of the stack.");
    }
    
    //
    // Put bounds into an array
    //
    
    double[] bounds = new double[((List) top).size()];

    //
    // Check that we have enough values
    //
    
    if (!rankToValue.isEmpty() && (rankToValue.size() != bounds.length + 1)) {
      throw new WarpScriptException(getName() + " expected " + (bounds.length + 1) + " values but got " + rankToValue.size());
    }
        
    for (int i = 0; i < bounds.length; i++) {
      bounds[i] = ((Number) ((List) top).get(i)).doubleValue();
    }
    
    //
    // Sort bounds
    //
    
    Arrays.sort(bounds);
    
    //
    // Make sure we don't have duplicate bounds
    //
    
    for (int i = 1; i < bounds.length; i++) {
      if (bounds[i] == bounds[i - 1]) {
        throw new WarpScriptException(getName() + " identified duplicate bounds.");
      }
    }
    
    
    Map<String,Object> params = new HashMap<String, Object>();

    if (!rankToValue.isEmpty()) {
      params.put(VALUES, rankToValue.toArray());      
    }
    
    params.put(BOUNDS, bounds);
    
    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {

    double[] bounds = (double[]) params.get(BOUNDS);    

    if (params.containsKey(VALUES)) {
      return GTSHelper.quantize(gts, bounds, (Object[]) params.get(VALUES));
    } else {
      return GTSHelper.quantize(gts, bounds, null);
    }
    
  }
}
