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

import java.util.ArrayList;
import java.util.List;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Clip a Geo Time Series according to a series of limits
 */
public class CLIP extends ATINDEX implements WarpScriptStackFunction {
  
  public CLIP(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of limit pairs on top of the stack.");
    }
    
    List<Object> limits = (List<Object>) top;
    
    top = stack.pop();
    
    if (!(top instanceof GeoTimeSerie) && !(top instanceof GTSEncoder)) {
      throw new WarpScriptException(getName() + " operates on a Geo Time Series or encoder.");
    }
     
    GeoTimeSerie gts = top instanceof GeoTimeSerie ? (GeoTimeSerie) top : null;
    GTSEncoder encoder = top instanceof GTSEncoder ? (GTSEncoder) top: null;

    List<Object> clipped = new ArrayList<Object>();
    
    for (Object o: limits) {
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " expects a list of limit pairs on top of the stack.");
      }
      
      List<Object> pair = (List<Object>) o;
      
      if (2 != pair.size()) {
        throw new WarpScriptException(getName() + " expects a list of limit pairs on top of the stack.");
      }
      
      long lower = ((Number) pair.get(0)).longValue();
      long upper = ((Number) pair.get(1)).longValue();
      
      if (lower > upper) {
        long tmp = lower;
        lower = upper;
        upper = tmp;
      }
      
      if (null != gts) {
        clipped.add(GTSHelper.timeclip(gts, lower, upper));
      } else {
        clipped.add(GTSHelper.timeclip(encoder, lower, upper));
      }
    }
    
    stack.push(clipped);
    
    return stack;
  }
}
