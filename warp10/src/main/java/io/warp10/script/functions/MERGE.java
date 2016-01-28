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
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

/**
 * Apply merge on GTS instances
 */
public class MERGE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public MERGE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list as input.");
    }
    
    List<Object> params = (List<Object>) top;
    
    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

    for (int i = 0; i < params.size(); i++) {      
      if (params.get(i) instanceof GeoTimeSerie) {
        series.add((GeoTimeSerie) params.get(i));
      } else if (params.get(i) instanceof List) {
        for (Object o: (List) params.get(i)) {
          if (!(o instanceof GeoTimeSerie)) {
            throw new WarpScriptException(getName() + " expects a list of geo time series as first parameter.");
          }
          series.add((GeoTimeSerie) o);
        }      
      }      
    }
    
    //
    // We clone the first element so we don't risk merging it with itself
    //
    
    GeoTimeSerie base = series.get(0).clone();
    
    for (int i = 1; i < series.size(); i++) {
      GTSHelper.merge(base, series.get(i));
    }
    
    stack.push(base);
    
    return stack;
  }
}
