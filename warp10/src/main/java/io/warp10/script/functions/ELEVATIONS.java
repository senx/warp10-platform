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

import com.geoxp.GeoXPLib;

/**
 * Extract the elevations of a GTS and push them onto the stack.
 * 
 * Only the ticks with actual values are returned
 */
public class ELEVATIONS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ELEVATIONS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects a geo time series on top of the stack.");
    }

    //
    // Sort GTS
    //
    
    GTSHelper.sort((GeoTimeSerie) o);

    List<Object> elevations = new ArrayList<Object>();
    
    int nvalues = GTSHelper.nvalues((GeoTimeSerie) o);
    
    for (int i = 0; i < nvalues; i++) {
      long elevation = GTSHelper.elevationAtIndex((GeoTimeSerie) o, i);
      
      if (GeoTimeSerie.NO_ELEVATION == elevation) {
        elevations.add(Double.NaN);
      } else {
        elevations.add(elevation);
      }
    }
    
    stack.push(elevations);
    
    return stack;
  }
}
