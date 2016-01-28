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

import com.geoxp.GeoXPLib;

/**
 * Add a value to a GTS. Expects 6 parameters on the stack
 * 
 * GTS
 * tick
 * latitude (or NaN)
 * longitude (or NaN)
 * elevation (or NaN)
 * TOP: value
 * 
 */
public class ADDVALUE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean overwrite;
  
  public ADDVALUE(String name, boolean overwrite) {
    super(name);
    this.overwrite = overwrite;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof Number) && !(o instanceof String) && !(o instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects a LONG, DOUBLE, STRING or BOOLEAN value.");
    }
    
    Object value = o;
    
    o = stack.pop();
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects the elevation to be numeric or NaN.");
    }
    
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    if (!(o instanceof Double && Double.isNaN((double) o))) {
      elevation = ((Number) o).longValue();
    }

    o = stack.pop();
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects the longitude to be numeric or NaN.");
    }
    
    double longitude = o instanceof Double ? (double) o : ((Number) o).doubleValue();

    o = stack.pop();
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects the latitude to be numeric or NaN.");
    }
    
    double latitude = o instanceof Double ? (double) o : ((Number) o).doubleValue();

    long location = GeoTimeSerie.NO_LOCATION;
    
    if (!Double.isNaN(latitude) && !Double.isNaN(longitude)) {
      location = GeoXPLib.toGeoXPPoint(latitude, longitude);
    }
    
    o = stack.pop();
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects the tick to be numeric.");
    }
    
    long timestamp = ((Number) o).longValue();
    
    o = stack.pop();
    
    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " operates on a single GTS.");
    }
    
    GeoTimeSerie gts = (GeoTimeSerie) o;
    
    GTSHelper.setValue(gts, timestamp, location, elevation, value, this.overwrite);
    
    stack.push(gts);
    
    return stack;
  }
}
