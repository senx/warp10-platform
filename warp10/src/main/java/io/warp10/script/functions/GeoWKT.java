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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import com.geoxp.GeoXPLib;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Converts a Well Known Text String into a GeoXP Shape suitable for geo filtering
 */
public class GeoWKT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean uniform;
  
  public GeoWKT(String name, boolean uniform) {
    super(name);
    this.uniform = uniform;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object inside = stack.pop();
    Object pcterror = stack.pop();
    Object wkt = stack.pop();
  
    if (!this.uniform && (!(wkt instanceof String) || !(inside instanceof Boolean) || !(pcterror instanceof Double))) {
      throw new WarpScriptException(getName() + " expects a WKT string, an error percentage and a boolean as the top 3 elements of the stack.");
    }

    if (this.uniform && (!(wkt instanceof String) || !(inside instanceof Boolean) || (!(pcterror instanceof Double) && !(pcterror instanceof Long)))) { 
      throw new WarpScriptException(getName() + " expects a WKT string, an error percentage or resolution (even number between 2 and 30) and a boolean as the top 3 elements of the stack.");
    }

    //
    // Read WKT
    //
    
    WKTReader reader = new WKTReader();    
    Geometry geometry = null;
    
    try {
      geometry = reader.read(wkt.toString());
    } catch (ParseException pe) {
      throw new WarpScriptException(pe);
    }
    
    //
    // Convert Geometry to a GeoXPShape
    //
    
    int maxcells = ((Number) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MAX_GEOCELLS)).intValue();
    
    if (!this.uniform) {
      stack.push(GeoXPLib.toGeoXPShape(geometry, ((Number) pcterror).doubleValue(), Boolean.TRUE.equals(inside)));
    } else {
      if (pcterror instanceof Double) {
        stack.push(GeoXPLib.toUniformGeoXPShape(geometry, ((Number) pcterror).doubleValue(), Boolean.TRUE.equals(inside), maxcells));
      } else {
        stack.push(GeoXPLib.toUniformGeoXPShape(geometry, ((Number) pcterror).intValue(), Boolean.TRUE.equals(inside), maxcells));
      }
    }
    
    return stack;
  }
}
