//
//   Copyright 2017  Cityzen Data
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

import org.wololo.jts2geojson.GeoJSONReader;
import com.geoxp.GeoXPLib;
import com.vividsolutions.jts.geom.Geometry;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Converts a Geo JSON Text String into a GeoXP Shape suitable for geo filtering
 */
public class GeoJSON extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public GeoJSON(String name) {
    super(name);
  }
  
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object inside = stack.pop();
    Object pcterror = stack.pop();
    Object geoJson = stack.pop();
    
    if (!(geoJson instanceof String) || !(inside instanceof Boolean) || !(pcterror instanceof Double)) {
      throw new WarpScriptException(getName() + " expects a GeoJSON String, an error percentage and a boolean as the top 3 elements of the stack.");
    }
    
    //
    // Read Geo JSON
    //
    
    GeoJSONReader reader = new GeoJSONReader();
    Geometry geometry = null;
    
    try {
      geometry = reader.read((String) geoJson);
    } catch (UnsupportedOperationException uoe) {
      throw new WarpScriptException(uoe);
    }
    
    //
    // Convert Geometry to a GeoXPShape
    //

    stack.push(GeoXPLib.toGeoXPShape(geometry, ((Number) pcterror).doubleValue(), Boolean.TRUE.equals(inside)));

    return stack;
  }
}