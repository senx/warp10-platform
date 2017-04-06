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

import java.io.IOException;
import java.util.List;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Builds an encoder from a list of tick,lat,lon,elev,value
 */
public class TOENCODER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOENCODER(String name) {
    super(name);
  }  

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list on top of the stack.");
    }
    
    List<Object> elements = (List<Object>) top;
    
    GTSEncoder encoder = new GTSEncoder(0L);
    
    for (Object element: elements) {
      if (!(element instanceof List)) {
        throw new WarpScriptException(getName() + " encountered an invalid element.");
      }
      
      List<Object> elt = (List<Object>) element;
      
      if (elt.size() < 2 || elt.size() > 5) {
        throw new WarpScriptException(getName() + " encountered an invalid element.");
      }
      
      Object tick = elt.get(0);
      
      if (!(tick instanceof Long)) {
        throw new WarpScriptException(getName() + " encountered an invalid timestamp.");
      }
      
      Object value = null;
      long location = GeoTimeSerie.NO_LOCATION;
      long elevation = GeoTimeSerie.NO_ELEVATION;
      
      if (2 == elt.size()) { // tick,value
        value = elt.get(1);
      } else if (3 == elt.size()) { // tick,elevation,value
        Object elev = elt.get(1);
        if (elev instanceof Number) {
          if (!Double.isNaN(((Number) elev).doubleValue())) {
            elevation = ((Number) elev).longValue();
          }
        } else {
          throw new WarpScriptException(getName() + " encountered an invalid element.");
        }
        value = elt.get(2);
      } else if (4 == elt.size()) { // tick,lat,lon,value
        Object lat = elt.get(1);
        Object lon = elt.get(2);
        if (lat instanceof Number && lon instanceof Number) {
          if (!Double.isNaN(((Number) lat).doubleValue()) && !Double.isNaN(((Number) lon).doubleValue())) {
            location = GeoXPLib.toGeoXPPoint(((Number) lat).doubleValue(), ((Number) lon).doubleValue());
          }
        } else {
          throw new WarpScriptException(getName() + " encountered an invalid element.");
        }
        value = elt.get(3);
      } else {
        Object lat = elt.get(1);
        Object lon = elt.get(2);
        if (lat instanceof Number && lon instanceof Number) {
          if (!Double.isNaN(((Number) lat).doubleValue()) && !Double.isNaN(((Number) lon).doubleValue())) {
            location = GeoXPLib.toGeoXPPoint(((Number) lat).doubleValue(), ((Number) lon).doubleValue());
          }
        } else {
          throw new WarpScriptException(getName() + " encountered an invalid element.");
        }
        Object elev = elt.get(3);
        if (elev instanceof Number) {
          if (!Double.isNaN(((Number) elev).doubleValue())) {
            elevation = ((Number) elev).longValue();
          }
        } else {
          throw new WarpScriptException(getName() + " encountered an invalid element.");
        }
        value = elt.get(4);
      }
      
      try {
        encoder.addValue((long) tick, location, elevation, value);
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " unable to add value.", ioe);
      }
    }
    
    stack.push(encoder);

    return stack;
  }  
}
