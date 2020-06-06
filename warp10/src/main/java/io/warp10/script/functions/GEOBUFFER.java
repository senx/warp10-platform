//
//   Copyright 2020  SenX S.A.S.
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

import java.util.HashMap;
import java.util.Map;

import com.vividsolutions.jts.operation.buffer.BufferParameters;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class GEOBUFFER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public static final String ATTR_GEOBUFFER = "geo.buffer";
  
  /**
   * Default distance, 1 m expressed in central angle degrees
   */
  private static final double DEFAULT_DISTANCE = 360.0D / (1852.0D * 360.0D * 60.0D);
  
  public static final String KEY_DIST = "dist";
  public static final String KEY_PARAMS = "params";
  private static final String KEY_CAP = "cap";
  private static final String KEY_JOIN = "join";
  private static final String KEY_SEGMENTS = "segments";
  private static final String KEY_LIMIT = "limit";
  public static final String KEY_WKT = "wkt";
  
  public GEOBUFFER(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a buffer definition map.");
    }
    
    Map<Object,Object> map = (Map<Object,Object>) top;

    //
    // Check keys and build buffer parameters
    //
    
    Map<String,Object> buffer = new HashMap<String, Object>();
    
    double distance = Double.parseDouble(String.valueOf(map.getOrDefault(KEY_DIST, DEFAULT_DISTANCE)));
    buffer.put(KEY_DIST, distance);
    
    int quadrantSegments = Integer.parseInt(String.valueOf(map.getOrDefault(KEY_SEGMENTS, BufferParameters.DEFAULT_QUADRANT_SEGMENTS)));
    int endCapStyle = BufferParameters.CAP_ROUND;
    int joinStyle = BufferParameters.JOIN_ROUND;
    
    if (map.containsKey(KEY_CAP)) {
      if ("SQUARE".equalsIgnoreCase(String.valueOf(map.get(KEY_CAP)))) {
        endCapStyle = BufferParameters.CAP_SQUARE;
      } else if ("FLAT".equalsIgnoreCase(String.valueOf(map.get(KEY_CAP)))) {
        endCapStyle = BufferParameters.CAP_FLAT;
      }
    }
      
    if (map.containsKey(KEY_JOIN)) {
      if ("BEVEL".equalsIgnoreCase(String.valueOf(map.get(KEY_JOIN)))) {
        endCapStyle = BufferParameters.JOIN_BEVEL;
      } else if ("MITRE".equalsIgnoreCase(String.valueOf(map.get(KEY_JOIN)))) {
        endCapStyle = BufferParameters.JOIN_MITRE;
      }      
    }
    
    double mitreLimit = BufferParameters.DEFAULT_MITRE_LIMIT;
    
    if (map.containsKey(KEY_LIMIT)) {
      mitreLimit = Double.parseDouble(String.valueOf(map.get(KEY_LIMIT)));
    }
    
    buffer.put(KEY_WKT, Boolean.TRUE.equals(map.get(KEY_WKT)));
    
    BufferParameters params = new BufferParameters(quadrantSegments, endCapStyle, joinStyle, mitreLimit);
    buffer.put(KEY_PARAMS, params);
    stack.setAttribute(ATTR_GEOBUFFER, buffer);
    
    return stack;
  }
}
