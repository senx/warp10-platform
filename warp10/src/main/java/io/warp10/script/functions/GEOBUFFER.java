//
//   Copyright 2020-2021  SenX S.A.S.
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

import org.wololo.jts2geojson.GeoJSONReader;
import org.wololo.jts2geojson.GeoJSONWriter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.buffer.BufferOp;
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
  public static final String KEY_MDIST = "mdist";
  public static final String KEY_PARAMS = "params";
  private static final String KEY_CAP = "cap";
  private static final String KEY_JOIN = "join";
  private static final String KEY_SEGMENTS = "segments";
  private static final String KEY_LIMIT = "limit";
  public static final String KEY_WKB = "wkb";
  public static final String KEY_WKT = "wkt";
  public static final String KEY_GEOJSON = "geojson";
  public static final String KEY_SINGLESIDED = "singlesided";
  
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
    
    double distance = DEFAULT_DISTANCE;
    
    if (map.containsKey(KEY_DIST)) {
      distance = Double.parseDouble(String.valueOf(map.get(KEY_DIST)));
    } else if (map.containsKey(KEY_MDIST)) {
      distance = Double.parseDouble(String.valueOf(map.get(KEY_MDIST)));
      // Convert in center angle in degrees
      distance = distance * (360.0D / (1852.0D * 360.0D * 60.0D));
    }
    
    buffer.put(KEY_DIST, distance);
    
    int quadrantSegments = Integer.parseInt(String.valueOf(map.getOrDefault(KEY_SEGMENTS, BufferParameters.DEFAULT_QUADRANT_SEGMENTS)));
    int endCapStyle = BufferParameters.CAP_ROUND;
    int joinStyle = BufferParameters.JOIN_ROUND;
    
    if (map.containsKey(KEY_CAP)) {
      String cap = String.valueOf(map.get(KEY_CAP));
      if ("SQUARE".equalsIgnoreCase(cap)) {
        endCapStyle = BufferParameters.CAP_SQUARE;
      } else if ("FLAT".equalsIgnoreCase(cap)) {
        endCapStyle = BufferParameters.CAP_FLAT;
      } else if ("ROUND".equalsIgnoreCase(cap)) {
        endCapStyle = BufferParameters.CAP_ROUND;
      }
    }
      
    if (map.containsKey(KEY_JOIN)) {
      String join = String.valueOf(map.get(KEY_JOIN));
      if ("BEVEL".equalsIgnoreCase(join)) {
        joinStyle = BufferParameters.JOIN_BEVEL;
      } else if ("MITRE".equalsIgnoreCase(join)) {
        joinStyle = BufferParameters.JOIN_MITRE;
      } else if ("ROUND".equalsIgnoreCase(join)) {
        joinStyle = BufferParameters.JOIN_ROUND;
      }
    }
    
    double mitreLimit = BufferParameters.DEFAULT_MITRE_LIMIT;
    
    if (map.containsKey(KEY_LIMIT)) {
      mitreLimit = Double.parseDouble(String.valueOf(map.get(KEY_LIMIT)));
    }
    
    BufferParameters params = new BufferParameters(quadrantSegments, endCapStyle, joinStyle, mitreLimit);

    if (map.containsKey(KEY_SINGLESIDED)) {
      params.setSingleSided(Boolean.TRUE.equals(map.get(KEY_SINGLESIDED)));
    }

    //
    // If any of KEY_WKB, KEY_WKT or KEY_GEOJSON is set, simply compute buffer and output modified geometry definition
    //
    
    if (map.get(KEY_WKB) instanceof byte[]) {
      if (map.containsKey(KEY_WKT) || map.containsKey(KEY_GEOJSON)) {
        throw new WarpScriptException(getName() + " only one of '" + KEY_WKB + "', '" + KEY_WKT + "' or '" + KEY_GEOJSON + "' can be specified.");
      }
      WKBReader reader = new WKBReader();

      Geometry geometry = null;

      try {
        byte[] bytes = (byte[]) map.get(KEY_WKB);
        geometry = reader.read(bytes);
        BufferOp bop = new BufferOp(geometry, params);
        geometry = bop.getResultGeometry(distance);
        WKBWriter writer = new WKBWriter();
        stack.push(writer.write(geometry));
      } catch (ParseException pe) {
        throw new WarpScriptException(getName() + " expects valid WKB BYTES.", pe);
      }
    } else if (map.get(KEY_WKT) instanceof String) {
      if (map.containsKey(KEY_WKB) || map.containsKey(KEY_GEOJSON)) {
        throw new WarpScriptException(getName() + " only one of '" + KEY_WKB + "', '" + KEY_WKT + "' or '" + KEY_GEOJSON + "' can be specified.");
      }      
      WKTReader reader = new WKTReader();
      Geometry geometry = null;

      try {
        geometry = reader.read(((String) map.get(KEY_WKT)).toString());
        BufferOp bop = new BufferOp(geometry, params);
        geometry = bop.getResultGeometry(distance);
        stack.push(geometry.toText());
      } catch (ParseException pe) {
        throw new WarpScriptException(getName() + " expects a valid WKT STRING.", pe);
      }
    } else if (map.get(KEY_GEOJSON) instanceof String) {
      if (map.containsKey(KEY_WKT) || map.containsKey(KEY_WKB)) {
        throw new WarpScriptException(getName() + " only one of '" + KEY_WKB + "', '" + KEY_WKT + "' or '" + KEY_GEOJSON + "' can be specified.");
      }
      GeoJSONReader reader = new GeoJSONReader();
      Geometry geometry = null;

      try {
        geometry = reader.read((String) map.get(KEY_GEOJSON));
        BufferOp bop = new BufferOp(geometry, params);
        geometry = bop.getResultGeometry(distance);
        GeoJSONWriter writer = new GeoJSONWriter();
        stack.push(writer.write(geometry).toString());
      } catch (UnsupportedOperationException uoe) {
        throw new WarpScriptException(getName() + " expects a valid GeoJSON STRING.", uoe);
      }
    } else {
      buffer.put(KEY_PARAMS, params);
      stack.setAttribute(ATTR_GEOBUFFER, buffer);      
    }    
    
    return stack;
  }
}
