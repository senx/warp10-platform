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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.vividsolutions.jts.io.gml2.GMLWriter;
import org.wololo.jts2geojson.GeoJSONWriter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.operation.buffer.BufferOp;
import com.vividsolutions.jts.operation.buffer.BufferParameters;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

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
  public static final String KEY_GML = "gml";
  public static final String KEY_KML = "kml";
  public static final String KEY_SINGLESIDED = "singlesided";

  public static final String[] ALL_INPUT_GEOMETRY_KEYS = new String[]{KEY_WKB, KEY_WKT, KEY_GEOJSON, KEY_GML, KEY_KML};
  
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
    // If any of KEY_WKB, KEY_WKT, KEY_GEOJSON, KEY_GML or KEY_KML is set, simply compute buffer and output modified geometry definition
    //

    // Check only one on these keys is set.
    String keySet = null;
    for (String key: ALL_INPUT_GEOMETRY_KEYS) {
      if (map.containsKey(key)) {
        if (null != keySet) {
          throw new WarpScriptException(getName() + " only one of '" + KEY_WKB + "', '" + KEY_WKT + "', '" + KEY_GEOJSON + "', '" + KEY_GML + "' or '" + KEY_KML + "' can be specified.");
        }
        keySet = key;
      }
    }

    Object inputGeometry = map.get(keySet);

    if (KEY_WKB.equals(keySet)) {
      if (!(inputGeometry instanceof byte[])) {
        throw new WarpScriptException(getName() + " expects WKB to be of type BYTES.");
      }

      try {
        Geometry geometry = GeoWKB.wkbToGeometry((byte[]) inputGeometry);
        BufferOp bop = new BufferOp(geometry, params);
        geometry = bop.getResultGeometry(distance);
        WKBWriter writer = new WKBWriter();
        stack.push(writer.write(geometry));
      } catch (ParseException pe) {
        throw new WarpScriptException(getName() + " expects valid WKB BYTES.", pe);
      }
    } else if (KEY_WKT.equals(keySet)) {
      if (!(inputGeometry instanceof String)) {
        throw new WarpScriptException(getName() + " expects WKT to be of type STRING.");
      }

      try {
        Geometry geometry = GeoWKT.wktToGeometry((String) inputGeometry);
        BufferOp bop = new BufferOp(geometry, params);
        geometry = bop.getResultGeometry(distance);
        stack.push(geometry.toText());
      } catch (ParseException pe) {
        throw new WarpScriptException(getName() + " expects a valid WKT STRING.", pe);
      }
    } else if (KEY_GEOJSON.equals(keySet)) {
      if (!(inputGeometry instanceof String)) {
        throw new WarpScriptException(getName() + " expects GeoJSON to be of type STRING.");
      }

      try {
        Geometry geometry = GeoJSON.geoJSONToGeometry((String) inputGeometry);
        BufferOp bop = new BufferOp(geometry, params);
        geometry = bop.getResultGeometry(distance);
        GeoJSONWriter writer = new GeoJSONWriter();
        stack.push(writer.write(geometry).toString());
      } catch (UnsupportedOperationException uoe) {
        throw new WarpScriptException(getName() + " expects a valid GeoJSON STRING.", uoe);
      }
    } else if (KEY_GML.equals(keySet)) {
      if (!(inputGeometry instanceof String)) {
        throw new WarpScriptException(getName() + " expects GML to be of type STRING.");
      }

      try {
        Geometry geometry = GeoGML.GMLToGeometry((String) inputGeometry);
        BufferOp bop = new BufferOp(geometry, params);
        geometry = bop.getResultGeometry(distance);
        stack.push(TOGML.GeometryToGML(geometry));
      } catch (IOException | ParserConfigurationException | SAXException e) {
        throw new WarpScriptException(getName() + " expects a valid GML STRING.", e);
      }
    } else if (KEY_KML.equals(keySet)) {
      if (!(inputGeometry instanceof String)) {
        throw new WarpScriptException(getName() + " expects KML to be of type STRING.");
      }

      try {
        Geometry geometry = GeoKML.KMLToGeometry((String) inputGeometry);
        BufferOp bop = new BufferOp(geometry, params);
        geometry = bop.getResultGeometry(distance);
        stack.push(TOKML.GeometryToKML(geometry));
      } catch (ParserConfigurationException | SAXException | IOException e) {
        throw new WarpScriptException(getName() + " expects a valid KML STRING.", e);
      }
    } else {
      buffer.put(KEY_PARAMS, params);
      stack.setAttribute(ATTR_GEOBUFFER, buffer);
    }    
    
    return stack;
  }
}
