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

package io.warp10.script;

import io.warp10.FloatUtils;
import io.warp10.WarpURLEncoder;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.UnsafeString;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.functions.SNAPSHOT.Snapshotable;
import io.warp10.warp.sdk.WarpScriptJavaFunctionGTS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.boon.json.JsonSerializer;
import org.boon.json.JsonSerializerFactory;

import com.geoxp.GeoXPLib;

public class StackUtils {
  
  //private static final GsonBuilder GSON_BUILDER = new GsonBuilder().serializeSpecialFloatingPointValues();
  private static final JsonSerializerFactory BOON_FACTORY = new JsonSerializerFactory();
  
  public static void toJSON(PrintWriter out, WarpScriptStack stack, int maxdepth) throws WarpScriptException {
    
    boolean strictJSON = Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_JSON_STRICT));
    
    AtomicInteger recursionLevel = new AtomicInteger(0);
    
    int depth = Math.min(stack.depth(), maxdepth);
    
    out.print("[");
    
    boolean first = true;
    
    for (int i = 0; i < depth; i++) {
      
      if (!first) {
        out.print(",");
      }
      first = false;
      
      stack.push(i);
      try {
        Object o = stack.peekn();
        
        objectToJSON(out, o, recursionLevel, strictJSON);
      } catch (WarpScriptException ee) {
      }
    }
    
    out.print("]");
  }

  public static void toJSON(PrintWriter out, WarpScriptStack stack) throws WarpScriptException {
    toJSON(out, stack, Integer.MAX_VALUE);
  }
  
  public static void objectToJSON(PrintWriter out, Object o, AtomicInteger recursionLevel, boolean strictJSON) {
    
    if (recursionLevel.addAndGet(1) > WarpScriptStack.DEFAULT_MAX_RECURSION_LEVEL && ((o instanceof Map) || (o instanceof List) || (o instanceof Macro))) {
      out.write(" ...NESTED_CONTENT_REMOVED... ");
      recursionLevel.addAndGet(-1);
      return;
    }
    
    //Gson gson = GSON_BUILDER.create();
    JsonSerializer serializer = BOON_FACTORY.create();

    if (strictJSON && (o instanceof Double && (Double.isNaN((double) o) || Double.isInfinite((double) o)))) {
      out.print("null");
    } else if (strictJSON && (o instanceof Float && (Float.isNaN((float) o) || Float.isInfinite((float) o)))) {
      out.print("null");
    } else if (o instanceof Number || o instanceof String || o instanceof Boolean) {
      //out.print(gson.toJson(o));
      out.print(serializer.serialize(o));
    } else if (o instanceof Map) {
      out.print("{");
      boolean first = true;
      for (Object key: ((Map) o).keySet()) {
        if (!first) {
          out.print(",");
        }
        if (null != key) {
          //out.print(gson.toJson(key.toString()));
          out.print(serializer.serialize(key.toString()));
        } else {
          out.print("null");
        }
        out.print(":");
        objectToJSON(out, ((Map) o).get(key), recursionLevel, strictJSON);
        first = false;
      }
      out.print("}");
    } else if (o instanceof List) {
      out.print("[");
      boolean first  = true;
      for (Object elt: ((List) o)) {
        if (!first) {
          out.print(",");
        }
        objectToJSON(out, elt, recursionLevel, strictJSON);
        first = false;
      }
      out.print("]");    
    } else if (o instanceof GeoTimeSerie) {
      out.print("{");
      out.print("\"c\":");
      //out.print(gson.toJson(((GeoTimeSerie) o).getMetadata().getName()));
      String name = ((GeoTimeSerie) o).getMetadata().getName();
      if (null == name) {
        name = "";
      }
      out.print(serializer.serialize(name));
      out.print(",\"l\":");
      objectToJSON(out, ((GeoTimeSerie) o).getMetadata().getLabels(), recursionLevel, strictJSON);
      out.print(",\"a\":");
      objectToJSON(out, ((GeoTimeSerie) o).getMetadata().getAttributes(), recursionLevel, strictJSON);
      out.print(",\"v\":[");
      boolean first = true;
      for (int i = 0; i < ((GeoTimeSerie) o).size(); i++) {
        if (!first) {
          out.print(",");
        }
        long ts = GTSHelper.tickAtIndex((GeoTimeSerie) o, i);
        long location = GTSHelper.locationAtIndex((GeoTimeSerie) o, i);
        long elevation = GTSHelper.elevationAtIndex((GeoTimeSerie) o, i);
        Object v = GTSHelper.valueAtIndex((GeoTimeSerie) o, i);
        out.print("[");
        out.print(ts);
        if (GeoTimeSerie.NO_LOCATION != location) {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          out.print(",");
          out.print(latlon[0]);
          out.print(",");
          out.print(latlon[1]);
        }
        if (GeoTimeSerie.NO_ELEVATION != elevation) {
          out.print(",");
          out.print(elevation);
        }
        out.print(",");
        //out.print(gson.toJson(v));    
        if (strictJSON && (v instanceof Double) && (Double.isNaN((double) v) || Double.isInfinite((double) v))) {
          out.print("null");
        } else {
          out.print(serializer.serialize(v));
        }
        out.print("]");
        first = false;
      }
      out.print("]");
      out.print("}");
    } else if (o instanceof GTSEncoder) {
      out.print("{");
      out.print("\"c\":");
      //out.print(gson.toJson(((GeoTimeSerie) o).getMetadata().getName()));
      String name = ((GTSEncoder) o).getMetadata().getName();
      if (null == name) {
        name = "";
      }
      out.print(serializer.serialize(name));
      out.print(",\"l\":");
      objectToJSON(out, ((GTSEncoder) o).getMetadata().getLabels(), recursionLevel, strictJSON);
      out.print(",\"a\":");
      objectToJSON(out, ((GTSEncoder) o).getMetadata().getAttributes(), recursionLevel, strictJSON);
      out.print(",\"v\":[");
      boolean first = true;
      GTSDecoder decoder = ((GTSEncoder) o).getUnsafeDecoder(false);
      while(decoder.next()) {
        if (!first) {
          out.print(",");
        }
        long ts = decoder.getTimestamp();
        long location = decoder.getLocation();
        long elevation = decoder.getElevation();
        Object v = decoder.getValue();
        out.print("[");
        out.print(ts);
        if (GeoTimeSerie.NO_LOCATION != location) {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          out.print(",");
          out.print(latlon[0]);
          out.print(",");
          out.print(latlon[1]);
        }
        if (GeoTimeSerie.NO_ELEVATION != elevation) {
          out.print(",");
          out.print(elevation);
        }
        out.print(",");
        //out.print(gson.toJson(v));    
        if (strictJSON && (v instanceof Double) && (Double.isNaN((double) v) || Double.isInfinite((double) v))) {
          out.print("null");
        } else {
          out.print(serializer.serialize(v));
        }
        out.print("]");
        first = false;
      }
      out.print("]");
      out.print("}");
      
    } else if (o instanceof Metadata) {
      out.print("{");
      out.print("\"c\":");
      out.print(serializer.serialize(((Metadata) o).getName()));
      out.print(",\"l\":");
      objectToJSON(out, ((Metadata) o).getLabels(), recursionLevel, strictJSON);
      out.print(",\"a\":");
      objectToJSON(out, ((Metadata) o).getAttributes(), recursionLevel, strictJSON);
      out.print("}");
    //} else if (o instanceof JsonArray || o instanceof JsonElement || o instanceof JsonBuilder) {
    //  out.print(o.toString());
    } else if (o instanceof Macro) {
      //out.print(gson.toJson(o.toString()));
      out.print(serializer.serialize(o.toString()));
    } else if (o instanceof NamedWarpScriptFunction) {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(o.toString());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      //out.print(gson.toJson(sb.toString()));
      out.print(serializer.serialize(sb.toString()));
    } else {
      out.print("null");
    }
    
    recursionLevel.addAndGet(-1);
  }
  
  /**
   * Sanitize a script instance, removing comments etc.
   * Inspired by MemoryWarpScriptStack#exec
   * 
   * @param script The script to sanitize.
   * @return A StringBuilder containing the sanitized version of the script.
   */
  public static StringBuilder sanitize(String script) throws WarpScriptException {
    StringBuilder sb = new StringBuilder();
    
    if (null == script) {
      return sb;
    }
    
    //
    // Build a BufferedReader to access each line of the script
    //
    
    BufferedReader br = new BufferedReader(new StringReader(script));
    
    boolean inComment = false;
    
    try {
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        line = line.trim();
        
        String[] statements;
        
        //
        // Replace whistespaces in Strings with '%20'
        //
        
        line = UnsafeString.sanitizeStrings(line);
        
        if (-1 != UnsafeString.indexOf(line, ' ')) {
          //statements = line.split(" +");
          statements = UnsafeString.split(line, ' ');
        } else {
          statements = new String[1];
          statements[0] = line;
        }
        
        //
        // Loop over the statements
        //
        
        for (String stmt: statements) {

          //
          // Skip empty statements
          //
          
          if (0 == stmt.length()) {
            continue;
          }
          
          //
          // Trim statement
          //
          
          stmt = stmt.trim();

          //
          // End execution on encountering a comment
          //
          
          if (stmt.charAt(0) == '#' || (stmt.charAt(0) == '/' && stmt.length() >= 2 && stmt.charAt(1) == '/')) {
            // Skip comments and blank lines
            break;
          }
          
          if (WarpScriptStack.COMMENT_END.equals(stmt)) {
            if (inComment) {
              throw new WarpScriptException("Not inside a comment.");
            }
            inComment = false;
            continue;
          } else if (inComment) {
            continue;
          } else if (WarpScriptStack.COMMENT_START.equals(stmt)) {
            inComment = true;
            continue;
          }
          
          if (sb.length() > 0) {
            sb.append(" ");
          }
          sb.append(stmt);
        }
      }

      br.close();
      
    } catch(IOException ioe) {
      throw new WarpScriptException(ioe);
    }
    
    
    return sb;
  }
  
  /**
   * Convert an object into a representation suitable for interacting with external functions.
   * 
   * The supported types are:
   * 
   * GeoTimeSerie -> converted to WarpScriptJavaFunctionGeoTimeSerie
   * Long/Double/Boolean/String -> passed verbatim
   * List of any of the above objects -> converted to a list of the converted objects. No nested lists
   * Map of any of the objects above 'List' -> converted to a map of the converted objects. No nested maps or maps of lists
   * 
   * Any other object will trigger an exception
   * 
   * @param o The converted object
   * @return
   */
  public static Object toSDKObject(Object o) throws WarpScriptException {
    if (o instanceof String || o instanceof Long || o instanceof Double || o instanceof Boolean) {
      return o;
    } else if (o instanceof GeoTimeSerie) {
      GeoTimeSerie inGTS = (GeoTimeSerie) o;
      
      WarpScriptJavaFunctionGTS gts = new WarpScriptJavaFunctionGTS();
      
      Metadata metadata = inGTS.getMetadata();
      
      gts.gtsClass = inGTS.getName();
      gts.gtsLabels = Collections.unmodifiableMap(metadata.getLabels());
      gts.gtsAttributes = Collections.unmodifiableMap(metadata.getAttributes());
            
      if (GTSHelper.isBucketized(inGTS)) {
        gts.bucketized = true;
        gts.bucketcount = GTSHelper.getBucketCount(inGTS);
        gts.bucketspan = GTSHelper.getBucketSpan(inGTS);
        gts.lastbucket = GTSHelper.getLastBucket(inGTS);
      } else {
        gts.bucketized = false;
      }
      
      if (0 == GTSHelper.nvalues(inGTS)) {
        return gts;
      }
      
      gts.ticks = GTSHelper.getTicks(inGTS);
      
      if (inGTS.hasLocations()) {
        long[] locations = GTSHelper.getOriginalLocations(inGTS);
        gts.latitudes = new float[locations.length];
        gts.longitudes = new float[locations.length];
        
        for (int i = 0; i < locations.length; i++) {
          if (GeoTimeSerie.NO_LOCATION != locations[i]) {
            double[] latlon = GeoXPLib.fromGeoXPPoint(locations[i]);
            gts.latitudes[i] = (float) latlon[0];
            gts.longitudes[i] = (float) latlon[1];            
          } else {
            gts.latitudes[i] = Float.NaN;
            gts.longitudes[i] = Float.NaN;
          }
        }
      } else {
        gts.latitudes = null;
        gts.longitudes = null;
      }
      
      if (inGTS.hasElevations()) {        
        gts.elevations = GTSHelper.getElevations(inGTS);
      } else {
        gts.elevations = null;
      }
      
      switch (inGTS.getType()) {
        case BOOLEAN:
          gts.booleanValues = GTSHelper.booleanValues(inGTS);
          break;
        case DOUBLE:
          gts.doubleValues = GTSHelper.doubleValues(inGTS);
          break;
        case LONG:
          gts.longValues = GTSHelper.longValues(inGTS);
          break;
        case STRING:
          gts.stringValues = GTSHelper.stringValues(inGTS);
          break;
        default:
      }
      
      return gts;
    } else if (o instanceof List) {
      List<Object> newlist = new ArrayList<Object>(((List) o).size());
      for (Object in: (List) o) {
        if (!(in instanceof List) && !(in instanceof Map)) {
          Object out = toSDKObject(in);
          newlist.add(out);
        } else {
          throw new WarpScriptException("Invalid nested complex type.");
        }
      }
      return newlist;
    } else if (o instanceof Map) {
      Map<Object,Object> newmap = new HashMap<Object, Object>(((Map) o).size());
      for (Entry<Object,Object> entry: ((Map<Object,Object>) o).entrySet()) {
        Object key = entry.getKey();
        Object value = entry.getValue();
        if (!(key instanceof List) && !(key instanceof Map) && !(value instanceof List) && !(value instanceof Map)) {
          newmap.put(toSDKObject(key), toSDKObject(value));
        } else {
          throw new WarpScriptException("Invalid nested complex type.");
        }
      }
      return newmap;
    } else {
      throw new WarpScriptException("Unsupported type " + o.getClass());
    }
  }
  
  /**
   * Convert Object instances coming back from an SDK function call
   * @param o Object to convert
   * @return converted object
   * @throws WarpScriptException
   */
  public static Object fromSDKObject(Object o) throws WarpScriptException {
    if (o instanceof String || o instanceof Long || o instanceof Double || o instanceof Boolean) {
      return o;
    } else if (o instanceof Integer || o instanceof Short || o instanceof Byte || o instanceof BigInteger) {
      return ((Number) o).longValue();
    } else if (o instanceof Float || o instanceof BigDecimal) {
      return ((Number) o).doubleValue();
    } else if (o instanceof WarpScriptJavaFunctionGTS) {
      
      WarpScriptJavaFunctionGTS inGTS = (WarpScriptJavaFunctionGTS) o;
      
      int len = null != inGTS.ticks ? inGTS.ticks.length : 0;
      
      GeoTimeSerie gts;
      
      if (inGTS.bucketized) {
        gts = new GeoTimeSerie(inGTS.lastbucket, inGTS.bucketcount, inGTS.bucketspan, len);
      } else {
        gts = new GeoTimeSerie(len);
      }

      Metadata metadata = new Metadata();
      
      if (null != inGTS.gtsClass) {
        metadata.setName(inGTS.gtsClass);
      }
      if (null != inGTS.gtsLabels) {
        metadata.setLabels(new HashMap<String, String>(inGTS.gtsLabels));
      } else {
        metadata.setLabels(new HashMap<String,String>());
      }
      if (null != inGTS.gtsAttributes) {
        metadata.setAttributes(new HashMap<String, String>(inGTS.gtsAttributes));
      } else {
        metadata.setAttributes(new HashMap<String,String>());
      }
            
      if (0 == len) {
        return gts;
      }
      
      boolean hasLocations = false;
      
      if (null != inGTS.latitudes && null != inGTS.longitudes) {
        if (inGTS.latitudes.length != len || inGTS.longitudes.length != len) {
          throw new WarpScriptException("Incoherent number of latitudes (" + inGTS.latitudes.length + ") / longitudes (" + inGTS.longitudes.length + "), expected " + len);
        }
        hasLocations = true;
      } else if (null != inGTS.latitudes) {
        throw new WarpScriptException("Missing longitudes.");
      } else if (null != inGTS.longitudes) {
        throw new WarpScriptException("Missing latitudes.");
      }
      
      boolean hasElevations = false;
      
      if (null != inGTS.elevations) {
        if (inGTS.elevations.length != len) {
          throw new WarpScriptException("Incoherent number of elevations (" + inGTS.elevations.length + "), expected " + len);
        }
      }
      
      TYPE type = TYPE.UNDEFINED;
      
      if (null != inGTS.booleanValues) {
        type = TYPE.BOOLEAN;
        if (inGTS.booleanValues.size() != len) {
          throw new WarpScriptException("Incoherent size for boolean values (" + inGTS.booleanValues.size() + "), expected " + len);
        }
      }
      
      if (null != inGTS.longValues) {
        if (TYPE.UNDEFINED != type) {
          throw new WarpScriptException("Incoherent GTS, multiple value types.");
        }
        type = TYPE.LONG;
        if (inGTS.longValues.length != len) {
          throw new WarpScriptException("Incoherent size for long values (" + inGTS.longValues.length + "), expected " + len);          
        }
      }

      if (null != inGTS.doubleValues) {
        if (TYPE.UNDEFINED != type) {
          throw new WarpScriptException("Incoherent GTS, multiple value types.");
        }
        type = TYPE.DOUBLE;
        if (inGTS.doubleValues.length != len) {
          throw new WarpScriptException("Incoherent size for double values (" + inGTS.doubleValues.length + "), expected " + len);          
        }
      }

      if (null != inGTS.stringValues) {
        if (TYPE.UNDEFINED != type) {
          throw new WarpScriptException("Incoherent GTS, multiple value types.");
        }
        type = TYPE.STRING;
        if (inGTS.stringValues.length != len) {
          throw new WarpScriptException("Incoherent size for string values (" + inGTS.stringValues.length + "), expected " + len);          
        }
      }

      for (int i = 0; i < inGTS.ticks.length; i++) {
        long location = GeoTimeSerie.NO_LOCATION;
        long elevation = GeoTimeSerie.NO_ELEVATION;
        
        if (hasLocations) {
          if (FloatUtils.isFinite(inGTS.latitudes[i]) && FloatUtils.isFinite(inGTS.longitudes[i])) {
            location = GeoXPLib.toGeoXPPoint(inGTS.latitudes[i], inGTS.longitudes[i]);
          }
        }
        
        if (hasElevations && Long.MIN_VALUE != inGTS.elevations[i]) {
          elevation = inGTS.elevations[i];
        }
        
        Object value = null;
        
        switch(type) {
          case BOOLEAN:
            value = inGTS.booleanValues.get(i);
            break;
          case DOUBLE:
            value = inGTS.doubleValues[i];
            break;
          case LONG:
            value = inGTS.longValues[i];
            break;
          case STRING:
            value = inGTS.stringValues[i];
            break;
          default:
            throw new WarpScriptException("Invalid return type.");
        }
        
        GTSHelper.setValue(gts, inGTS.ticks[i], location, elevation, value, false);
      }
      
      return gts;
    } else if (o instanceof List) {
      List<Object> newlist = new ArrayList<Object>(((List) o).size());
      for (Object in: (List) o) {
        if (!(in instanceof List) && !(in instanceof Map)) {
          Object out = fromSDKObject(in);
          newlist.add(out);
        } else {
          throw new WarpScriptException("Invalid nested complex type.");
        }
      }
      return newlist;
    } else if (o instanceof Map) {
      Map<Object,Object> newmap = new HashMap<Object, Object>(((Map) o).size());
      for (Entry<Object,Object> entry: ((Map<Object,Object>) o).entrySet()) {
        Object key = entry.getKey();
        Object value = entry.getValue();
        if (!(key instanceof List) && !(key instanceof Map) && !(value instanceof List) && !(value instanceof Map)) {
          newmap.put(fromSDKObject(key), fromSDKObject(value));
        } else {
          throw new WarpScriptException("Invalid nested complex type.");
        }
      }
      return newmap;
    } else {
      return o.toString();
    }
  }
  
  public static String toString(Object o) {
    StringBuilder sb = new StringBuilder();
    
    if (null == o) {
      sb.append("NULL");
    } else if (o instanceof Number) {
      sb.append(o);
    } else if (o instanceof String) {
      sb.append("'");
      try {
        sb.append(WarpURLEncoder.encode(o.toString(), "UTF-8"));
      } catch (UnsupportedEncodingException uee) {        
      }
      sb.append("'");
    } else if (o instanceof Boolean) {
      sb.append(Boolean.toString((boolean) o));
    } else if (o instanceof WarpScriptStackFunction) {
      sb.append(o.toString());
    } else if (o instanceof Snapshotable) {
      ((Snapshotable) o).snapshot();
    } else if (o instanceof Macro) {
      sb.append(o.toString());
    } else if (o instanceof NamedWarpScriptFunction){
      sb.append(o.toString());
    }
    return sb.toString();
  }
}
