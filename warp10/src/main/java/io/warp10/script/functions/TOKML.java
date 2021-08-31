//
//    Copyright 2021  SenX S.A.S.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package io.warp10.script.functions;

import com.geoxp.GeoXPLib;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.io.gml2.GMLConstants;
import com.vividsolutions.jts.io.gml2.GMLWriter;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.IOException;
import java.io.Writer;

public class TOKML extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final TOGEOJSON togeojson;

  protected static class KMLWriter extends GMLWriter {

    public KMLWriter() {
      setPrefix(null);
    }

    @Override
    public void write(Geometry geometry, Writer writer) throws IOException {
      if (geometry instanceof MultiPoint || geometry instanceof MultiLineString || geometry instanceof MultiPolygon) {
        // In KML those do no exists and are represented using a GeometryCollection
        writer.write("<" + GMLConstants.GML_MULTI_GEOMETRY + ">");

        for (int t = 0; t < geometry.getNumGeometries(); t++) {
          write(geometry.getGeometryN(t), writer);
        }
        writer.write("</" + GMLConstants.GML_MULTI_GEOMETRY + ">");
      } else {
        super.write(geometry, writer);
      }
    }
  }

  public TOKML(String name) {
    super(name);
    togeojson = new TOGEOJSON(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    // The top of the stack is either a GeoXPShape or a Boolean, then we apply GEOJSON to convert it to
    // GeoJSON before converting it to WKB. This is a quick and easy way of converting GeoXPShape to WKB.
    Object peeked = stack.peek();

    if (peeked instanceof GeoXPLib.GeoXPShape || peeked instanceof Boolean) {
      togeojson.apply(stack);
    }

    Object geomObject = stack.pop();

    try {
      Geometry geometry = TOGEOJSON.toGeometry(geomObject);
      stack.push(GeometryToKML(geometry));
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " expects a GEOSHAPE, a WKT STRING, a GeoJSON STRING, a KML STRING or a GeoJSON STRING.", wse);
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " was given invalid input.", e);
    }

    return stack;
  }

  public static String GeometryToKML(Geometry geometry) {
    KMLWriter writer = new KMLWriter();
    String kml = writer.write(geometry);
    // Remove formatting
    return kml.replaceAll("\\n\\s*", "");
  }
}
