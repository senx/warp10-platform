//
//    Copyright 2020  SenX S.A.S.
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
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TOWKB extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private TOGEOJSON togeojson;

  public TOWKB(String name) {
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
      WKBWriter writer = new WKBWriter();
      byte[] wkb = writer.write(geometry);
      stack.push(wkb);
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " expects a GEOSHAPE, a WKT STRING or a GeoJSON STRING.", wse);
    } catch (
        ParseException pe) {
      throw new WarpScriptException(getName() + " was given invalid input.", pe);
    }

    return stack;
  }
}
