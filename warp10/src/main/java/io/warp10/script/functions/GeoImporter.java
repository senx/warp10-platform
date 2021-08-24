//
//   Copyright 2021  SenX S.A.S.
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

import com.geoxp.GeoXPLib;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.buffer.BufferOp;
import com.vividsolutions.jts.operation.buffer.BufferParameters;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.Map;

public class GeoImporter extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  protected Class inputClass;
  protected String inputType;

  private final boolean uniform;

  public GeoImporter(String name, boolean uniform, Class inClass, String inType) {
    super(name);
    this.uniform = uniform;
    this.inputClass = inClass;
    this.inputType = inType;
  }

  protected Geometry convert(Object input) throws Exception {
    return null;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object inside = stack.pop();
    Object pcterror = stack.pop();
    Object input = stack.pop();

    if (!(inputClass.isInstance(input)) || !(inside instanceof Boolean) || (!(pcterror instanceof Double) && !(pcterror instanceof Long))) {
      throw new WarpScriptException(getName() + " expects " + inputType + ", a DOUBLE error percentage or a LONG resolution (even number between 2 and 30) and a BOOLEAN.");
    }

    // Check the resolution is even and in 2..30, if relevant
    if (pcterror instanceof Long) {
      long res = ((Number) pcterror).longValue();
      if (1 == (res % 2) || res > 30 || res < 2) {
        throw new WarpScriptException(getName() + " expects the resolution to be an even number between 2 and 30");
      }
    }

    //
    // Read Input
    //

    Geometry geometry;

    try {
      geometry = convert(input);
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " expects valid " + inputType + ".", e);
    }

    //
    // Apply buffer if defined
    //
    Map<Object,Object> buffer = (Map<Object,Object>) stack.getAttribute(GEOBUFFER.ATTR_GEOBUFFER);

    if (null != buffer) {
      // Clear the buffer
      stack.setAttribute(GEOBUFFER.ATTR_GEOBUFFER, null);
      // Apply the buffer operation
      BufferOp bop = new BufferOp(geometry, (BufferParameters) buffer.get(GEOBUFFER.KEY_PARAMS));
      geometry = bop.getResultGeometry(((Double) buffer.get(GEOBUFFER.KEY_DIST)).doubleValue());
    }

    //
    // Convert Geometry to a GeoXPShape
    //

    int maxcells = ((Number) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MAX_GEOCELLS)).intValue();
    Object shape;

    if (!this.uniform) {
      if (pcterror instanceof Double) {
        shape = GeoXPLib.toGeoXPShape(geometry, ((Number) pcterror).doubleValue(), Boolean.TRUE.equals(inside), maxcells);
      } else {
        shape = GeoXPLib.toGeoXPShape(geometry, ((Number) pcterror).intValue(), Boolean.TRUE.equals(inside), maxcells);
      }
    } else {
      if (pcterror instanceof Double) {
        shape = GeoXPLib.toUniformGeoXPShape(geometry, ((Number) pcterror).doubleValue(), Boolean.TRUE.equals(inside), maxcells);
      } else {
        shape = GeoXPLib.toUniformGeoXPShape(geometry, ((Number) pcterror).intValue(), Boolean.TRUE.equals(inside), maxcells);
      }
    }

    if (null == shape) {
      throw new WarpScriptException(getName() + " reached the maximum number of cells in a geographic shape (warpscript.maxgeocells=" + maxcells + ").");
    }

    stack.push(shape);
    return stack;
  }
}
