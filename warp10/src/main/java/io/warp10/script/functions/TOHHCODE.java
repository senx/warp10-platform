//
//   Copyright 2018-2020  SenX S.A.S.
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
import com.geoxp.geo.GeoHashHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;

/**
 * Convert a lat/lon pair or a GeoHash to an HHCode or GeoXPShape to a list of HHCodes.
 */
public class TOHHCODE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean tostring;
  private final boolean useGtsConvention;

  public TOHHCODE(String name, boolean tostring) {
    this(name, tostring, false);
  }

  public TOHHCODE(String name, boolean tostring, boolean useGtsConvention) {
    super(name);
    this.tostring = tostring;
    this.useGtsConvention = useGtsConvention;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (top instanceof GeoXPLib.GeoXPShape) {
      // GeoXPShape -> HHCodes
      GeoXPLib.GeoXPShape shape = (GeoXPLib.GeoXPShape) top;

      ArrayList<Object> hhcodes = new ArrayList<Object>();

      long[] geocells = GeoXPLib.getCells(shape);

      for (long geocell: geocells) {
        if (this.tostring) {
          String hhcode = Long.toHexString(geocell).substring(1, (int) ((geocell >>> 60) + 1));
          hhcodes.add(hhcode);
        } else {
          hhcodes.add(geocell);
        }
      }

      stack.push(hhcodes);
    } else if (top instanceof String) {
      // GeoHash -> HHCode
      long geoxppoint = GeoHashHelper.toHHCode((String) top);

      if (this.tostring) {
        String hhcode = Long.toHexString(geoxppoint);

        stack.push(hhcode);
      } else {
        stack.push(geoxppoint);
      }
    } else if (top instanceof Number) {
      // Lat/lon -> HHCode
      double lonDouble = ((Number) top).doubleValue();

      top = stack.pop();

      if (!(top instanceof Number)) {
        throw new WarpScriptException(getName() + " expects a latitude and a longitude of type DOUBLE.");
      }

      double latDouble = ((Number) top).doubleValue();

      long geoxppoint;

      if (useGtsConvention) {
        if (Double.isNaN(latDouble) != Double.isNaN(lonDouble)) {
          throw new WarpScriptException(getName() + " expects latitude and longitude to both be NaN or both be not NaN");
        } else if (!Double.isNaN(latDouble)) { // also !Double.isNaN(lonDouble)
          geoxppoint = GeoXPLib.toGeoXPPoint(latDouble, lonDouble);
        } else {
          geoxppoint = GeoTimeSerie.NO_LOCATION;
        }
      } else {
        geoxppoint = GeoXPLib.toGeoXPPoint(latDouble, lonDouble);
      }

      if (this.tostring) {
        String hhcode = Long.toHexString(geoxppoint);

        stack.push(hhcode);
      } else {
        stack.push(geoxppoint);
      }
    } else {
      throw new WarpScriptException(getName() + " expects numeric lat/lon, a STRING GeoHash or a GEOSHAPE shape.");
    }
    return stack;
  }
}
