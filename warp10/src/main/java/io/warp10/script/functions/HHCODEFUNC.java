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

import com.geoxp.geo.HHCodeHelper;
import com.google.common.primitives.Longs;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.Arrays;

/**
 * Template function to interface with HHCodeHelper
 */
public class HHCODEFUNC extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public enum HHCodeAction {
    NORTH, SOUTH, EAST, WEST, NORTH_EAST, NORTH_WEST, SOUTH_EAST, SOUTH_WEST, BBOX, CENTER
  }

  private final HHCodeAction action;

  public HHCODEFUNC(String name, HHCodeAction action) {
    super(name);
    this.action = action;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    int resOverride = -1;

    if (top instanceof Number) {
      resOverride = ((Number) top).intValue();

      if (resOverride < 0 || resOverride > 32) {
        throw new WarpScriptException(getName() + " expects a resolution which is an long between 0 and 32, inclusive.");
      }

      top = stack.pop();
    }

    long[] hhAndRes;
    try {
      hhAndRes = hhAndRes(top);
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " was given unexpected arguments.", wse);
    }
    long hh = hhAndRes[0];
    int res = (int) hhAndRes[1]; // We know hhAndRes returns an int here.

    if (resOverride >= 0) {
      res = resOverride;
    }

    switch (this.action) {
      case NORTH:
        stack.push(manageFormat(HHCodeHelper.northHHCode(hh, res), res, top));
        break;
      case SOUTH:
        stack.push(manageFormat(HHCodeHelper.southHHCode(hh, res), res, top));
        break;
      case EAST:
        stack.push(manageFormat(HHCodeHelper.eastHHCode(hh, res), res, top));
        break;
      case WEST:
        stack.push(manageFormat(HHCodeHelper.westHHCode(hh, res), res, top));
        break;
      case NORTH_EAST:
        stack.push(manageFormat(HHCodeHelper.northEastHHCode(hh, res), res, top));
        break;
      case NORTH_WEST:
        stack.push(manageFormat(HHCodeHelper.northWestHHCode(hh, res), res, top));
        break;
      case SOUTH_EAST:
        stack.push(manageFormat(HHCodeHelper.southEastHHCode(hh, res), res, top));
        break;
      case SOUTH_WEST:
        stack.push(manageFormat(HHCodeHelper.southWestHHCode(hh, res), res, top));
        break;
      case BBOX:
        double[] bbox = HHCodeHelper.getHHCodeBBox(hh, res);
        stack.push(bbox[0]);
        stack.push(bbox[1]);
        stack.push(bbox[2]);
        stack.push(bbox[3]);
        break;
      case CENTER:
        double[] latlon = HHCodeHelper.getCenterLatLon(hh, res);
        stack.push(latlon[0]);
        stack.push(latlon[1]);
        break;
      default:
        throw new WarpScriptException("Unknown HHCODE action.");
    }

    return stack;
  }

  private static Object manageFormat(long hh, int res, Object input) {
    Object o;
    if (input instanceof byte[]) {
      o = Longs.toByteArray(hh);
    } else if (input instanceof String) {
      o = HHCodeHelper.toString(hh, res);
    } else {
      o = hh;
    }
    return o;
  }

  /**
   * Convert a HHCode representation to a long hhcode and an int resolution. The resolution is even and between 2 and 32, inclusive.
   * @param hhcode The HHCode representation, which may be a String, a byte[] or a Long.
   * @return an array of two long, the first being the HHCode and the second being the resolution, which can be safely casted to an int.
   * @throws WarpScriptException if the given Object is not a Long, a byte[] of length <= 8 or a String or length <= 16.
   */
  public static long[] hhAndRes(Object hhcode) throws WarpScriptException {
    long hh;
    int res;

    if (hhcode instanceof Long) {
      hh = (long) hhcode;
      // We don't know the resolution of a HHCode when represented as a long, assume it's full res.
      res = 32;
    } else if (hhcode instanceof String) {
      String hhstr = hhcode.toString();
      // Each character represents 4 bits, so 2 bits per lat or lon.
      res = hhstr.length() * 2;

      // Make sure the hex string is 16 character-long (64 bits), right-pad with zeros if shorter, throw if longer.
      if (hhstr.length() > 16) {
        throw new WarpScriptException("Hexadecimal HHCode string of must be of length <= 16");
      } else if (hhstr.length() < 16) {
        hhcode = new StringBuilder(hhstr).append("0000000000000000");
        ((StringBuilder) hhcode).setLength(16);
      }

      hh = Long.parseUnsignedLong(hhcode.toString(), 16);
    } else if (hhcode instanceof byte[]) {
      byte[] hhbytes = (byte[]) hhcode;
      // Each byte represents 8 bits, so 4 bits par lat or lon.
      res = hhbytes.length * 4;

      // Make sure the byte array is 8 bytes (64 bits), right-pad with zeros if shorter, throw if longer.
      if (hhbytes.length > 8) {
        throw new WarpScriptException("Byte array HHCode must be of length <= 8");
      } else if (hhbytes.length < 8) {
        hhbytes = Arrays.copyOf(hhbytes, 8);
      }

      hh = Longs.fromByteArray(hhbytes);
    } else {
      throw new WarpScriptException("HHCode must be represented as a long, a string or a byte array.");
    }

    return new long[] {hh, res};
  }
}
