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

/**
 * Template function to interface with HHCodeHelper
 */
public class HHCODEFUNC extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public enum HHCodeAction {
    NORTH, SOUTH, EAST, WEST, NORTH_EAST, NORTH_WEST, SOUTH_EAST, SOUTH_WEST, BBOX, CENTER
  }

  private HHCodeAction action;

  public HHCODEFUNC(String name, HHCodeAction action) {
    super(name);
    this.action = action;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object o = stack.pop();

    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects resolution (even number between 2 and 32).");
    }

    int res = ((Number) o).intValue();

    if (0 != res && (res < 2 || res > 32 || (0 != (res & 1)))) {
      throw new WarpScriptException(getName() + " expects a maximum resolution which is an even number between 2 and 32 or 0.");
    }


    Object hhcode = stack.pop();

    long hh;

    if (hhcode instanceof Long) {
      hh = (long) hhcode;
    } else if (hhcode instanceof String) {
      String hhstr = hhcode.toString();
      if (hhstr.length() > 16) {
        throw new WarpScriptException(getName() + " expects an hexadecimal HHCode string of length <= 16");
      } else if (hhstr.length() < 16) {
        hhcode = (hhstr + "0000000000000000").substring(0, 16);
      }
      hh = Long.parseUnsignedLong(hhcode.toString(), 16);
    } else if (hhcode instanceof byte[]) {
      hh = Longs.fromByteArray((byte[]) hhcode);
    } else {
      throw new WarpScriptException(getName() + " expects a long, a string or a byte array.");
    }

    switch (this.action) {
      case NORTH:
        stack.push(this.manageFormat(HHCodeHelper.northHHCode(hh, res), res, hhcode));
        break;
      case SOUTH:
        stack.push(this.manageFormat(HHCodeHelper.southHHCode(hh, res), res, hhcode));
        break;
      case EAST:
        stack.push(this.manageFormat(HHCodeHelper.eastHHCode(hh, res), res, hhcode));
        break;
      case WEST:
        stack.push(this.manageFormat(HHCodeHelper.westHHCode(hh, res), res, hhcode));
        break;
      case NORTH_EAST:
        stack.push(this.manageFormat(HHCodeHelper.northEastHHCode(hh, res), res, hhcode));
        break;
      case NORTH_WEST:
        stack.push(this.manageFormat(HHCodeHelper.northWestHHCode(hh, res), res, hhcode));
        break;
      case SOUTH_EAST:
        stack.push(this.manageFormat(HHCodeHelper.southEastHHCode(hh, res), res, hhcode));
        break;
      case SOUTH_WEST:
        stack.push(this.manageFormat(HHCodeHelper.southWestHHCode(hh, res), res, hhcode));
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
        throw new WarpScriptException("Unknown HHCODE action");
    }

    return stack;
  }

  private Object manageFormat(long hh, int res, Object input) {
    Object o;
    if (input instanceof byte[]) {
      o = Longs.toByteArray(hh);
    } else if (input instanceof String) {
      o = HHCodeHelper.toString(((Long) hh).longValue(), res);
    } else {
      o = ((Long) hh).longValue();
    }
    return o;
  }
}
