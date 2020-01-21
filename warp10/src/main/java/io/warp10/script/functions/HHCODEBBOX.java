//
//   Copyright 2020 Apache License, Version 2.0 (the "License");
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
 * Get the bounding box of the given hhcode at the given resolution.
 * <p>
 * The bounding box is an array of doubles representing the lat/lon of ll(sw)/ur(ne) corners.
 */
public class HHCODEBBOX extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public HHCODEBBOX(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object o = stack.pop();

    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects resolution (even number between 2 and 30) and a boolean as the top 3 elements of the stack.");
    }

    int res = ((Number) o).intValue();

    if (0 != res && (res < 2 || res > 30 || (0 != (res & 1)))) {
      throw new WarpScriptException(getName() + " expects a maximum resolution which is an even number between 2 and 30 or 0.");
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
        hhcode = new StringBuilder(hhstr).append("0000000000000000");
        ((StringBuilder) hhcode).setLength(16);
      }
      hh = Long.parseUnsignedLong(hhcode.toString(), 16);
    } else if (hhcode instanceof byte[]) {
      hh = Longs.fromByteArray((byte[]) hhcode);
    } else {
      throw new WarpScriptException(getName() + " expects a long, a string or a byte array.");
    }

    double[] bbox = HHCodeHelper.getHHCodeBBox(hh, res);
    stack.push(bbox[0]);
    stack.push(bbox[1]);
    stack.push(bbox[2]);
    stack.push(bbox[3]);

    return stack;
  }
}
