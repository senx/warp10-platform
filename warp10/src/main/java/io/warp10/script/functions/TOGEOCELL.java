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
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Converts a String or byte[] HHCode prefix to a Long geocell.
 * The resolution of the HHCode String prefix is implicitly given by the length of the String.
 */
public class TOGEOCELL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOGEOCELL(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String) && !(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " expects a " + TYPEOF.TYPE_STRING + " or a " + TYPEOF.TYPE_BYTES + ".");
    }

    long[] hhAndRes;
    try {
      hhAndRes = HHCODEFUNC.hhAndRes(top);
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " expects a " + TYPEOF.TYPE_STRING + " or a " + TYPEOF.TYPE_BYTES + ".", wse);
    }
    long hh = hhAndRes[0];
    int res = (int) hhAndRes[1]; // We know hhAndRes returns an int here.

    long geocell = HHCodeHelper.toGeoCell(hh, res);

    // Check cell is valid
    if (0 == (geocell >>> 60)) {
      throw new WarpScriptException(getName() + " expects the geocell resolution to be even and between 2 and 30, inclusive.");
    }

    stack.push(geocell);

    return stack;
  }
}
