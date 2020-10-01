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
import com.geoxp.geo.HHCodeHelper;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.List;
import java.util.TreeSet;

/**
 * Converts a list of geocells (longs) or HHCode prefixes (strings or bytes) to a GeoXPShape.
 */
public class TOGEOSHAPE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOGEOSHAPE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a " + TYPEOF.TYPE_LIST + " of " + TYPEOF.TYPE_LONG + "," + TYPEOF.TYPE_STRING + " or " + TYPEOF.TYPE_BYTES + " representing geocells.");
    }

    List cellList = (List) top;
    // Use a TreeSet to make sure geocells are sorted and without duplicate.
    TreeSet<Long> geocells = new TreeSet<Long>();

    long[] hhAndRes;
    for (Object cell: cellList) {
      long geocell;

      if (cell instanceof Long) {
        // Explicit GeoCell: 4 bits resolution + 60 bits HHCode.
        geocell = ((Long) cell).longValue();
      } else if (cell instanceof String || cell instanceof byte[]) {
        // Implicit GeoCell: resolution is defined by the length of the String or byte[]
        try {
          hhAndRes = HHCODEFUNC.hhAndRes(cell);
        } catch (WarpScriptException wse) {
          throw new WarpScriptException(getName() + " expects the given list to contain valid STRING or BYTES HHCodes.", wse);
        }
        long hh = hhAndRes[0];
        int res = (int) hhAndRes[1]; // We know hhAndRes returns an int here.

        geocell = HHCodeHelper.toGeoCell(hh, res);
      } else {
        throw new WarpScriptException(getName() + " expects a " + TYPEOF.TYPE_LIST + " of " + TYPEOF.TYPE_LONG + "," + TYPEOF.TYPE_STRING + " or " + TYPEOF.TYPE_BYTES + " representing geocells.");
      }

      // Check cell is valid
      if (0 == (geocell >>> 60)) {
        throw new WarpScriptException(getName() + " expects geocells resolutions to be even and between 2 and 30, inclusive.");
      }

      geocells.add(geocell);
    }

    stack.push(GeoXPLib.fromCells(geocells));

    return stack;
  }
}
