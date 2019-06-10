//
//   Copyright 2018  SenX S.A.S.
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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.math.BigInteger;

import com.geoxp.GeoXPLib;
import com.google.common.primitives.Longs;

/**
 * Convert a GeoHash to lat/lon
 */
public class HHCODETO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public HHCODETO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object hhcode = stack.pop();
    
    double[] latlon = null;
    
    if (hhcode instanceof Long) {
      latlon = GeoXPLib.fromGeoXPPoint((long) hhcode);
    } else if (hhcode instanceof String) {
      String hhstr = hhcode.toString();
      if (hhstr.length() > 16) {
        throw new WarpScriptException(getName() + " expects an hexadecimal HHCode string of length <= 16");
      } else if (hhstr.length() < 16) {
        hhcode = new StringBuilder(hhstr).append("0000000000000000");
        ((StringBuilder) hhcode).setLength(16);
      }
      long hh = new BigInteger(hhcode.toString(),16).longValue();
      latlon = GeoXPLib.fromGeoXPPoint(hh);
    } else if (hhcode instanceof byte[]) {
      long hh = Longs.fromByteArray((byte[]) hhcode); 
      latlon = GeoXPLib.fromGeoXPPoint(hh);
    } else {
      throw new WarpScriptException(getName() + " expects a long, a string or a byte array.");
    }
    
    stack.push(latlon[0]);
    stack.push(latlon[1]);

    return stack;
  }
}
