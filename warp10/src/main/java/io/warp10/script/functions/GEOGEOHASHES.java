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

import java.util.List;

import com.geoxp.GeoXPLib;
import com.geoxp.geo.Coverage;
import com.geoxp.geo.GeoHashHelper;
import com.geoxp.geo.HHCodeHelper;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class GEOGEOHASHES extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public GEOGEOHASHES(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of Geohashes.");
    }

    List<Object> geohashes = (List<Object>) top;
    
    Coverage c = new Coverage();
    
    for (Object geohash: geohashes) {
      
      String hash = String.valueOf(geohash).toLowerCase();
      long hhcode = GeoHashHelper.toHHCode(hash);
      int nbits = 5 * Math.min(12, hash.length());
      
      int target = nbits;
      
      switch (nbits) {
        case 5:
          target = 8;
          break;
        case 10:
          target = 12;
          break;
        case 15:
          target = 16;
          break;
        case 25:
          target = 28;
          break;
        case 30:
          target = 32;
          break;
        case 35:
          target = 36;
          break;
        case 45:
          target = 48;
          break;
        case 50:
          target = 52;
          break;
        case 55:
          target = 56;
          break;
      }
      
      int deltabits = target - nbits;
      int resolution = target >>> 1;
      
      if (deltabits > 0) {
        hhcode = (hhcode >>> (64 - nbits)) << deltabits;
        for (long delta = 0; delta < 1L << deltabits; delta++) {
          c.addCell(resolution, (hhcode | delta) << (64 - target));
        }
      } else {
        c.addCell(resolution, hhcode);        
      }      
    }
    
    c.dedup();
    c.optimize(0L);

    long[] geocells = c.toGeoCells(HHCodeHelper.MAX_RESOLUTION);
    
    stack.push(GeoXPLib.fromCells(geocells, false));
    
    return stack;
  }
}
