//
//   Copyright 2016  Cityzen Data
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

import com.geoxp.GeoXPLib;
import com.geoxp.geo.GeoHashHelper;

/**
 * Convert a GeoHash to lat/lon
 */
public class GEOHASHTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public GEOHASHTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    String geohash = stack.pop().toString();
    
    long geoxppoint = GeoHashHelper.toHHCode(geohash);
    
    double[] latlon = GeoXPLib.fromGeoXPPoint(geoxppoint);
    
    stack.push(latlon[0]);
    stack.push(latlon[1]);

    return stack;
  }
}
