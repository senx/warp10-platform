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

import java.util.ArrayList;
import java.util.List;

import com.geoxp.GeoXPLib;
import com.geoxp.geo.GeoHashHelper;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

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

    List<Object> l = (List<Object>) top;
    
    List<String> geohashes = new ArrayList<String>(l.size());
    
    for (Object geohash: l) {
      geohashes.add(String.valueOf(geohash));
    }

    long[] geocells = GeoHashHelper.toGeoCells(geohashes);
    
    stack.push(GeoXPLib.fromCells(geocells, false));
    
    return stack;
  }
}
