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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;

/**
 * Computes the union of two GeoXPShape
 */
public class GeoUnion extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public GeoUnion(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object a = stack.pop();
    Object b = stack.pop();
    
    if (!(a instanceof GeoXPShape) || !(b instanceof GeoXPShape)) {
      throw new WarpScriptException(getName() + " expects two GeoShape instances as the top 2 elements of the stack.");
    }

    //
    // Compute union
    //
    
    stack.push(GeoXPLib.union((GeoXPShape) a, (GeoXPShape) b));
    
    return stack;
  }
}
