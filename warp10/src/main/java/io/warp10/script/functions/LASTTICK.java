//
//   Copyright 2019-2023  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Push on the stack the last tick of the GTS on top of the stack.
 * If the GTS does not have values, Long.MIN_VALUE is pushed.
 */
public class LASTTICK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public LASTTICK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (top instanceof GeoTimeSerie) {
      stack.push(GTSHelper.lasttick((GeoTimeSerie) top));      
    } else if (top instanceof List) {
      long last = Long.MIN_VALUE;
      for(Object o: (List) top) {
        if (!(o instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " expects a Geo Time Series or a list thereof on top of the stack.");          
        }
        long lt = GTSHelper.lasttick((GeoTimeSerie) o);
        if (lt > last) {
          last = lt;
        }
      }
      stack.push(last);      
    } else {
      throw new WarpScriptException(getName() + " expects a Geo Time Series or a list thereof on top of the stack.");
    }
    
    return stack;
  }
}
