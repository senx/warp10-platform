//
//   Copyright 2017  Cityzen Data
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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Compute Skewness of a numerical GTS
 */
public class SKEWNESS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SKEWNESS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects a boolean on top of the stack to determine if Bessel's correction should be applied or not.");
    }
    
    boolean applyBessel = Boolean.TRUE.equals(o);
    
    o = stack.pop();
    
    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects a Geo Time Series instance below the top of the stack.");
    }
    
    GeoTimeSerie gts = (GeoTimeSerie) o;
    
    int n = GTSHelper.nvalues(gts);
    
    if (0 == n) {
      throw new WarpScriptException(getName() + " can only skewness for non empty series.");
    }
    
    if (TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) {
      throw new WarpScriptException(getName() + " can only compute skewness for numerical series.");
    }
    
    double skewness = GTSHelper.skewness(gts, applyBessel);
    stack.push(skewness);
    
    return stack;
  }
}
