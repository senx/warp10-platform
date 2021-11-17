//
//   Copyright 2018-2021  SenX S.A.S.
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

package io.warp10.script.aggregator;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class StandardDeviation extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptReducerFunction, WarpScriptBucketizerFunction {
  
  private final boolean forbidNulls;
  private final WarpScriptAggregatorFunction variance;

  public StandardDeviation(String name, boolean useBessel, boolean forbidNulls) {
    this(name, useBessel, forbidNulls, false);
  }

  public StandardDeviation(String name, boolean useBessel, boolean forbidNulls, boolean useWelford) {
    super(name);
    this.forbidNulls = forbidNulls;
    if (useWelford) {
      this.variance = new VarianceWelford("", useBessel, forbidNulls);
    } else {
      this.variance = new Variance("", useBessel, forbidNulls);
    }
  }
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    private final boolean forbidNulls;
    private final boolean useWelford;

    public Builder(String name, boolean forbidNulls) {
      this(name, forbidNulls, false);
    }

    public Builder(String name, boolean forbidNulls, boolean useWelford) {
      super(name);
      this.forbidNulls = forbidNulls;
      this.useWelford = useWelford;
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object o = stack.pop();
      
      if (!(o instanceof Boolean)) {
        throw new WarpScriptException(getName() + " expects a boolean parameter to determine whether or not to apply Bessel's correction.");
      }
      
      stack.push(new StandardDeviation(getName(), (boolean) o, forbidNulls, useWelford));
      
      return stack;
    }    
  }
  
  @Override
  public Object apply(Object[] args) throws io.warp10.script.WarpScriptException {
    Object[] var = (Object[]) variance.apply(args); 
    
    if (4 != var.length || !(var[3] instanceof Number)) {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
    
    var[3] = Math.sqrt(((Number) var[3]).doubleValue());
    
    return var;
  }
  
  @Override
  public String toString() {
    return Boolean.toString(this.forbidNulls) + " " + this.getName();
  }
}
