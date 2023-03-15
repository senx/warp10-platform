//
//   Copyright 2018-2023  SenX S.A.S.
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

package io.warp10.script.mapper;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Mapper which returns the probability of a value within a gaussian distribution with given mu/sigma
 */
public class MapperNPDF extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

  private final double mu;
  private final double sigma;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object value = stack.pop();

      if (!(value instanceof Number)) {
        throw new WarpScriptException(getName() + " expects a standard deviation (sigma) on top of the stack.");
      }

      double sigma = ((Number) value).doubleValue();

      if (sigma <= 0) {
        throw new WarpScriptException(getName() + " expects a positive standard deviation (sigma) on top of the stack.");
      }
      
      value = stack.pop();

      if (!(value instanceof Number)) {
        throw new WarpScriptException(getName() + " expects a mean (mu) below the standard deviation.");
      }

      double mu = ((Number) value).doubleValue();
      
      stack.push(new MapperNPDF(getName(), mu, sigma));
      
      return stack;
    }
  }

  public MapperNPDF(String name, double mu, double sigma) {
    super(name);
    this.mu = mu;
    this.sigma = sigma;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (0 == values.length) {
      return new Object[] {0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
    }
    
    if (1 != values.length) {
      throw new WarpScriptException(getName() + " can only be applied to a single value.");
    }
    
    double value;
    long location = locations[0];
    long elevation = elevations[0];
        
    if (values[0] instanceof Long) {
      value = (long) values[0];
    } else if (values[0] instanceof Double) {
      value = (double) values[0];
    } else {
      throw new WarpScriptException(getName() + " can only be applied to LONG or DOUBLE values.");
    }
    
    double p = (1.0D / (this.sigma * Math.sqrt(2.0D * Math.PI))) * Math.exp(-1.0D * (value - this.mu) * (value - this.mu) / (2.0D * this.sigma * this.sigma));
    
    return new Object[] { tick, location, elevation, p };
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(mu));
    sb.append(" ");
    sb.append(StackUtils.toString(sigma));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }

}
