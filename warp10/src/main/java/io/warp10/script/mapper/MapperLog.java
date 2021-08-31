//
//   Copyright 2018-2020  SenX S.A.S.
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
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.Map;

/**
 * Extract the log of the value passed as parameter in a constant base
 */
public class MapperLog extends NamedWarpScriptFunction implements WarpScriptMapperFunction {
  
  private double dvalue;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object value = stack.pop();
      
      if (!(value instanceof Long) && !(value instanceof Double)) {
        throw new WarpScriptException("Invalid parameter for " + getName());
      }
      
      stack.push(new MapperLog(getName(), value));
      return stack;
    }
  }
  
  public MapperLog(String name, Object value) throws WarpScriptException {
    super(name);
    
    if (value instanceof Long) {
      this.dvalue = Math.log((long) value);
    } else if (value instanceof Double) {
      this.dvalue = Math.log((double) value);
    } else {
      throw new WarpScriptException("Invalid value type for " + getName());
    }
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (0 == values.length) {
      return new Object[] { 0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
    
    if (1 != values.length) {
      throw new WarpScriptException(getName() + " can only be applied to a single value.");
    }
    
    Object value = null;
    long location = locations[0];
    long elevation = elevations[0];
        
    if (values[0] instanceof Long) {
      value = Math.log((long) values[0]) / dvalue;
    } else if (values[0] instanceof Double) {
      value = Math.log((double) values[0]) / dvalue;
    } else {
      throw new WarpScriptException(getName() + " can only be applied to LONG or DOUBLE values.");
    }
    
    return new Object[] { tick, location, elevation, value };
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(dvalue));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
