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

package io.warp10.script.mapper;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;

/**
 * Wrap a Mapper so we return no value when the number of values in the
 * window is outside a given range
 * 
 */
public class STRICTMAPPER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public STRICTMAPPER(String name) {
    super(name);
  }

  
  private static final class StringentMapper extends NamedWarpScriptFunction implements WarpScriptMapperFunction {
    
    private final int min;
    private final int max;
    private final WarpScriptMapperFunction mapper;
    
    public StringentMapper(String name, int min, int max, WarpScriptMapperFunction mapper) {
      super(name);
      this.min = min;
      this.max = max;
      this.mapper = mapper;
    }
    
    @Override
    public Object apply(Object[] args) throws WarpScriptException {
      
      Object[] values = (Object[]) args[6];
      
      if (values.length < min || values.length > max) {
        return new Object[] { args[0], GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
      }
      
      return mapper.apply(args);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(mapper.toString());
      sb.append(" ");
      sb.append(min);
      sb.append(" ");
      sb.append(max);
      sb.append(" ");
      sb.append(getName());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      return sb.toString();
    }
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop(); // maxpoints
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a maximum (inclusive) number of values on top of the stack.");
    }


    int max = (int) Math.min(Math.max(0L, ((Number) o).longValue()), Integer.MAX_VALUE); // Clamp to positive int
    
    o = stack.pop(); // minpoints
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a minimum (inclusive) number of values below the top of the stack.");
    }

    int min = (int) Math.min(Math.max(0L, ((Number) o).longValue()), Integer.MAX_VALUE); // Clamp to positive int

    if ( min > max ){
      throw new WarpScriptException(getName() + " expects min <= max.");
    }
    
    o = stack.pop(); // mapper
    
    if (!(o instanceof WarpScriptMapperFunction)) {
      throw new WarpScriptException(getName() + " expects a mapper below the extrema defining the value count range.");
    }
    
    WarpScriptMapperFunction mapper = (WarpScriptMapperFunction) o;
    
    stack.push(new StringentMapper(getName(), min, max, mapper));
    
    return stack;
  }
}
