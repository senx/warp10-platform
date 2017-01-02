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

import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.Map;

/**
 * Mapper which returns the double value of the value passed as parameter
 */
public class MapperToDouble extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

  
  private final NumberFormat format;
  
  public MapperToDouble(String name) {
    super(name);
    this.format = null;
  }
  
  public MapperToDouble(String name, Object language) throws WarpScriptException {   
    super(name);
    
    if (language instanceof String) {
      Locale locale = Locale.forLanguageTag((String) language);
      format = NumberFormat.getInstance(locale);
    } else {
      throw new WarpScriptException("Invalid value type for " + getName() + ", expects a String");
    }
  }
  
  /**
   * Builder for case user specify a language tag according to an
   * IETF BCP 47 language tag string of the Locale Java class
   */
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object value = stack.pop();
      
      if (!(value instanceof String)) {
        throw new WarpScriptException("Invalid parameter for " + getName());
      }
      
      stack.push(new MapperToDouble(getName(), value));
      return stack;
    }
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    String[] names = (String[]) args[1];
    Map<String,String>[] labels = (Map<String,String>[]) args[2];
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (1 != values.length) {
      throw new WarpScriptException(getName() + " can only be applied to a single value.");
    }
    
    Object value = null;
    long location = locations[0];
    long elevation = elevations[0];
    
    if (null != values[0]) {
      if (values[0] instanceof Long) {
        value = ((Number) values[0]).doubleValue();
      } else if (values[0] instanceof Double) {
        value = values[0];
      } else if (values[0] instanceof Boolean) {
        value = Boolean.TRUE.equals(values[0]) ? 1.0D : 0.0D;
      } else if (values[0] instanceof String) {
        try {
          if (null == this.format) {
            value = Double.parseDouble((String) values[0]);
          } else
          {           
            value = format.parse((String) values[0]).doubleValue();
          }
        } catch (NumberFormatException | ParseException e) {
          value = null;
        }
      }
    }
    
    return new Object[] { tick, location, elevation, value };
  }
}
