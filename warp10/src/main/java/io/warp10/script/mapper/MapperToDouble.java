//
//   Copyright 2018-2022  SenX S.A.S.
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
import io.warp10.script.WarpScriptException;

import java.util.Map;

/**
 * Mapper which returns the absolute value of the value passed as parameter
 */
public class MapperToDouble extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

  public MapperToDouble(String name) {
    super(name);
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
          value = Double.valueOf((String) values[0]);
        } catch (NumberFormatException nfe) {
          value = null;
        }
      }
    }
    
    return new Object[] { tick, location, elevation, value };
  }
}
