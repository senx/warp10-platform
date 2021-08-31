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
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptException;

import java.util.Map;

/**
 * Mapper which clears the position at 'tick'
 */
public class MapperGeoClearPosition extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

  public MapperGeoClearPosition(String name) {
    super(name);
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    Object[] values = (Object[]) args[6];

    if (1 != values.length) {
      throw new WarpScriptException(getName() + " can only be applied to a single value.");
    }
    
    return new Object[] { tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, values[0] };
  }
}
