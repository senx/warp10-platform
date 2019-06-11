//
//   Copyright 2019  SenX S.A.S.
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

/**
 * Unitary mapper which returns the negation of a boolean GTS
 */
public class MapperNot extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

  public MapperNot(String name) {
    super(name);
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (values.length > 1) {
      throw new WarpScriptException(getName() + " can only be applied to a single value.");
    }

    Object value = null;
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;

    if (values.length > 0) {
      location = locations[0];
      elevation = elevations[0];
      if (values[0] instanceof Boolean) {
        value = !((Boolean) values[0]);
      } else {
        throw new WarpScriptException(getName() + " can only be applied to BOOLEAN values.");
      }
    }

    return new Object[] { tick, location, elevation, value };
  }
}
