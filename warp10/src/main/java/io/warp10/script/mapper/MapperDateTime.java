//
//   Copyright 2022  SenX S.A.S.
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
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public abstract class MapperDateTime extends NamedWarpScriptFunction implements WarpScriptMapperFunction {

  private final DateTimeZone dtz;

  public MapperDateTime(String name, Object timezone) throws WarpScriptException {
    super(name);
    if (timezone instanceof String) {
      this.dtz = DateTimeZone.forID(timezone.toString());
    } else if (timezone instanceof Number) {
      this.dtz = DateTimeZone.forOffsetMillis(((Number) timezone).intValue());
    } else {
      throw new WarpScriptException(getName() + " expects a STRING timezone or a NUMBER millisecond offset.");
    }
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

    long location = locations[0];
    long elevation = elevations[0];

    DateTime dt = new DateTime(tick / Constants.TIME_UNITS_PER_MS, this.dtz);

    return new Object[] {tick, location, elevation, getDateTimeInfo(dt, tick)};
  }

  public abstract Object getDateTimeInfo(DateTime dt, long tick);

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.dtz.getID()));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
