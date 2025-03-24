//
//   Copyright 2023-2025  SenX S.A.S.
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

package io.warp10.script.filler;

import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptSingleValueFillerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.SNAPSHOT;

import java.util.List;

public class FillerValue extends NamedWarpScriptFunction implements WarpScriptFillerFunction, SNAPSHOT.Snapshotable, WarpScriptSingleValueFillerFunction {

  private long latlon;
  private long elev;
  private Object value;

  @Override
  public void fillTick(long tick, GeoTimeSerie filled, Object invalidValue) throws WarpScriptException {
    GTSHelper.setValue(filled, tick, latlon, elev, value, false);
  }

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    public Builder(String name) {
      super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object o = stack.pop();


      long latlon = GeoTimeSerie.NO_LOCATION;
      long elev = GeoTimeSerie.NO_ELEVATION;
      Object value;

      if (o instanceof List) {
        List datapoint = (List) o;
        if (datapoint.size() != 4) {
          throw new WarpScriptException(getName() + " expects a LIST of 4 parameters: latitude longitude elevation value");
        }

        if (!(datapoint.get(0) instanceof Number)) {
          throw new WarpScriptException(getName() + " expects the latitude to be a NUMBER");
        }

        if (!(datapoint.get(1) instanceof Number)) {
          throw new WarpScriptException(getName() + " expects the longitude to be a NUMBER");
        }

        if (!(datapoint.get(2) instanceof Long)) {
          if (!(datapoint.get(2) instanceof Double) || !((Double) datapoint.get(2)).isNaN()) {
            throw new WarpScriptException(getName() + " expects the elevation to be a LONG or NAN");
          }
        }

        double lat = ((Number) datapoint.get(0)).doubleValue();
        double lon = ((Number) datapoint.get(1)).doubleValue();
        if (!Double.isNaN(lat) && !Double.isNaN(lon)) {
          latlon = GeoXPLib.toGeoXPPoint(lat, lon);
        }

        if (!(datapoint.get(2) instanceof Double && ((Double) datapoint.get(2)).isNaN())) {
          elev = (long) datapoint.get(2);
        }

        value = datapoint.get(3);

      } else {

        value = o;
      }

      stack.push(new FillerValue(getName(), latlon, elev, value));
      return stack;
    }
  }

  public FillerValue(String name, long latlon, long elev, Object value) throws WarpScriptException {
    super(name);

    if (!(value instanceof Long) && !(value instanceof Double) && !(value instanceof Boolean) && !(value instanceof String) && !(value instanceof byte[])) {
      throw new WarpScriptException(getName() + " expects the value to be either a LONG, a DOUBLE, a BOOLEAN, a STRING or a BYTEARRAY");
    }

    this.latlon = latlon;
    this.elev = elev;
    this.value = value;
  }
  
  @Override
  public Object[] apply(Object[] args) throws WarpScriptException {
    
    Object[] results = new Object[4];
    Object[] other = (Object[]) args[1];
    long tick = ((Number) other[0]).longValue();

    results[0] = tick;
    results[1] = latlon;
    results[2] = elev;
    results[3] = value;
    
    return results;
  }
    
  @Override
  public int getPostWindow() {
    return 0;
  }
  
  @Override
  public int getPreWindow() {
    return 0;
  }

  @Override
  public String snapshot() {
    double[] tuple = GeoXPLib.fromGeoXPPoint(latlon);
    double lat = tuple[0];
    double lon = tuple[1];

    StringBuilder sb = new StringBuilder();
    try {
      sb.append(WarpScriptLib.LIST_START);
      sb.append(" ");
      SNAPSHOT.addElement(sb, lat);
      SNAPSHOT.addElement(sb, lon);
      SNAPSHOT.addElement(sb, elev);
      SNAPSHOT.addElement(sb, value);
      sb.append(WarpScriptLib.LIST_END);
    } catch (WarpScriptException wse) {
      sb.append(WarpScriptStack.COMMENT_START);
      sb.append(" Error while snapshoting function " + getName());
      sb.append(" ");
      sb.append(WarpScriptStack.COMMENT_END);
    }
    sb.append(" ");
    sb.append(getName());
    return sb.toString();
  }

  @Override
  public String toString() {
    return snapshot();
  }
}
