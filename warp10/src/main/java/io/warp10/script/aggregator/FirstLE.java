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

package io.warp10.script.aggregator;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptException;

/**
 * Return the first value less or equal to a threshold
 *
 */
public class FirstLE extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptReducerFunction, WarpScriptBucketizerFunction {
  
  private long lthreshold;
  private double dthreshold;
  private String sthreshold;
  
  private TYPE type;
  
  public FirstLE(String name, long threshold) {
    super(name);
    this.type = TYPE.LONG;
    this.lthreshold = threshold;
  }
  
  public FirstLE(String name, double threshold) {
    super(name);
    this.type = TYPE.DOUBLE;
    this.dthreshold = threshold;
  }

  public FirstLE(String name, String threshold) {
    super(name);
    this.type = TYPE.STRING;
    this.sthreshold = threshold;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    
    long tick = Long.MAX_VALUE;
    
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];
    
    int idx = -1;
    
    for (int i = 0; i < values.length; i++) {
      // skip ticks older than the one we already identified
      if (ticks[i] >= tick) {
        continue;
      }

      if (values[i] instanceof Number) {
        switch (type) {
          case LONG:
            if (((Number) values[i]).longValue() <= lthreshold) {
              idx = i;
              tick = ticks[i];
            }
            break;
          case DOUBLE:
            if (((Number) values[i]).doubleValue() <= dthreshold) {
              idx = i;
              tick = ticks[i];
            }
            break;
          case STRING:
            if (sthreshold.compareTo(values[i].toString()) >= 0) {
              idx = i;
              tick = ticks[i];
            }
            break;
        }        
      } else if (values[i] instanceof String && TYPE.STRING == type) {
        if (sthreshold.compareTo(values[i].toString()) >= 0) {
          idx = i;
          tick = ticks[i];
        }
      }
    }    
    
    if (-1 != idx) {
      return new Object[] { ticks[idx], locations[idx], elevations[idx], values[idx] };
    } else {
      return new Object[] { args[0], GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };      
    }
  }
}
