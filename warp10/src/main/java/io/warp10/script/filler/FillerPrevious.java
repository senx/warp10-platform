//
//   Copyright 2018  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptSingleValueFillerFunction;

public class FillerPrevious extends NamedWarpScriptFunction implements WarpScriptFillerFunction, WarpScriptSingleValueFillerFunction.Precomputable {
  
  public FillerPrevious(String name) {
    super(name);
  }
  
  @Override
  public Object[] apply(Object[] args) throws WarpScriptException {
    
    Object[] results = new Object[4];
    
    Object[] prev = (Object[]) args[1];
    Object[] other = (Object[]) args[2];

    // We cannot interpolate on the edges
    if (null == prev[0]) {
      return results;
    }

    long tick = ((Number) other[0]).longValue();
    
    long prevloc = ((Number) prev[1]).longValue();
    long prevelev = ((Number) prev[2]).longValue();
    Object prevvalue = prev[3];
    
    results[0] = tick;
    results[1] = prevloc;
    results[2] = prevelev;
    results[3] = prevvalue;
    
    return results;
  }
    
  @Override
  public int getPostWindow() {
    return 0;
  }
  
  @Override
  public int getPreWindow() {
    return 1;
  }

  @Override
  public WarpScriptSingleValueFillerFunction compute(GeoTimeSerie gts) throws WarpScriptException {

    GTSHelper.sort(gts);
    final int nTicks = gts.size();
    final GeoTimeSerie original = gts;
    final long firstTick = GTSHelper.tickAtIndex(original, 0);

    return new WarpScriptSingleValueFillerFunction() {
      private int currentIndex = 0;

      @Override
      public void fillTick(long tick, GeoTimeSerie filled, Object invalidValue) throws WarpScriptException {

        if (0 == nTicks || tick <= firstTick) {
          if (null != invalidValue && tick != firstTick) {
            GTSHelper.setValue(filled, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, invalidValue, false);
          }
          return;
        }
        while (tick > GTSHelper.tickAtIndex(original, currentIndex) && currentIndex < nTicks) {
          currentIndex++;
        }
        int previousIndex = 0 == currentIndex ? 0 : currentIndex - 1;
        GTSHelper.setValue(filled, tick, GTSHelper.locationAtIndex(original, previousIndex), GTSHelper.elevationAtIndex(original, previousIndex), GTSHelper.valueAtIndex(original, previousIndex), false);

      }

    };
  }
}
