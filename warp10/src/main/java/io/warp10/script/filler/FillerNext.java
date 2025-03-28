//
//   Copyright 2018-2025  SenX S.A.S.
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

public class FillerNext extends NamedWarpScriptFunction implements WarpScriptFillerFunction, WarpScriptSingleValueFillerFunction.Precomputable {
  
  public FillerNext(String name) {
    super(name);
  }
  
  @Override
  public Object[] apply(Object[] args) throws WarpScriptException {
    
    Object[] results = new Object[4];
    
    Object[] next = (Object[]) args[2];
    Object[] other = (Object[]) args[1];

    // We cannot interpolate on the edges
    if (null == next[0]) {
      return results;
    }

    long tick = ((Number) other[0]).longValue();
    
    long nextloc = ((Number) next[1]).longValue();
    long nextelev = ((Number) next[2]).longValue();
    Object nextvalue = next[3];
    
    results[0] = tick;
    results[1] = nextloc;
    results[2] = nextelev;
    results[3] = nextvalue;
    
    return results;
  }
  
  @Override
  public int getPostWindow() {
    return 1;
  }
  
  @Override
  public int getPreWindow() {
    return 0;
  }

  @Override
  public WarpScriptSingleValueFillerFunction compute(GeoTimeSerie gts) throws WarpScriptException {

    GTSHelper.sort(gts);
    final int nTicks = gts.size();
    final GeoTimeSerie original = gts;
    final long lastTick = GTSHelper.tickAtIndex(original, nTicks - 1);

    return new WarpScriptSingleValueFillerFunction() {
      private int currentIndex = 0;
      @Override
      public void fillTick(long tick, GeoTimeSerie filled, Object invalidValue) throws WarpScriptException {
        if (0 == nTicks || tick >= lastTick) {
          if (null != invalidValue && tick != lastTick) {
            GTSHelper.setValue(filled, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, invalidValue, false);
          }
          return;
        }
        while (tick > GTSHelper.tickAtIndex(original, currentIndex) && currentIndex < nTicks) {
          currentIndex++;
        }
        GTSHelper.setValue(filled, tick, GTSHelper.locationAtIndex(original, currentIndex), GTSHelper.elevationAtIndex(original, currentIndex), GTSHelper.valueAtIndex(original, currentIndex), false);
      }
    };
  }
}
