//
//   Copyright 2024-2025  SenX S.A.S.
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

package io.warp10.script;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;

public interface WarpScriptSingleValueFillerFunction {

  public interface Precomputable extends WarpScriptSingleValueFillerFunction {
    public WarpScriptSingleValueFillerFunction compute(GeoTimeSerie gts) throws WarpScriptException;

    default public void fillTick(long tick, GeoTimeSerie filled, Object invalidValue) throws WarpScriptException {
      Object v = evaluate(tick);
      GTSHelper.setValue(filled, tick, null == v ? invalidValue : v);
    }

    @Deprecated
    default public Object evaluate(long tick) throws WarpScriptException {
      throw new WarpScriptException("Invalid Filler Error: evaluator function has not been precomputed yet.");
    }
  }

  /**
   * This method allows a filler to fill geo, elevation, value most efficiently. It delegates the setValue to the filler. 
   * The default implementation calls evaluate for retro compatibility with older fillers.
   * @param tick is the tick to fill
   * @param filled is a sorted clone of the original GTS
   * @param invalidValue may be defined by the user when calling the FILL function
   */
  default public void fillTick(long tick, GeoTimeSerie filled, Object invalidValue) throws WarpScriptException {
    Object v = evaluate(tick);
    GTSHelper.setValue(filled, tick, null == v ? invalidValue : v);
  }

  /**
   * This method returns the filled value only. There is no way to fill geo or elevation.
   * @param tick is the tick to fill
   * @return the value of the GTS to add at tick
   */
  @Deprecated
  default public Object evaluate(long tick) throws WarpScriptException {
    throw new WarpScriptException("Invalid Filler Definition Error: evaluate function is deprecated.");
  }
}
