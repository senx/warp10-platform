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

package io.warp10.script.op;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptNAryFunction;
import io.warp10.script.WarpScriptException;

/**
 * OR or AND values from multiple time series. The elevation and location are cleared.
 */
public class OpBoolean extends NamedWarpScriptFunction implements WarpScriptNAryFunction {

  /**
   * Does this apply an "or" (true) or an "and" (false).
   */
  private final Boolean or;

  /**
   * Should we ignore nulls (false) or forbid them (true)
   */
  private final boolean forbidNulls;

  /**
   * Build a boolean operator applying either OR or AND.
   * @param name Name given to this function
   * @param applyOr Whether to apply "or" (true) or an "and" (false).
   * @param forbidNulls Whether to ignore nulls (false) or forbid them (true).
   */
  public OpBoolean(String name, boolean applyOr, boolean forbidNulls) {
    super(name);
    this.or = applyOr;
    this.forbidNulls = forbidNulls;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    Object[] values = (Object[]) args[6];
    
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;

    for (Object value: values) {
      // If one of the values is 'null' (absent), return null as the value
      if (null == value) {
        if (this.forbidNulls) {
          return new Object[] {tick, location, elevation, null};
        } else {
          continue;
        }
      }

      // If this function applies an OR, this can stop as soon as it encounters a True value and the result will be True.
      // If this function applies an AND, this can stop as soon as it encounters a False value and the result will be False.
      if (this.or.equals(value)) {
        return new Object[] {tick, location, elevation, this.or};
      }
    }

    // No trigger value (see comment above) has been found, return False in the case of OR or True in the case of AND.
    return new Object[] {tick, location, elevation, !this.or};
  }
}
