//
//   Copyright 2024  SenX S.A.S.
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

import io.warp10.continuum.gts.GeoTimeSerie;

public interface WarpScriptSingleValueFillerFunction {

  public interface Precomputable extends WarpScriptSingleValueFillerFunction {
    public WarpScriptSingleValueFillerFunction compute(GeoTimeSerie gts) throws WarpScriptException;

    default public Object evaluate(long tick) throws WarpScriptException {
      throw new WarpScriptException("Invalid Filler Error: evaluator function has not been precomputed yet.");
    }
  }

  public Object evaluate(long tick) throws WarpScriptException;
}
