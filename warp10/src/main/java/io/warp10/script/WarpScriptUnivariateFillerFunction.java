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

public interface WarpScriptUnivariateFillerFunction extends WarpScriptFillerFunction {

  /**
   * The interface used to evaluate the filled value.
   * The implementation can compute an evaluator specific to the data in the GTS being filled
   */
  public interface Evaluator {

    // number of input ticks depends on the window size
    Object[] evaluate(Long... ticks);
  }

  /**
   * Compute the Evaluator
   * @param gts
   */
  public Evaluator computeEvaluator(GeoTimeSerie gts);

  /**
   * Returns the size of the pre-window (in number of ticks)
   */
  public int getPreWindow();

  /**
   * Returns the size of the post-window (in ticks)
   */
  public int getPostWindow();

  /**
   * Implemented for compatibility with cross fill
   * @param args
   * @return
   * @throws WarpScriptException
   */
  default public Object[] apply(Object[] args) throws WarpScriptException {
    throw new RuntimeException("This method should not be called");
  }
}
