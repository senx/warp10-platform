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

/**
 * An interface to create function used for data imputation.
 */
public interface WarpScriptUnivariateFillerFunction {

  /**
   * This method implements the imputation function.
   * If the input tick is an invalid point, null is expected to be returned.
   * @param tick
   * @return Imputed value or null
   * @throws WarpScriptException
   */
  //public Object value(Object params, long tick) throws WarpScriptException;

  // todo
}
