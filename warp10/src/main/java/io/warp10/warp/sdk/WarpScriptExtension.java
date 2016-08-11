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

package io.warp10.warp.sdk;

import io.warp10.script.WarpScriptLib;

import java.util.Map;

/**
 * Interface for extending WarpScript.
 * 
 * A class implementing this interface is called a WarpScript extension,
 * it makes available a set of functions which will redefine, remove or augment
 * the functions already defined in WarpScriptLib.
 *
 */
public abstract class WarpScriptExtension {
  /**
   * Return a map of function name to actual function.
   * If a function should be removed from WarpScriptLib,
   * simply associate null as the value.
   */
  public abstract Map<String,Object> getFunctions();
  
  public final void register() {
    WarpScriptLib.register(this);
  }
}
