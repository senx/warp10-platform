//
//   Copyright 2021  SenX S.A.S.
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

import java.util.ArrayList;
import java.util.List;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

public abstract class MacroResolver {
  private static final List<MacroResolver> resolvers = new ArrayList<MacroResolver>();

  public static void register(MacroResolver resolver) {
    resolvers.add(resolver);
  }

  /**
   * Implementation of the actual macro finding method
   *
   * @param stack WarpScriptStack context within which to find the macro
   * @param name Name of Macro to find
   * @return The found Macro or null if none was found
   * @throws WarpScriptException in case of error
   */
  public abstract Macro findMacro(WarpScriptStack stack, String name) throws WarpScriptException;

  /**
   * Attempt to find the named macro by invoking resolvers in the order in which they appear.
   * The first non null result will be returned.
   * If no resolver found the named macro, null will be ultimately returned.
   *
   * @param stack WarpScriptStack context within which to find the macro
   * @param name Name of Macro to find
   * @return The found Macro or null if none was found
   * @throws WarpScriptException in case of error
   */
  public static Macro find(WarpScriptStack stack, String name) throws WarpScriptException {
    for (MacroResolver resolver: resolvers) {
      Macro macro = resolver.findMacro(stack, name);

      if (null != macro) {
        return macro;
      }
    }
    return null;
  }

}
