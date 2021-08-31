//
//   Copyright 2018-2021  SenX S.A.S.
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

package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptLoopBreakException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class BREAK extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static String[] loopNames = new String[]{WarpScriptLib.FOR, WarpScriptLib.FOREACH, WarpScriptLib.FORSTEP, WarpScriptLib.WHILE, WarpScriptLib.UNTIL};

  // The stack trace is not used so it can be instantiated once.
  private final WarpScriptLoopBreakException ex;

  public BREAK(String name) {
    super(name);
    ex = new WarpScriptLoopBreakException("Exception at " + name + ": must be used in a one of " + String.join(", ", loopNames) + " loop.");
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    throw ex;
  }
}
