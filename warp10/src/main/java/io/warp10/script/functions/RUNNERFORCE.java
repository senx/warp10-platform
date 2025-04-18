//
//   Copyright 2025  SenX S.A.S.
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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.ScriptRunner;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

/**
 * Force the next execution of a runner
 */
public class RUNNERFORCE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public RUNNERFORCE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPNAME_RUNNERFORCE)) {
      throw new WarpScriptException(getName() + " requires capability " + WarpScriptStack.CAPNAME_RUNNERFORCE+ ".");
    }

    Matcher matcher = Pattern.compile(Capabilities.get(stack, WarpScriptStack.CAPNAME_RUNNERFORCE)).matcher("");

    Object o = stack.pop();

    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a timestamp (LONG).");
    }

    long timestamp = ((Long) o).longValue();

    o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a script name.");
    }

    String script = (String) o;

    if (!matcher.reset(script).matches()) {
      throw new WarpScriptException(getName() + " capability '" + WarpScriptStack.CAPNAME_RUNNERFORCE + "' doesn't allow acting on script '" + script + "'.");
    }

    ScriptRunner sr = ScriptRunner.getInstance();

    sr.reschedule(script, timestamp * (1_000_000_000 / Constants.TIME_UNITS_PER_S));

    return stack;
  }
}
