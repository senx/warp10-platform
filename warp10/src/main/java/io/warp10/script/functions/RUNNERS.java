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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.ScriptRunner;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

/**
 * List runners matching a regexp
 */
public class RUNNERS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public RUNNERS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPNAME_RUNNERS)) {
      throw new WarpScriptException(getName() + " requires capability " + WarpScriptStack.CAPNAME_RUNNERS+ ".");
    }

    Matcher matcher = Pattern.compile(Capabilities.get(stack, WarpScriptStack.CAPNAME_RUNNERS)).matcher("");

    Object o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a regular expression (STRING).");
    }

    ScriptRunner sr = ScriptRunner.getInstance();

    if (null == sr) {
      throw new WarpScriptException(getName() + " runners are not supported.");
    }

    try {
      Map<String,Object> runners = sr.getScheduled((String) o);

      Map<String,Object> filtered = new LinkedHashMap<String,Object>();

      for (Entry<String,Object> entry: runners.entrySet()) {
        if(matcher.reset(entry.getKey()).matches()) {
          filtered.put(entry.getKey(), entry.getValue());
        }
      }

      stack.push(filtered);
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " error retrieving runners.", ioe);
    }

    return stack;
  }
}
