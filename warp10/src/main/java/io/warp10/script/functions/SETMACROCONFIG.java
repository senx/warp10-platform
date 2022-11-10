//
//   Copyright 2019-2022  SenX S.A.S.
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.warp10.WarpConfig;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

/**
 * Sets a configuration for some macros
 */
public class SETMACROCONFIG extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public SETMACROCONFIG(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    String capability = Capabilities.get(stack, WarpScriptStack.CAPABILITY_SETMACROCONFIG);

    if (null == capability) {
      throw new WarpScriptException(getName() + " missing capability.");
    }

    Pattern pattern = Pattern.compile(capability);

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects the configuration key to be a STRING.");
    }

    String key = String.valueOf(top);

    if (!(key.contains("@"))) {
      throw new WarpScriptException(getName() + " expects the configuration key to adhere to the format name@scope.");
    }

    Matcher m = pattern.matcher(key);

    if (!m.matches()) {
      throw new WarpScriptException(getName() + " key is not modifiable using the current capability.");
    }

    top = stack.pop();

    String value = null != top ? String.valueOf(top) : null;

    WarpConfig.setProperty(key, value);

    return stack;
  }
}
