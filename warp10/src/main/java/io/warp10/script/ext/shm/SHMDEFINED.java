//
//   Copyright 2020-2022  SenX S.A.S.
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

package io.warp10.script.ext.shm;

import java.util.regex.Pattern;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

public class SHMDEFINED extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public SHMDEFINED(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    String cap = Capabilities.get(stack, SharedMemoryWarpScriptExtension.CAPABILITY_SHMLOAD);
    if (null == cap) {
      throw new WarpScriptException(getName() + " expected capability '" + SharedMemoryWarpScriptExtension.CAPABILITY_SHMLOAD + "' to be set.");
    }

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a symbol name.");
    }

    String symbol = (String) top;

    if (!"".equals(cap) && !Pattern.matches(cap, symbol)) {
      throw new WarpScriptException(getName() + " capability does not grant access to symbol '" + symbol + "'.");
    }

    stack.push(SharedMemoryWarpScriptExtension.defined(symbol));

    return stack;
  }
}
