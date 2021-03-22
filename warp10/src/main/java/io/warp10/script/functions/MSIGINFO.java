//
//   Copyright 2020-2021  SenX S.A.S.
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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bouncycastle.jce.interfaces.ECPublicKey;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class MSIGINFO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String KEY_SIG = "sig";
  private static final String KEY_KEY = "key";

  public MSIGINFO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a signature macro.");
    }

    Macro macro = (Macro) top;

    int size = macro.size();

    Map<Object,Object> siginfo = new LinkedHashMap<Object,Object>();

    if (size < 3
        || !(macro.get(size - 1) instanceof MSIG)
        || !(macro.get(size - 2) instanceof ECPublicKey)
        || !(macro.get(size - 3) instanceof byte[])) {
    }

    siginfo.put(KEY_SIG, Arrays.copyOf((byte[]) macro.get(size - 3), ((byte[]) macro.get(size - 3)).length));
    siginfo.put(KEY_KEY, (ECPublicKey) macro.get(size - 2));

    return stack;
  }
}
