//
//   Copyright 2020  SenX S.A.S.
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

import java.util.Map;
import java.util.Map.Entry;

import io.warp10.continuum.Tokens;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

public class CAPADD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public CAPADD(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a TOKEN.");
    }

    String token = (String) top;

    Map<String,String> attributes = null;

    try {
      ReadToken rtoken = Tokens.extractReadToken(token);
      attributes = rtoken.getAttributes();
    } catch (Exception e) {
      try {
        WriteToken wtoken = Tokens.extractWriteToken(token);
        attributes = wtoken.getAttributes();
      } catch (Exception ee) {
        throw new WarpScriptException(getName() + " invalid token.");
      }
    }

    if (null != attributes && !attributes.isEmpty()) {
      Capabilities capabilities = null;

      if (stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR) instanceof Capabilities) {
        capabilities = (Capabilities) stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR);
      }

      for (Entry<String,String> entry: attributes.entrySet()) {
        if (entry.getKey().startsWith(WarpScriptStack.CAPABILITIES_PREFIX)) {
          if (null == capabilities) {
            capabilities = new Capabilities();
            stack.setAttribute(WarpScriptStack.CAPABILITIES_ATTR, capabilities);
          }
          capabilities.putIfAbsent(entry.getKey().substring(WarpScriptStack.CAPABILITIES_PREFIX.length()), entry.getValue());
        }
      }
    }

    return stack;
  }
}
