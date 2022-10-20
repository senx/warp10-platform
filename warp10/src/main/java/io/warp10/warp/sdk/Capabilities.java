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

package io.warp10.warp.sdk;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.warp10.continuum.Tokens;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * This class is there only to prevent the capabilities map from being accessible
 * on the stack after a STACKATTRIBUTE call so capabilities cannot be added manually.
 *
 */
public class Capabilities {

  protected Map<String,String> capabilities = new HashMap<String,String>();

  public static String get(WarpScriptStack stack, String name) {
    Capabilities capabilities = null;
    if (stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR) instanceof Capabilities) {
      capabilities = (Capabilities) stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR);
    }
    if (null != capabilities) {
      return capabilities.capabilities.get(name);
    } else {
      return null;
    }
  }

  public static Map<String,String> get(WarpScriptStack stack, List<Object> names) {
    Capabilities capabilities = null;

    if (stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR) instanceof Capabilities) {
      capabilities = (Capabilities) stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR);
    }

    Map<String,String> caps = new LinkedHashMap<String,String>();
    if (null != capabilities) {
      if (null == names) {
        caps.putAll(capabilities.capabilities);
      } else {
        for (Object elt: names) {
          if (elt instanceof String) {
            caps.put((String) elt, capabilities.capabilities.get((String) elt));
          }
        }
      }
    }

    return caps;
  }

  /**
   * Add the capabilities from a token to a stack
   *
   * @param stack Stack which should be modified
   * @param token Token representation containing the capabilities to add
   * @throws WarpScriptException in case the token is invalid
   */
  public static void add(WarpScriptStack stack, String token) throws WarpScriptException {
    Map<String,String> attributes = null;

    try {
      ReadToken rtoken = Tokens.extractReadToken(token);
      attributes = rtoken.getAttributes();
    } catch (Exception e) {
      try {
        WriteToken wtoken = Tokens.extractWriteToken(token);
        attributes = wtoken.getAttributes();
      } catch (Exception ee) {
        throw new WarpScriptException("invalid token.");
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
  }

  public void clear() {
    this.capabilities.clear();
  }

  public boolean containsKey(String key) {
    return this.capabilities.containsKey(key);
  }

  public Object remove(String key) {
    return this.capabilities.remove(key);
  }

  public Object putIfAbsent(String key, String value) {
    return this.capabilities.putIfAbsent(key, value);
  }
}
