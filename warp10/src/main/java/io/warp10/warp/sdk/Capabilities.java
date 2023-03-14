//
//   Copyright 2020-2023  SenX S.A.S.
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
import java.util.concurrent.atomic.AtomicReference;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * This class is there only to prevent the capabilities map from being accessible
 * on the stack after a STACKATTRIBUTE call so capabilities cannot be added manually.
 *
 */
public class Capabilities {

  protected Map<String,String> capabilities = new HashMap<String,String>();

  private static final Map<String,String> DEFAULT_CAPABILITIES = new LinkedHashMap<String,String>();

  static {
    if (null != WarpConfig.getProperty(Configuration.CONFIG_WARP_CAPABILITIES_DEFAULT)) {
      MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
      stack.maxLimits();

      try {
        stack.execMulti(WarpConfig.getProperty(Configuration.CONFIG_WARP_CAPABILITIES_DEFAULT));
        if (1 != stack.depth() || !(stack.peek() instanceof Map)) {
          throw new WarpScriptException("Invalid value for '" + Configuration.CONFIG_WARP_CAPABILITIES_DEFAULT + "', expected a WarpScript map.");
        }
        Map<Object,Object> capabilities = (Map<Object,Object>) stack.pop();
        for (Entry<Object,Object> capability: capabilities.entrySet()) {
          if (!(capability.getKey() instanceof String && capability.getValue() instanceof String)) {
            throw new WarpScriptException("Invalid value for '" + Configuration.CONFIG_WARP_CAPABILITIES_DEFAULT + "', expected a WarpScript map with STRING keys and values.");
          }
          DEFAULT_CAPABILITIES.put(capability.getKey().toString(), capability.getValue().toString());
        }
      } catch (WarpScriptException wse) {
        throw new RuntimeException("Error initializing default capabilities.");
      }
    }
  }

  public static Capabilities get(WarpScriptStack stack) {
    if (stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR) instanceof AtomicReference
        && ((AtomicReference) stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR)).get() instanceof Capabilities) {
      return (Capabilities) ((AtomicReference) stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR)).get();
    } else {
      return null;
    }
  }

  public static void set(WarpScriptStack stack, Capabilities capabilities) {
    stack.setAttribute(WarpScriptStack.CAPABILITIES_ATTR, new AtomicReference<Capabilities>(capabilities));
  }

  public static String get(WarpScriptStack stack, String name) {
    Capabilities capabilities = get(stack);
    String cap = null;
    if (null != capabilities) {
      cap = capabilities.capabilities.get(name);
    }

    if (null == cap) {
      cap = DEFAULT_CAPABILITIES.get(name);
    }

    return cap;
  }

  public static Long getLong(WarpScriptStack stack, String name) throws WarpScriptException {
    return getLong(stack, name, null);
  }

  public static Long getLong(WarpScriptStack stack, String name, Long defaultValue) throws WarpScriptException {
    String cap = get(stack, name);

    if (null != cap) {
      try {
        return Long.parseLong(cap);
      } catch (Throwable t) {
        throw new WarpScriptException("Error parsing capability.");
      }
    }

    return defaultValue;
  }

  public static Map<String,String> get(WarpScriptStack stack, List<Object> names) {
    Capabilities capabilities = get(stack);

    Map<String,String> caps = new LinkedHashMap<String,String>();

    if (null != capabilities) {
      if (null == names || names.isEmpty()) {
        caps.putAll(capabilities.capabilities);
      } else {
        for (Object elt: names) {
          if (elt instanceof String) {
            caps.put((String) elt, capabilities.capabilities.get((String) elt));
          }
        }
      }
    }

    if (null != names && !names.isEmpty()) {
      for (Object elt: names) {
        String def = DEFAULT_CAPABILITIES.get(elt);
        if (null != def) {
          caps.putIfAbsent((String) elt, def);
        }
      }
    } else {
      for (String cap: DEFAULT_CAPABILITIES.keySet()) {
        caps.putIfAbsent(cap, DEFAULT_CAPABILITIES.get(cap));
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
      Capabilities capabilities = get(stack);

      for (Entry<String,String> entry: attributes.entrySet()) {
        if (entry.getKey().startsWith(WarpScriptStack.CAPABILITIES_PREFIX)) {
          if (null == capabilities) {
            capabilities = new Capabilities();
            set(stack, capabilities);
          }
          capabilities.putIfAbsent(entry.getKey().substring(WarpScriptStack.CAPABILITIES_PREFIX.length()), entry.getValue());
        }
      }
    }
  }

  public void clear() {
    this.capabilities.clear();
  }

  public Object remove(String key) {
    return this.capabilities.remove(key);
  }

  public Object putIfAbsent(String key, String value) {
    return this.capabilities.putIfAbsent(key, value);
  }

  public String get(String key) {
    return this.capabilities.get(key);
  }

  public Capabilities clone() {
    Capabilities newcaps = new Capabilities();
    newcaps.capabilities.putAll(this.capabilities);
    return newcaps;
  }
}
