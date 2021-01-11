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

package io.warp10.warp.sdk;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
