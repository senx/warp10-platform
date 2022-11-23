//
//   Copyright 2021-2022 SenX S.A.S.
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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptATCException;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

public class GUARD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String EXPORTED_CAPABILITIES_ATTR = "guard.exported.capabilities";

  public GUARD(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();
    Object hide = null;
    boolean hasHide = false;

    if (null == top || top instanceof Long) {
      hide = top;
      hasHide = true;
      top = stack.pop();
    }

    List<Object> symbols = null;
    StackContext context = null;

    if (top instanceof List) {
      symbols = (List<Object>) top;
      top = stack.pop();
    } else if (top instanceof StackContext) {
      context = (StackContext) top;
      top = stack.pop();
    }

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a MACRO.");
    }

    Macro macro = (Macro) top;

    int hidden = 0;

    Capabilities capabilities = null;
    Object exportedCapabilities = null;
    Throwable error = null;

    try {
      if (hasHide) {
        if (null == hide) {
          hidden = stack.hide();
        } else {
          hidden = stack.hide(((Long) hide).intValue());
        }
      }

      //
      // Clone the capabilities if they were defined and save the exported capabilities
      //

      capabilities = Capabilities.get(stack);

      if (null != capabilities) {
        Capabilities.set(stack, capabilities.clone());
      }

      exportedCapabilities = stack.getAttribute(EXPORTED_CAPABILITIES_ATTR);
      stack.setAttribute(EXPORTED_CAPABILITIES_ATTR, new AtomicReference<Set>(new HashSet<String>()));

      stack.exec(macro);
    } catch (Throwable t) {
      error = t;
      //
      // If any exception was raised during the execution of the macro,
      // clear the stack and the specified symbols so no information leak
      // occurs
      //
      stack.clear();

      throw new WarpScriptATCException("Exception in GUARDed macro.");
    } finally {
      //
      // Clear the specified symbols or restore the context
      //

      if (null != symbols) {
        if (symbols.isEmpty()) {
          // Clear all registers and symbols
          Object[] regs = stack.getRegisters();

          for (int i = 0; i < regs.length; i++) {
            regs[i] = null;
          }
          stack.getSymbolTable().clear();
        } else {
          // Clear specified symbols/registers
          for (Object symbol: symbols) {
            if (symbol instanceof String) {
              stack.forget((String) symbol);
            } else if (symbol instanceof Long) {
              stack.store(((Long) symbol).intValue(), null);
            }
          }
        }
      } else if (null != context) {
        stack.restore(context);
      }

      if (hasHide) {
        stack.show(hidden);
      }

      // Restore capabilities

      boolean copyExported = false;

      if (exportedCapabilities instanceof AtomicReference) {
        AtomicReference ref = (AtomicReference) exportedCapabilities;
        if (ref.get() instanceof Set && ((Set) ref.get()).contains(null)) {
          copyExported = true;
        }
      }

      Capabilities caps = Capabilities.get(stack);

      if (null != caps) {
        // Export the specified capabilities if no error occurred
        if (null == error) {
          Capabilities newcaps = caps;
          if (!getExportedCapabilities(stack).isEmpty() && null == capabilities) {
            capabilities = new Capabilities();
          }
          for (String cap: getExportedCapabilities(stack)) {
            if (null != cap) {
              if (copyExported) {
                ((AtomicReference<Set>) exportedCapabilities).get().add(cap);
              }
              capabilities.remove(cap);
              capabilities.putIfAbsent(cap, newcaps.get(cap));
            }
          }
        }
      }

      Capabilities.set(stack, capabilities);

      stack.setAttribute(EXPORTED_CAPABILITIES_ATTR, exportedCapabilities);
    }

    return stack;
  }

  public static Set<String> getExportedCapabilities(WarpScriptStack stack) {
    if (stack.getAttribute(EXPORTED_CAPABILITIES_ATTR) instanceof AtomicReference) {
      AtomicReference ref = (AtomicReference) stack.getAttribute(EXPORTED_CAPABILITIES_ATTR);
      if (ref.get() instanceof Set) {
        return (Set<String>) ref.get();
      }
    }
    return null;
  }
}
