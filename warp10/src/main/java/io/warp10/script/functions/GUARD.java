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
import java.util.List;
import java.util.Set;

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

      if (stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR) instanceof Capabilities) {
        capabilities = (Capabilities) stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR);
        stack.setAttribute(WarpScriptStack.CAPABILITIES_ATTR, capabilities.clone());
      }

      exportedCapabilities = stack.getAttribute(EXPORTED_CAPABILITIES_ATTR);
      stack.setAttribute(EXPORTED_CAPABILITIES_ATTR, new HashSet<String>());

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

      if (exportedCapabilities instanceof Set && ((Set) exportedCapabilities).contains(null)) {
        copyExported = true;
      }

      if (stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR) instanceof Capabilities) {
        // Export the specified capabilities if no error occurred
        if (null == error) {
          Capabilities newcaps = (Capabilities) stack.getAttribute(WarpScriptStack.CAPABILITIES_ATTR);
          for (String cap: (Set<String>) stack.getAttribute(EXPORTED_CAPABILITIES_ATTR)) {
            if (null != cap) {
              if (copyExported) {
                ((Set) exportedCapabilities).add(cap);
              }
              capabilities.remove(cap);
              capabilities.putIfAbsent(cap, newcaps.get(cap));
            }
          }
        }
      }

      if (null != capabilities) {
        stack.setAttribute(WarpScriptStack.CAPABILITIES_ATTR, capabilities);
      }

      stack.setAttribute(EXPORTED_CAPABILITIES_ATTR, exportedCapabilities);
    }

    return stack;
  }
}
