//
//   Copyright 2019-2024  SenX S.A.S.
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WrappedStatementUtils;

/**
 * Extract all used variables in a macro. If a STORE/CSTORE or LOAD operation is
 * found with a parameter which is not a string, then an error is raised.
 *
 * This function can also be restricted to return only variables which are used
 * by STORE/CSTORE and POPR. This avoid returning variables only used by LOAD
 * which are typical of "global" variables. This behaviour is particularly useful
 * for ASREGS because it should avoid replacing those "global" variables by registers.
 * Indeed, the STORE would still be called on the original variable name while the
 * PUSHR will be done on an empty register.
 */
public class VARS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public VARS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    boolean onlyStoreAndPopr = false;
    if (top instanceof Boolean) {
      onlyStoreAndPopr = (Boolean) top;
      top = stack.pop();
    }

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a macro.");
    }

    Macro macro = (Macro) top;

    try {
      stack.push(getVars(macro, onlyStoreAndPopr));
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " failed.", wse);
    }

    return stack;
  }

  public static List<Object> getVars(Macro macro) throws WarpScriptException {
    return getVars(macro, false);
  }

  /**
   * Loop over the macro statements, in a recursive manner, extracting all variable names.
   *
   * @param macro The root macro to extract the variable names from.
   * @param onlyStoreAndPopr Return only variables used for storage. Useful for ASREGS to avoid replacing "global" variables.
   * @return The list of variable names.
   * @throws WarpScriptException If a STORE call is found to be using neither a String, Long or a list thereof.
   */
  public static List<Object> getVars(Macro macro, boolean onlyStoreAndPopr) throws WarpScriptException {
    Set<Object> symbols = new LinkedHashSet<Object>();

    final Map<Object, AtomicInteger> occurrences = new HashMap<Object, AtomicInteger>();

    List<Macro> allmacros = new ArrayList<Macro>();
    allmacros.add(macro);

    boolean abort = false;

    while (!abort && !allmacros.isEmpty()) {
      Macro m = allmacros.remove(0);

      List<Object> statements = new ArrayList<Object>(m.statements());

      for (int i = 0; i < statements.size(); i++) {
        Object currentSymbol = WrappedStatementUtils.unwrapAll(statements.get(i));

        if (currentSymbol instanceof Macro) {
          allmacros.add((Macro) currentSymbol);
          continue;
        } else if (currentSymbol instanceof POPR) {
          symbols.add((long) ((POPR) currentSymbol).getRegister());
          AtomicInteger occ = occurrences.get((long) ((POPR) currentSymbol).getRegister());
          if (null == occ) {
            occ = new AtomicInteger();
            occurrences.put((long) ((POPR) currentSymbol).getRegister(), occ);
          }
          occ.incrementAndGet();
        } else if (currentSymbol instanceof PUSHR) {
          if(!onlyStoreAndPopr) {
            symbols.add((long) ((PUSHR) currentSymbol).getRegister());
          }
          AtomicInteger occ = occurrences.get((long) ((PUSHR) currentSymbol).getRegister());
          if (null == occ) {
            occ = new AtomicInteger();
            occurrences.put((long) ((PUSHR) currentSymbol).getRegister(), occ);
          }
          occ.incrementAndGet();
        } else if (currentSymbol instanceof RUNR) {
          if(!onlyStoreAndPopr) {
            symbols.add((long) ((RUNR) currentSymbol).getRegister());
          }
          AtomicInteger occ = occurrences.get((long) ((RUNR) currentSymbol).getRegister());
          if (null == occ) {
            occ = new AtomicInteger();
            occurrences.put((long) ((RUNR) currentSymbol).getRegister(), occ);
          }
          occ.incrementAndGet();
        } else if (currentSymbol instanceof LOAD || currentSymbol instanceof CSTORE || currentSymbol instanceof RUN) {
          if (0 == i) {
            abort = true;
            break;
          }
          Object previousSymbol = WrappedStatementUtils.unwrapAll(statements.get(i - 1));
          // If the parameter to LOAD/CSTORE is not a string or a long, then we cannot extract
          // the variables in a safe way as some may be unknown to us (as their name may be the result
          // of a computation), so in this case we abort the process
          if (previousSymbol instanceof String || previousSymbol instanceof Long) {
            // Never add RUN symbol because they may reference a macro in a repository, not a macro stored in
            // a variable. Use them for the occurences count nonetheless.
            if(currentSymbol instanceof CSTORE || (currentSymbol instanceof LOAD && !onlyStoreAndPopr)) {
              symbols.add(previousSymbol);
            }
            AtomicInteger occ = occurrences.get(previousSymbol);
            if (null == occ) {
              occ = new AtomicInteger();
              occurrences.put(previousSymbol, occ);
            }
            occ.incrementAndGet();
          } else {
            abort = true;
            break;
          }
        } else if (currentSymbol instanceof STORE) {
          if (0 == i) {
            abort = true;
            break;
          }
          Object previousSymbol = WrappedStatementUtils.unwrapAll(statements.get(i - 1));
          if (previousSymbol instanceof List) {
            // We inspect the list, looking for registers
            for (Object elt: (List) previousSymbol) {
              if (elt instanceof String || elt instanceof Long) {
                symbols.add(elt);
                AtomicInteger occ = occurrences.get(elt);
                if (null == occ) {
                  occ = new AtomicInteger();
                  occurrences.put(elt, occ);
                }
                occ.incrementAndGet();
              } else if (null != elt) {
                abort = true;
                break;
              }
            }
          } else if (previousSymbol instanceof ENDLIST) {
            // We go backwards in statements until we find a MARK, inspecting elements
            // If we encounter something else than String/Long/NULL, we abort as we cannot
            // determine if a register is used or not
            int idx = i - 2;
            while (idx >= 0 && !(WrappedStatementUtils.unwrapAll(statements.get(idx)) instanceof MARK)) {
              Object stmt = WrappedStatementUtils.unwrapAll(statements.get(idx--));
              if (stmt instanceof String || stmt instanceof Long) {
                symbols.add(stmt);
                AtomicInteger occ = occurrences.get(stmt);
                if (null == occ) {
                  occ = new AtomicInteger();
                  occurrences.put(stmt, occ);
                }
                occ.incrementAndGet();
              } else if (null != stmt && !(stmt instanceof NULL)) {
                abort = true;
                break;
              }
            }
          } else if (previousSymbol instanceof String || previousSymbol instanceof Long) {
            symbols.add(previousSymbol);
            AtomicInteger occ = occurrences.get(previousSymbol);
            if (null == occ) {
              occ = new AtomicInteger();
              occurrences.put(previousSymbol, occ);
            }
            occ.incrementAndGet();
          } else {
            // We encountered a STORE with something that is neither a register, a string or
            // a list, so we cannot determine if a register is involved or not, so we abort
            abort = true;
            break;
          }
        }
      }
    }

    if (abort) {
      throw new WarpScriptException("Encountered a LOAD/STORE or CSTORE operation with a non explicit symbol name.");
    }

    List<Object> vars = new ArrayList<Object>(symbols);

    // Now sort according to the number of occurrences (decreasing)

    vars.sort(new Comparator<Object>() {
      @Override
      public int compare(Object s1, Object s2) {
        AtomicInteger occ1 = occurrences.get(s1);
        AtomicInteger occ2 = occurrences.get(s2);

        if (occ1.get() < occ2.get()) {
          return 1;
        } else if (occ1.get() > occ2.get()) {
          return -1;
        } else {
          return s1.toString().compareTo(s2.toString());
        }
      }
    });

    return vars;
  }
}
