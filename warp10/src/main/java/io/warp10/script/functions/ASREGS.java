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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Modifies a Macro so the LOAD/STORE/RUN operations for the given variables are
 * replaced by use of registers.
 * Also converts LOAD/STORE/CSTORE/RUN used on register numbers to PUSHRx/POPRx/CPOPRx/RUNRx.
 * Optimization for STORE on lists is only done if the list contains only registers numbers
 * or NULL and is a list construction (MARK....ENDLIST STORE), not a list instance (!$a_list STORE).
 */
public class ASREGS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static class XNOOP extends NOOP {
    public XNOOP(String name) {
      super(name);
    }
  }

  private static final XNOOP XNOOP = new XNOOP(WarpScriptLib.NOOP);
  // DROP used for NULL or duplicate registers in STORE lists.
  private static final DROP DROP = new DROP(WarpScriptLib.DROP);

  private static final HashMap<Integer, PUSHR> PUSHRX = new HashMap<Integer, PUSHR>();
  private static final HashMap<Integer, POPR> POPRX = new HashMap<Integer, POPR>();
  private static final HashMap<Integer, POPR> CPOPRX = new HashMap<Integer, POPR>();
  private static final HashMap<Integer, RUNR> RUNRX = new HashMap<Integer, RUNR>();

  public ASREGS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    List vars = null;

    // Set the optional list of variable names to convert to registers.
    if (top instanceof List) {
      vars = (List) top;
      top = stack.pop();
    }

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects an optional list of variable names and a macro.");
    }

    Macro macro = (Macro) top;

    if (null == vars) {
      try {
        // If variables are not defined, only get variables and registers used by STORE and POPR
        // to avoid global variables.
        vars = VARS.getVars(macro, true);
      } catch (WarpScriptException wse) {
        throw new WarpScriptException(getName() + " failed.", wse);
      }
    }

    Object[] regs = stack.getRegisters();
    int nregs = regs.length;

    // Bitset to determine the used registers
    BitSet inuse = new BitSet(nregs);

    //
    // Inspect the macro to determine the registers which are already used
    //
    List<Macro> allmacros = new ArrayList<Macro>();
    allmacros.add(macro);

    boolean abort = false;

    while(!abort && !allmacros.isEmpty()) {
      Macro m = allmacros.remove(0);

      if (m.isSecure()) {
        throw new WarpScriptException(getName() + " cannot operate on a secure Macro.");
      }

      List<Object> statements = new ArrayList<Object>(m.statements());

      for (int i = 0; i < statements.size(); i++) {
        Object currentSymbol = statements.get(i);

        if (currentSymbol instanceof PUSHR) {
          inuse.set(((PUSHR) currentSymbol).getRegister());
        } else if (currentSymbol instanceof POPR) {
          inuse.set(((POPR) currentSymbol).getRegister());
        } else if (currentSymbol instanceof RUNR) {
          inuse.set(((RUNR) currentSymbol).getRegister());
        } else if (currentSymbol instanceof LOAD || currentSymbol instanceof CSTORE  || currentSymbol instanceof RUN) {
          // If the statement is the first, we cannot determine if what
          // we load or update is a register or a variable, so abort.
          if (0 == i) {
            abort = true;
            break;
          }
          // Fetch what precedes the LOAD/CSTORE/RUN
          Object previousSymbol = statements.get(i - 1);
          if (previousSymbol instanceof Long) {
            inuse.set(((Long) previousSymbol).intValue());
          } else if (currentSymbol instanceof RUN && previousSymbol instanceof String) {
            // RUN applied to a STRING means we call an external macro which might
            // use some symbols, so we cannot do any replacement
            abort = true;
            break;
          } else if (!(previousSymbol instanceof String)) {
            // We encountered a LOAD/CSTORE with a non string and non long param,
            // we cannot determine what is being loaded or updated, so no replacement
            // can occur
            abort = true;
            break;
          }
        } else if (currentSymbol instanceof STORE) {
          // If the statement is the first, we cannot determine if what
          // we load is a register or a variable, so abort.
          if (0 == i) {
            abort = true;
            break;
          }
          // Fetch what precedes the STORE
          Object previousSymbol = statements.get(i - 1);
          if (previousSymbol instanceof Long) {
            inuse.set(((Long) previousSymbol).intValue());
          } else if (previousSymbol instanceof List) {
            // We inspect the list, looking for registers
            for (Object elt: (List) previousSymbol) {
              if (elt instanceof Long) {
                inuse.set(((Long) elt).intValue());
              }
            }
          } else if (previousSymbol instanceof ENDLIST) {
            // We go backwards in statements until we find a MARK, inspecting elements
            // If we encounter something else than String/Long/NULL, we abort as we cannot
            // determine if a register is used or not
            int idx = i - 2;
            while (idx >= 0 && !(statements.get(idx) instanceof MARK)) {
              Object stmt = statements.get(idx--);
              if (stmt instanceof Long) {
                inuse.set(((Long) stmt).intValue());
              } else if (null != stmt && !(stmt instanceof String) && !(stmt instanceof NULL)) {
                abort = true;
                break;
              }
            }
          } else if (!(previousSymbol instanceof String)) {
            // We encountered a STORE with something that is neither a register, a string or
            // a list, so we cannot determine if a register is involved or not, so we abort
            abort = true;
            break;
          }
        } else if (currentSymbol instanceof Macro) {
          allmacros.add((Macro) currentSymbol);
        } else if (currentSymbol instanceof LSTORE || currentSymbol instanceof MSTORE) {
          abort = true;
          break;
        }
      }
    }

    if (abort) {
      throw new WarpScriptException(getName() + " was unable to convert variables to registers.");
    }

    Map<String, Integer> varregs = new HashMap<String, Integer>();

    for (Object v: vars) {

      if (v instanceof Long) {
        continue;
      }

      // Stop processing variables if we already assigned all the registers
      if (inuse.cardinality() == nregs) {
        break;
      }

      if (!(v instanceof String)) {
        throw new WarpScriptException(getName() + " expects a list of variable names on top of the stack.");
      }

      if (null == varregs.get(v.toString())) {
        int regidx = inuse.nextClearBit(0);
        inuse.set(regidx);
        varregs.put(v.toString(), regidx);
      }
    }

    //
    // Now loop over the macro statement, replacing occurrences of X LOAD/STORE/CSTORE/RUN by the use
    // of the assigned register
    //

    allmacros.clear();
    allmacros.add((Macro) top);

    while(!abort && !allmacros.isEmpty()) {
      Macro m = allmacros.remove(0);

      List<Object> statements = new ArrayList<Object>(m.statements());

      for (int i = 0; i < statements.size(); i++) {
        Object currentSymbol = statements.get(i);

        if (currentSymbol instanceof Macro) {
          allmacros.add((Macro) currentSymbol);
          continue;
        } else if (i > 0 && currentSymbol instanceof LOAD) {
          Object previousSymbol = statements.get(i - 1);

          if (previousSymbol instanceof String) {
            Integer regno = varregs.get(previousSymbol.toString());
            if (null != regno) {
              statements.set(i - 1, XNOOP);
              PUSHR pushr = PUSHRX.computeIfAbsent(regno, r -> new PUSHR(WarpScriptLib.PUSHR + r, r));
              statements.set(i, pushr);
            }
          } else if (previousSymbol instanceof Long) {
            // Also optimize LOAD on a long with PUSHR which is much faster
            statements.set(i - 1, XNOOP);
            PUSHR pushr = PUSHRX.computeIfAbsent(((Long) previousSymbol).intValue(), r -> new PUSHR(WarpScriptLib.PUSHR + r, r));
            statements.set(i, pushr);
          } else {
            abort = true;
            break;
          }
        } else if (i > 0 && currentSymbol instanceof RUN) {
          Object previousSymbol = statements.get(i - 1);

          if (previousSymbol instanceof Long) {
            // Optimize RUN on a long with RUNR which is faster
            statements.set(i - 1, XNOOP);
            RUNR runr = RUNRX.computeIfAbsent(((Long) previousSymbol).intValue(), r -> new RUNR(WarpScriptLib.RUNR + r, r));
            statements.set(i, runr);
          } else {
            abort = true;
            break;
          }
        } else if (i > 0 && currentSymbol instanceof STORE) {
          Object previousSymbol = statements.get(i - 1);
          if (previousSymbol instanceof String) {
            Integer regno = varregs.get(previousSymbol.toString());
            if (null != regno) {
              statements.set(i - 1, XNOOP);
              POPR popr = POPRX.computeIfAbsent(regno, r -> new POPR(WarpScriptLib.POPR + r, r));
              statements.set(i, popr);
            }
          } else if (previousSymbol instanceof List) {
            for (int k = 0; k < ((List) previousSymbol).size(); k++) {
              if (((List) previousSymbol).get(k) instanceof String) {
                Integer regno = varregs.get(((List) previousSymbol).get(k).toString());
                if (null != regno) {
                  ((List) previousSymbol).set(k, (long) regno);
                }
              }
            }
          } else if (previousSymbol instanceof ENDLIST) {
            int idx = i - 2;
            int nbOfRegOrNull = 0; // Keeps track of the number of registers or nulls in this list
            while (idx >= 0 && !(statements.get(idx) instanceof MARK)) {
              Object stmt = statements.get(idx);
              if (stmt instanceof String) {
                Integer regno = varregs.get(stmt);
                if (null != regno) {
                  statements.set(idx, (long) regno);
                  nbOfRegOrNull++;
                }
              } else if (stmt instanceof Long || stmt instanceof NULL) {
                nbOfRegOrNull++;
              }
              idx--;
            }

            // Further optimization: if the list contains only registers or nulls, replace by POPRs or DROP
            // which are much faster.
            // For instance, replace [ 3 7 NULL 9 ] STORE by NOOP POPR9 DROP POPR7 POPR3 NOOP NOOP.
            int listLength = i - idx - 2;
            if (nbOfRegOrNull == listLength) {
              statements.set(idx, XNOOP); // replace MARK
              statements.set(i - 1, XNOOP); // replace ENDLIST
              statements.set(i, XNOOP); // replace STORE

              // Set of register numbers to detect duplicates.
              HashSet<Integer> regset = new HashSet<Integer>(listLength);

              // As we must flip the order of the list, we must store the registers first.
              int[] regInList = new int[listLength];
              for (int listIndex = listLength - 1; listIndex >= 0; listIndex--) {
                Object stmt = statements.get(idx + 1 + listIndex);
                if(stmt instanceof Long) {
                  int regno = ((Long)stmt).intValue();
                  if(regset.add(regno)) {
                    regInList[listIndex] = regno;
                  } else {
                    // The register is already used after in this list so we ignore this one, it will be
                    // replaced with a DROP. For instance [ 1 1 ] STORE will be replaced by POPR1 DROP.
                    regInList[listIndex] = - 1;
                  }
                } else {
                  regInList[listIndex] = - 1; // NULL
                }
              }

              // Replace register number by POPRs. Be careful, we flip the list order!
              for (int listIndex = 0; listIndex < listLength; listIndex++) {
                int regno = regInList[listIndex];
                if (regno < 0) {
                  statements.set(i - 2 - listIndex, DROP);
                } else {
                  POPR popr = POPRX.computeIfAbsent(regInList[listIndex], r -> new POPR(WarpScriptLib.POPR + r, r));
                  statements.set(i - 2 - listIndex, popr);
                }
              }
            }
          } else if (previousSymbol instanceof Long) {
            // Also optimize STORE on a long with POPR which is much faster
            statements.set(i - 1, XNOOP);
            POPR popr = POPRX.computeIfAbsent(((Long) previousSymbol).intValue(), r -> new POPR(WarpScriptLib.POPR + r, r));
            statements.set(i, popr);
          } else {
            abort = true;
            break;
          }
        } else if (i > 0 && currentSymbol instanceof CSTORE) {
          Object previousSymbol = statements.get(i - 1);
          if (previousSymbol instanceof String) {
            Integer regno = varregs.get(previousSymbol.toString());
            if (null != regno) {
              statements.set(i - 1, XNOOP);
              POPR cpopr = CPOPRX.computeIfAbsent(regno, r -> new POPR(WarpScriptLib.CPOPR + r, r, true));
              statements.set(i, cpopr);
            }
          } else if (previousSymbol instanceof Long) {
            // Also optimize CSTORE on a long with CPOPR which is much faster
            statements.set(i - 1, XNOOP);
            POPR cpopr = CPOPRX.computeIfAbsent(((Long) previousSymbol).intValue(), r -> new POPR(WarpScriptLib.CPOPR + r, r, true));
            statements.set(i, cpopr);
          } else {
            abort = true;
            break;
          }
        }
      }

      if (!abort) {
        List<Object> macstmt = m.statements();
        // Ignore the XNOOPs
        int xnoops = 0;
        for (int i = 0; i < statements.size(); i++) {
          if (statements.get(i) instanceof XNOOP) {
            xnoops++;
            continue;
          }
          macstmt.set(i - xnoops, statements.get(i));
        }
        m.setSize(statements.size() - xnoops);
      }
    }

    if (abort) {
      throw new WarpScriptException(getName() + " was unable to convert variables to registers.");
    }
    stack.push(top);

    return stack;
  }
}
