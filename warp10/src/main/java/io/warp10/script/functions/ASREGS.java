//
//   Copyright 2019-2020  SenX S.A.S.
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
 * Modifies a Macro so the LOAD/STORE operations for the given variables are
 * replaced by use of registers
 */
public class ASREGS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final NOOP NOOP = new NOOP(WarpScriptLib.NOOP);
  
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
        vars = VARS.getVars(macro);
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
      
      List<Object> statements = new ArrayList<Object>(m.statements());
                
      for (int i = 0; i < statements.size(); i++) {
        if (statements.get(i) instanceof PUSHR) {
          inuse.set(((PUSHR) statements.get(i)).getRegister());
        } else if (statements.get(i) instanceof POPR) {
          inuse.set(((POPR) statements.get(i)).getRegister());
        } else if (statements.get(i) instanceof LOAD || statements.get(i) instanceof CSTORE) {
          // If the statement is the first, we cannot determine if what
          // we load or update is a register or a variable, so abort.
          if (0 == i) {
            abort = true;
            break;
          }
          // Fetch what precedes the LOAD/CSTORE
          Object symbol = statements.get(i - 1);
          if (symbol instanceof Long) {
            inuse.set(((Long) symbol).intValue());
          } else if (!(symbol instanceof String)) {
            // We encountered a LOAD/CSTORE with a non string and non long param,
            // we cannot determine what is being loaded or updated, so no replacement
            // can occur
            abort = true;
          }
        } else if (statements.get(i) instanceof STORE) {
          // If the statement is the first, we cannot determine if what
          // we load is a register or a variable, so abort.
          if (0 == i) {
            abort = true;
            break;
          }
          // Fetch what precedes the STORE
          Object symbol = statements.get(i - 1);
          if (symbol instanceof Long) {
            inuse.set(((Long) symbol).intValue());
          } else if (symbol instanceof List) {
            // We inspect the list, looking for registers
            for (Object elt: (List) symbol) {
              if (elt instanceof Long) {
                inuse.set(((Long) elt).intValue());
              }
            }
          } else if (symbol instanceof ENDLIST) {
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
          } else if (!(symbol instanceof String)) {
            // We encountered a STORE with something that is neither a register, a string or
            // a list, so we cannot determine if a register is involved or not, so we abort
            abort = true;
          }
        } else if (statements.get(i) instanceof Macro) {
          allmacros.add((Macro) statements.get(i));
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

    HashMap<Integer, PUSHR> PUSHRs = new HashMap<Integer, PUSHR>();
    HashMap<Integer, POPR> POPRs = new HashMap<Integer, POPR>();
    HashMap<Integer, POPR> CPOPRs = new HashMap<Integer, POPR>();

    //
    // Now loop over the macro statement, replacing occurrences of X LOAD and X STORE by the use
    // of the assigned register
    //
    
    allmacros.clear();
    allmacros.add((Macro) top);
    
    while(!abort && !allmacros.isEmpty()) {
      Macro m = allmacros.remove(0);
      
      List<Object> statements = new ArrayList<Object>(m.statements());
                
      for (int i = 0; i < statements.size(); i++) {
        if (statements.get(i) instanceof Macro) {
          allmacros.add((Macro) statements.get(i));
          continue;
        } else if (i > 0 && statements.get(i) instanceof LOAD) {
          Object symbol = statements.get(i - 1);
          
          if (symbol instanceof String) {
            Integer regno = varregs.get(symbol.toString());
            if (null != regno) {
              statements.set(i - 1, NOOP);
              PUSHR pushr = PUSHRs.computeIfAbsent(regno, r -> new PUSHR("PUSHR" + r, r));
              statements.set(i, pushr);
            }
          } else if (symbol instanceof Long) {
            // Also optimize LOAD on a long with PUSHR which is much faster
            statements.set(i - 1, NOOP);
            PUSHR pushr = PUSHRs.computeIfAbsent(((Long) symbol).intValue(), r -> new PUSHR("PUSHR" + r, r));
            statements.set(i, pushr);
          } else {
            abort = true;
            break;
          }
        } else if (i > 0 && statements.get(i) instanceof STORE) {
          Object symbol = statements.get(i - 1);
          if (symbol instanceof String) {
            Integer regno = varregs.get(symbol.toString());
            if (null != regno) {
              statements.set(i - 1, NOOP);
              POPR popr = POPRs.computeIfAbsent(regno, r -> new POPR("POPR" + r, r));
              statements.set(i, popr);
            }
          } else if (symbol instanceof List) {
            for (int k = 0; k < ((List) symbol).size(); k++) {
              if (((List) symbol).get(k) instanceof String) {
                Integer regno = varregs.get(((List) symbol).get(k).toString());
                if (null != regno) {
                  ((List) symbol).set(k, (long) regno);
                }
              }
            }
          } else if (symbol instanceof ENDLIST) {
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
              statements.set(idx, NOOP); // replace MARK
              statements.set(i - 1, NOOP); // replace ENDLIST
              statements.set(i, NOOP); // replace STORE

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
                    // replaced with a DROP.
                    regInList[listIndex] = - 1;
                  }
                } else {
                  regInList[listIndex] = - 1; // NULL
                }
              }

              // DROP used for NULL or duplicate registers.
              // [ 1 1 ] STORE will be replaced by POPR1 DROP
              DROP drop = new DROP("DROP");

              // Replace register number by POPRs. Be careful, we flip the list order!
              for (int listIndex = 0; listIndex < listLength; listIndex++) {
                int regno = regInList[listIndex];
                if (regno < 0) {
                  statements.set(i - 2 - listIndex, drop);
                } else {
                  POPR popr = POPRs.computeIfAbsent(regInList[listIndex], r -> new POPR("POPR" + r, r));
                  statements.set(i - 2 - listIndex, popr);
                }
              }
            }
          } else if (symbol instanceof Long) {
            // Also optimize STORE on a long with POPR which is much faster
            statements.set(i - 1, NOOP);
            POPR popr = POPRs.computeIfAbsent(((Long) symbol).intValue(), r -> new POPR("POPR" + r, r));
            statements.set(i, popr);
          } else {
            abort = true;
            break;
          }
        } else if (i > 0 && statements.get(i) instanceof CSTORE) {
          Object symbol = statements.get(i - 1);
          if (symbol instanceof String) {
            Integer regno = varregs.get(symbol.toString());
            if (null != regno) {
              statements.set(i - 1, NOOP);
              POPR cpopr = CPOPRs.computeIfAbsent(regno, r -> new POPR("CPOPR" + r, r));
              statements.set(i, cpopr);
            }
          } else if (symbol instanceof Long) {
            // Also optimize CSTORE on a long with CPOPR which is much faster
            statements.set(i - 1, NOOP);
            POPR cpopr = CPOPRs.computeIfAbsent(((Long) symbol).intValue(), r -> new POPR("CPOPR" + r, r));
            statements.set(i, cpopr);
          } else {
            abort = true;
            break;
          }
        }
      }      
      
      if (!abort) {
        List<Object> macstmt = m.statements();
        // Ignore the NOOPs
        int noops = 0;
        for (int i = 0; i < statements.size(); i++) {
          if (statements.get(i) instanceof NOOP) {
            noops++;
            continue;
          }
          macstmt.set(i - noops, statements.get(i));
        }
        m.setSize(statements.size() - noops);
      }
    }

    if (abort) {
      throw new WarpScriptException(getName() + " was unable to convert variables to registers.");
    }
    stack.push(top);
    
    return stack;
  }
}
