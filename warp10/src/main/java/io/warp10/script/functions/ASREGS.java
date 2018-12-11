//
//   Copyright 2018  SenX S.A.S.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

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
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of variable names on top of the stack.");
    }

    Object[] regs = stack.getRegisters();
    int nregs = regs.length;

    Map<String,Integer> varregs = new HashMap<String,Integer>();
    
    int regidx = 0;
    
    for (Object v: (List) top) {
      
      // Stop processing variables if we already assigned all the registers
      if (regidx >= nregs) {
        break;
      }
      
      if (!(v instanceof String)) {
        throw new WarpScriptException(getName() + " expects a list of variable names on top of the stack.");        
      }
      
      if (null == varregs.get(v.toString())) {
        varregs.put(v.toString(), regidx);
        regidx++;
      }
    }
    
    WarpScriptStackFunction[] regfuncs = new WarpScriptStackFunction[regidx * 3];
    
    for (int i = 0; i < regidx; i++) {
      regfuncs[i] = new PUSHR(WarpScriptLib.PUSHR + i, i); // LOAD
      regfuncs[regidx + i] = new POPR(WarpScriptLib.POPR + i, i); // STORE
      regfuncs[regidx + regidx + i] = new POPR(WarpScriptLib.POPR + i, i, true); // CSTORE
    }
    
    top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a macro.");
    }
    
    //
    // Now loop over the macro statement, replacing occurrences of X LOAD and X STORE by the use
    // of the assigned register
    //
    
    List<Macro> allmacros = new ArrayList<Macro>();
    allmacros.add((Macro) top);
    
    boolean abort = false;

    while(!abort && !allmacros.isEmpty()) {
      Macro m = allmacros.remove(0);
      
      List<Object> statements = new ArrayList<Object>(m.statements());
                
      for (int i = 0; i < statements.size(); i++) {
        if (statements.get(i) instanceof Macro) {
          allmacros.add((Macro) statements.get(i));
          continue;
        } else if (statements.get(i) instanceof LOAD) {
          Object symbol = statements.get(i - 1);
          if (!(symbol instanceof String)) {
            abort = true;
            break;
          }
          Integer regno = varregs.get(symbol.toString());
          if (null != regno) {
            statements.set(i - 1, NOOP);
            statements.set(i, regfuncs[regno]);
          }
        } else if (statements.get(i) instanceof STORE) {
          Object symbol = statements.get(i - 1);
          if (!(symbol instanceof String)) {
            abort = true;
            break;
          }
          Integer regno = varregs.get(symbol.toString());
          if (null != regno) {
            statements.set(i - 1, NOOP);
            statements.set(i, regfuncs[regno+regidx]);
          }        
        } else if (statements.get(i) instanceof CSTORE) {
          Object symbol = statements.get(i - 1);
          if (!(symbol instanceof String)) {
            abort = true;
            break;
          }
          Integer regno = varregs.get(symbol.toString());
          if (null != regno) {
            statements.set(i - 1, NOOP);
            statements.set(i, regfuncs[regno+regidx+regidx]);
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
