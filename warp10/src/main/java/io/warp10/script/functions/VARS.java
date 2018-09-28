//
//   Copyright 2018  Cityzen Data
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

/**
 * Extract all used variables in a macro. If a STORE/CSTORE or LOAD operation is
 * found with a parameter which is not a string, then an error is raised.
 */
public class VARS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public VARS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a macro.");
    }
    
    //
    // Now loop over the macro statement, extracting variable names
    //
    
    Set<String> symbols = new LinkedHashSet<String>();
    
    final Map<String,AtomicInteger> occurrences = new HashMap<String,AtomicInteger>();
    
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
        } else if (statements.get(i) instanceof LOAD|| statements.get(i) instanceof STORE|| statements.get(i) instanceof CSTORE) {
          Object symbol = statements.get(i - 1);
          // If the parameter to LOAD/STORE/CSTORE is not a string, then we cannot extract
          // the variables in a safe way as some may be unknown to us (as their name may be the result
          // of a computation), so in this case we abort the process
          if (!(symbol instanceof String)) {
            abort = true;
            break;
          }
          symbols.add(symbol.toString());
          AtomicInteger occ = occurrences.get(symbol.toString());
          if (null == occ) {
            occ = new AtomicInteger();
            occurrences.put(symbol.toString(), occ);
          }
          occ.incrementAndGet();
        }
      }            
    }
    
    if (abort) {
      throw new WarpScriptException(getName() + " encountered a LOAD/STORE or CSTORE operation with a non explicit symbol name.");
    }
    
    List<String> vars = new ArrayList<String>(symbols);
    
    // Now sort according to the number of occurrences (decreasing)
    
    vars.sort(new Comparator<String>() {
      @Override
      public int compare(String s1, String s2) {
        AtomicInteger occ1 = occurrences.get(s1);
        AtomicInteger occ2 = occurrences.get(s2);
        
        if (occ1.get() < occ2.get()) {
          return 1;
        } else if (occ1.get() > occ2.get()) {
          return -1;
        } else {
          // Compare the strings
          return s1.compareTo(s2);
        }
      }
    });
        
    stack.push(vars);
    
    return stack;
  }
}
