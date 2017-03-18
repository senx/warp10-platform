//
//   Copyright 2016  Cityzen Data
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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptLoopBreakException;
import io.warp10.script.WarpScriptLoopContinueException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Implements a 'foreach' loop on a list or map
 * 
 * 2: LIST or MAP
 * 1: RUN-macro
 * FOREACH
 * 
 */
public class FOREACH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public FOREACH(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object macro = stack.pop(); // RUN-macro
    Object obj = stack.pop(); // LIST or MAP
    
    if (!WarpScriptLib.isMacro(macro)) {
      throw new WarpScriptException(getName() + " expects a macro on top of the stack.");
    }
    
    if (!(obj instanceof List) && !(obj instanceof Map) && !(obj instanceof Iterator) && !(obj instanceof Iterable)) {
      throw new WarpScriptException(getName() + " operates on a list, map, iterator or iterable.");
    }
    
    if (obj instanceof List) {
      for (Object o: ((List<Object>) obj)) {
        stack.push(o);
        //
        // Execute RUN-macro
        //        
        try {
          stack.exec((Macro) macro);
        } catch (WarpScriptLoopBreakException elbe) {
          break;
        } catch (WarpScriptLoopContinueException elbe) {
          // Do nothing!
        }
      }
    } else if (obj instanceof Map) {
      for (Entry<Object,Object> entry: ((Map<Object,Object>) obj).entrySet()) {
        stack.push(entry.getKey());
        stack.push(entry.getValue());
        try {
          stack.exec((Macro) macro);
        } catch (WarpScriptLoopBreakException elbe) {
          break;
        } catch (WarpScriptLoopContinueException elbe) {
          // Do nothing!
        }
      }
    } else if (obj instanceof Iterator || obj instanceof Iterable) {
      Iterator<Object> iter = obj instanceof Iterator ? (Iterator<Object>) obj : ((Iterable<Object>) obj).iterator();
      while(iter.hasNext()) {
        Object o = iter.next();
        stack.push(o);
        try {
          stack.exec((Macro) macro);
        } catch (WarpScriptLoopBreakException elbe) {
          break;
        } catch (WarpScriptLoopContinueException elbe) {
          // Do nothing!
        }
      }
    }

    return stack;
  }
}
