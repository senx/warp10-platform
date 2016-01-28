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
import io.warp10.script.WarpScriptStack;

import java.util.Map.Entry;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.renjin.eval.Session;
import org.renjin.eval.SessionBuilder;
import org.renjin.script.RenjinScriptEngineFactory;
import org.renjin.sexp.Symbol;

public class R extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public R(String name) {
    super(name);    
  }
   
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    final RenjinScriptEngineFactory factory = new RenjinScriptEngineFactory();
    // FIXME(hbs): should we build one without base packages
    Session session = new SessionBuilder().withDefaultPackages().build();
    
    final ScriptEngine engine = factory.getScriptEngine(session);
    
    String script = stack.pop().toString();
    
    try {    
      //
      // Copy symbol table from stack
      //
      
      for (Entry<String,Object> entry: stack.getSymbolTable().entrySet()) {
        if (entry.getValue().getClass().getCanonicalName().startsWith("org.renjin.sexp")) {
          engine.put(entry.getKey(), entry.getValue());
        }
      }
      
      Object result = engine.eval(script);

      for (Symbol symbol: session.getTopLevelContext().getEnvironment().getSymbolNames()) {
        stack.getSymbolTable().put(symbol.getPrintName(), engine.get(symbol.getPrintName()));
      }
      
      stack.push(result);
    } catch (ScriptException se) {
      throw new WarpScriptException(se);
    }
    
    return stack;
  }

}
