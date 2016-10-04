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

package io.warp10.script;

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptStack.Macro;

import org.apache.hadoop.util.Progressable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class WarpScriptExecutor {

  private final Progressable progressable;
  
  public static enum StackSemantics {
    // Single stack which will be shared among calling threads
    SYNCHRONIZED,
    // One stack per thread
    PERTHREAD,
    // A new stack for every execution
    NEW,
  };

  /**
   * Macro this executor will tirelessly apply
   */
  private final Macro macro;

  /**
   * Single stack to use when StackSemantics is SYNCHRONIZED, null otherwise
   */
  private final WarpScriptStack stack;
  
  /**
   * Semaphore for synchronizing threads
   */
  private final Semaphore sem;

  private final StackSemantics semantics;
  
  private final Map<String,Object> symbolTable;  
  
  private static final Properties properties;
  
  private ThreadLocal<WarpScriptStack> perThreadStack = new ThreadLocal<WarpScriptStack>() {
    @Override
    protected WarpScriptStack initialValue() {
      MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, null, properties);
      stack.maxLimits();
      if (null != progressable) {
        stack.setAttribute(WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE, progressable);
      }
      try {
        stack.exec(WarpScriptLib.BOOTSTRAP);
      } catch (WarpScriptException e) {
        throw new RuntimeException(e);
      }

      return stack;
    }
  };

  static {
    //
    // Initialize WarpConfig
    //
  
    try {
      WarpConfig.safeSetProperties((String) System.getProperty(WARP10_CONFIG));
      properties = WarpConfig.getProperties();
      WarpScriptLib.registerExtensions();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public WarpScriptExecutor(StackSemantics semantics, String script) throws WarpScriptException {
    this(semantics, script, null);
  }

  public WarpScriptExecutor(StackSemantics semantics, String script, Map<String,Object> symbols) throws WarpScriptException {
    this(semantics,script,symbols,null);
  }

  public WarpScriptExecutor(StackSemantics semantics, String script, Map<String,Object> symbols, Progressable progressable) throws WarpScriptException {

    this.semantics = semantics;
    this.progressable = progressable;
    
    if (StackSemantics.SYNCHRONIZED.equals(semantics)) {
      this.stack = perThreadStack.get();
      if (null != this.progressable) {
        this.stack.setAttribute(WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE, progressable);
      }
      this.sem = new Semaphore(1);
    } else {
      this.stack = null;
      this.sem = new Semaphore(Integer.MAX_VALUE);
    }
    
    //
    // Attempt to execute the script code on the stack
    // to define a macro
    //
      
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, null, new Properties());
    stack.maxLimits();
    if (null != this.progressable) {
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE, this.progressable);
    }
    
    try {
      stack.exec(WarpScriptLib.BOOTSTRAP);
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }

    stack.exec(WarpScriptLib.BOOTSTRAP);

    this.symbolTable = new HashMap<String,Object>();
    
    if (null != symbols) {
      this.symbolTable.putAll(symbols);
    }
        
    StringBuilder sb = new StringBuilder();
    sb.append(WarpScriptStack.MACRO_START);
    sb.append("\n");
    sb.append(script);
    sb.append("\n");
    sb.append(WarpScriptStack.MACRO_END);
    
    stack.execMulti(sb.toString());
      
    if (1 != stack.depth()) {
      throw new WarpScriptException("Stack depth was not 1 after the code execution.");
    }
    
    if (!(stack.peek() instanceof Macro)) {
      throw new WarpScriptException("No macro was found on top of the stack.");
    }
    
    Macro macro = (Macro) stack.pop();
    
    this.macro = macro;    
  }

  /**
   * Execute the embedded macro on the given stack content.
   * 
   * @param input Stack content to push on the stack, index 0 is the top of the stack.
   * @return Return the state of the stack post execution, index 0 is the top of the stack.
   * @throws WarpScriptException
   */
  public List<Object> exec(List<Object> input) throws WarpScriptException {
    try {
      this.sem.acquire();
    } catch (InterruptedException ie) {
      throw new WarpScriptException("Got interrupted while attempting to acquire semaphore.");
    }
    
    WarpScriptStack stack = this.stack;

    try {
      if (null == stack) {
        if (StackSemantics.PERTHREAD.equals(this.semantics)) {
          stack = perThreadStack.get();
        } else if (StackSemantics.NEW.equals(this.semantics)) {
          stack = new MemoryWarpScriptStack(null, null, null, properties);
          ((MemoryWarpScriptStack) stack).maxLimits();
          if (null != this.progressable) {
            stack.setAttribute(WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE, this.progressable);
          }
          try {
            stack.exec(WarpScriptLib.BOOTSTRAP);
          } catch (WarpScriptException e) {
            throw new RuntimeException(e);
          }
        } else {
          throw new WarpScriptException("Invalid stack semantics.");
        }
      }
      
      //
      // Update the symbol table if 'symbolTable' is set
      //
      
      if (null != this.symbolTable) {
        stack.save();
        MemoryWarpScriptStack.StackContext context = (MemoryWarpScriptStack.StackContext) stack.peek();
        context.symbolTable.putAll(this.symbolTable);
        stack.restore();
      }
      
      //
      // Push the parameters onto the stack
      //
      
      for (int i = input.size() - 1; i >= 0; i--) {
        stack.push(input.get(i));
      }
      
      //
      // Execute the macro
      //
      
      stack.exec(this.macro);
      
      //
      // Pop the output off the stack
      //
      
      List<Object> output = new ArrayList<Object>();
      
      while(stack.depth() > 0) {
        output.add(stack.pop());
      }
      
      return output;
    } finally {
      if (null != stack) {
        // Clear the stack
        stack.clear();
      }
      this.sem.release();
    }
  }
  
  /**
   * Store a symbol in the symbol table.
   */
  public WarpScriptExecutor store(String key, Object value) {
    this.symbolTable.put(key, value);
    return this;
  }
}
