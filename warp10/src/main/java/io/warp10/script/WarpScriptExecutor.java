package io.warp10.script;

import io.warp10.WarpDist;
import io.warp10.script.WarpScriptStack.Macro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class WarpScriptExecutor {
  
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
  
  private ThreadLocal<WarpScriptStack> perThreadStack = new ThreadLocal<WarpScriptStack>() {
    @Override
    protected WarpScriptStack initialValue() {
      MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, null, WarpDist.getProperties());
      
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
    // Initialize WarpDist
    //
  
    try {
      WarpDist.setProperties((String) null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public WarpScriptExecutor(StackSemantics semantics, String script) throws WarpScriptException {
    this(semantics, script, null);
  }
  
  public WarpScriptExecutor(StackSemantics semantics, String script, Map<String,Object> symbols) throws WarpScriptException {

    this.semantics = semantics;
    
    if (StackSemantics.SYNCHRONIZED.equals(semantics)) {
      this.stack = perThreadStack.get();
      this.sem = new Semaphore(1);
    } else {
      this.stack = null;
      this.sem = new Semaphore(Integer.MAX_VALUE);
    }
    
    //
    // Attempt to execute the script code on the stack
    // to define a macro
    //
      
    WarpScriptStack stack = new MemoryWarpScriptStack(null, null, null, new Properties());
    stack.exec(WarpScriptLib.BOOTSTRAP);

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
          stack = new MemoryWarpScriptStack(null, null, null, WarpDist.getProperties());
          stack.exec(WarpScriptLib.BOOTSTRAP);
        } else {
          throw new WarpScriptException("Invalid stack semantics.");
        }
      }
      
      //
      // Push the parameters onto the stack
      //
      
      for (Object o: input) {
        stack.push(o);
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
      
      java.util.Collections.reverse(output);
      
      return output;
    } finally {
      // Clear the stack
      stack.clear();
      this.sem.release();
    }
  }
}
