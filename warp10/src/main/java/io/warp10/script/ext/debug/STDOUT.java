package io.warp10.script.ext.debug;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class STDOUT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public STDOUT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    System.out.println(top.toString());
    
    return stack;
  }
}
