package io.warp10.script.ext.stackps;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStackRegistry;

public class WSKILL extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public WSKILL(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    //
    // A non null stackps secret was configured, check it
    //
    String secret = StackPSWarpScriptExtension.STACKPS_SECRET;
    
    if (null != secret) {     
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a secret.");
      }
      if (!secret.equals(top)) {
        throw new WarpScriptException(getName() + " invalid secret.");
      }
      top = stack.pop();
    }      

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a UUID.");
    }
    
    stack.push(WarpScriptStackRegistry.abort(top.toString()));
        
    return stack;
  }
}
