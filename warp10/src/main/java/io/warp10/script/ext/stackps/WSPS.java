package io.warp10.script.ext.stackps;

import java.util.ArrayList;
import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStackRegistry;

public class WSPS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public WSPS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    //
    // A non null stackps secret was configured, check it
    //
    String secret = StackPSWarpScriptExtension.STACKPS_SECRET;
    
    if (null != secret) {
      Object top = stack.pop();
      
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a secret.");
      }
      if (!secret.equals(top)) {
        throw new WarpScriptException(getName() + " invalid secret.");
      }
    }      

    List<Object> results = new ArrayList<Object>();
    
    for (WarpScriptStack stck: WarpScriptStackRegistry.stacks()) {
      List<Object> result = new ArrayList<Object>();
      
      result.add(stck.getUUID());
      result.add(stck.getAttribute(WarpScriptStack.ATTRIBUTE_CREATION_TIME));
      result.add(stck.getAttribute(WarpScriptStack.ATTRIBUTE_NAME));
      results.add(result);
    }
    
    stack.push(results);
    
    return stack;
  }
}
