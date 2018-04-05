package io.warp10.script.ext.inventory;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStackFunction;


public class WARP10FUNCTIONS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public WARP10FUNCTIONS(String name) {
    super(name);
  }

  public Object apply(WarpScriptStack stack) throws WarpScriptException {


    //
    // Apply function and push its outputs onto the stack or raise an exception
    //

    try {
      stack.push(WarpScriptLib.getListOfRegisteredFunctions());

    } catch (WarpScriptException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new WarpScriptException("Stack exception" + e.getMessage() + e.getStackTrace().toString());
    }

    //
    // Return the new state of the stack
    //

    return stack;
  }
}
