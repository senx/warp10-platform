package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class TIMED extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TIMED(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o1 = stack.pop();
    String alias = o1.toString();

    Object o2 = stack.pop();

    if (!(o2 instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a macro under the top of the stack which will be timed.");
    }

    Macro macro = (Macro) o2;

    // Encapsulate the CHRONOEND function in a macro to use it as a finally block.
    // Make TIMED resilient to STOP, RETURN and any exception.
    Macro finallyMacro = new Macro();
    finallyMacro.add(alias);
    finallyMacro.add(WarpScriptLib.getFunction(WarpScriptLib.CHRONOEND));

    // Build a new macro starting with CHRONOSTART, calling the given macro and ending with CHRONOEND in the finally block of a TRY.
    Macro timedMacro = new Macro();
    timedMacro.add(alias);
    timedMacro.add(WarpScriptLib.getFunction(WarpScriptLib.CHRONOSTART));
    timedMacro.add(macro);        // Try
    timedMacro.add(new Macro());  // Catch
    timedMacro.add(finallyMacro); // Finally
    timedMacro.add(WarpScriptLib.getFunction(WarpScriptLib.TRY));

    stack.push(timedMacro);

    return stack;
  }
}
