package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class CHRONO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public CHRONO(String name) {
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

    Macro finallyMacro = new Macro();
    finallyMacro.add(alias);
    finallyMacro.add(WarpScriptLib.getFunction(WarpScriptLib.CHRONOEND));

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
