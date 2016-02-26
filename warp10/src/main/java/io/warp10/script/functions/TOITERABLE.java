
package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.List;

/**
 * Make an Iterable List from a List
 * @param element A List
 */
public class TOITERABLE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOITERABLE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object list = stack.pop();
    if (!(list instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list on top of the stack.");
    }
    stack.push((Iterable) list);
    return stack;
  }
}
