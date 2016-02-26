
package io.warp10.script.functions;

import org.python.google.common.collect.Iterables;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Return the concatenation of two ITERABLE
 * This is a constant time operation similar to APPEND,
 * but it will only work on ITERABLE.
 * @param element A List
 */
public class CONCAT extends NamedWarpScriptFunction implements WarpScriptStackFunction  {
  public CONCAT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    Object undertop = stack.pop();
    if ((!(top instanceof Iterable)) ||
        (!(undertop instanceof Iterable))) {
      throw new WarpScriptException(getName() + " expects two list on top of the stack.");
    }

    Iterable concatenated = Iterables.concat((Iterable) undertop, 
                                             (Iterable) top);
    stack.push(concatenated);
    return stack;
  }
}
