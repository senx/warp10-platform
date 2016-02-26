
package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * Make a copy of an Iterable and transform it to a List.
 * @param element A List
 */
public class ITERABLETO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ITERABLETO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object iterable = stack.pop();
    if (!(iterable instanceof Iterable)) {
      throw new WarpScriptException(getName() + " expects an Iterable on top of the stack.");
    }
    List list = Lists.newArrayList((Iterable) iterable);
    stack.push(list);
    return stack;
  }
}
