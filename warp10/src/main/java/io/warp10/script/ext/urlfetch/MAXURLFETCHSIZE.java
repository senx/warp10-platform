package io.warp10.script.ext.urlfetch;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Change the maximum cumulative downloaded size of the data fetched by URLFETCH. Value cannot exceed the hard limit.
 */
public class MAXURLFETCHSIZE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public MAXURLFETCHSIZE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (!stack.isAuthenticated()) {
      throw new WarpScriptException(getName() + " requires the stack to be authenticated.");
    }

    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a numeric (long) limit.");
    }

    long limit = ((Number) top).longValue();

    if (limit > (long) UrlFetchWarpScriptExtension.getAttribute(stack, UrlFetchWarpScriptExtension.ATTRIBUTE_URLFETCH_MAXSIZE_HARD)) {
      throw new WarpScriptException(getName() + " cannot extend limit past " + UrlFetchWarpScriptExtension.getAttribute(stack, UrlFetchWarpScriptExtension.ATTRIBUTE_URLFETCH_MAXSIZE_HARD));
    }

    stack.setAttribute(UrlFetchWarpScriptExtension.ATTRIBUTE_URLFETCH_MAXSIZE, limit);

    return stack;
  }
}
