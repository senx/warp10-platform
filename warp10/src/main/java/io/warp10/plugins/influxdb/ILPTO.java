package io.warp10.plugins.influxdb;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class ILPTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ILPTO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a STRING.");
    }

    return stack;
  }

}
