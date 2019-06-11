package io.warp10.script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ElementOrListStackFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  @FunctionalInterface
  public interface ElementStackFunction {
    Object applyOnElement(Object element) throws WarpScriptException;
  }

  public abstract ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException;

  public ElementOrListStackFunction(String name) {
    super(name);
  }

  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    ElementStackFunction function = generateFunction(stack);

    Object o = stack.pop();

    if (o instanceof List) {
      List list = (List) o;
      ArrayList<Object> result = new ArrayList<Object>(list.size());

      for (Object element: list) {
        result.add(function.applyOnElement(element));
      }

      stack.push(result);
    } else {
      stack.push(function.applyOnElement(o));
    }

    return stack;
  }
}

