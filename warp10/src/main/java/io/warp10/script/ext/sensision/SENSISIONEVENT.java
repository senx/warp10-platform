package io.warp10.script.ext.sensision;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.sensision.Sensision;

import java.util.List;
import java.util.Map;

/**
 * Produces a Sensision event
 */
public class SENSISIONEVENT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SENSISIONEVENT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list on top of the stack.");
    }
    
    List<Object> args = (List<Object>) top;
        
    
    if (3 == args.size()) { // class,labels,value
      String cls = args.get(0).toString();
      Map<String,String> labels = (Map<String,String>) args.get(1);
      Object value = args.get(2);
      Sensision.event(cls, labels, value);
    } else if (4 == args.size()) { // ts,class,labels,value
      long ts = ((Number) args.get(0)).longValue();
      String cls = args.get(1).toString();
      Map<String,String> labels = (Map<String,String>) args.get(2);
      Object value = args.get(3);
    } else {
      throw new WarpScriptException(getName() + " unsupported number of arguments.");
    }
    
    return stack;
  }
  
}
