// WILL BE REMOVED BEFORE PR MERGE
//

package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;

public class TEST3 extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TEST3(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    int length = (Integer) stack.pop();

    long[] dataLong = new long[length];

    
    for (int i = 0; i < length; i++) {
      dataLong[i] = i+1;      
    }
    
    ArrayList mutableCopy = new ArrayList(length);
    for (int i = 0; i < length; i++) {
      mutableCopy.add(dataLong[i + 0]);
    }
    stack.push(mutableCopy);
      
    return stack;


  }
}
