// WILL BE REMOVED BEFORE PR MERGE
//

package io.warp10.script.functions;

import io.warp10.continuum.gts.COWList;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.BitSet;

public class TEST extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TEST(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    long[] array = {0, 1, 2, 3, 4, 5, 6};
    double[] arrayd = {0.0, 1.0, 2.0, 3.14, 4.0, 5.0, 6.0};
    String[] arrays = {"00", "01", "02", "03", "04", "05", "06"};
    BitSet bs = new BitSet();
    bs.set(0);
    bs.set(1);
    bs.set(2);
    bs.set(3);
    bs.clear(4);
    bs.set(5);
    bs.set(6);

    int len = ((Long) stack.pop()).intValue();
    int startidx = ((Long) stack.pop()).intValue();
    stack.push(new COWList(array, startidx, len));
    stack.push(new COWList(arrayd, startidx, len));
    stack.push(new COWList(arrays, startidx, len));
    stack.push(new COWList(bs, startidx, len));
    return stack;
  }
}
