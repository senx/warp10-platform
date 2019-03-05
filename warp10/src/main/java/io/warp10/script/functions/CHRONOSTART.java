package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.concurrent.atomic.AtomicLong;

public class CHRONOSTART extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public CHRONOSTART(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o1 = stack.pop();
    String alias = o1.toString();

    String keyStart = CHRONOSTATS.getStartKey(alias, stack);
    String keyActiveCount = CHRONOSTATS.getActiveCountKey(alias, stack);
    String keyTotalCount = CHRONOSTATS.getTotalCountKey(alias, stack);

    // Increase the active count for this alias
    AtomicLong activeCount = (AtomicLong) stack.getAttribute(keyActiveCount);
    if(null == activeCount){
      activeCount = new AtomicLong();
      stack.setAttribute(keyActiveCount, activeCount);
    }
    activeCount.incrementAndGet();

    // Increase the total count for this alias
    AtomicLong totalCount = (AtomicLong) stack.getAttribute(keyTotalCount);
    if(null == totalCount){
      totalCount = new AtomicLong();
      stack.setAttribute(keyTotalCount, totalCount);
    }
    totalCount.incrementAndGet();

    // Keep start nano only if first start. Takes care of recursivity.
    if (1 == activeCount.intValue()) {
      stack.setAttribute(keyStart, new AtomicLong(System.nanoTime()));
    }

    return stack;
  }
}
