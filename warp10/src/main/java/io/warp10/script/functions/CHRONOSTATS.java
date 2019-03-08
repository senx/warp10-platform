package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CHRONOSTATS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String key = "chronostats";

  public CHRONOSTATS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Map<String, AtomicLong[]> stats = (Map<String, AtomicLong[]>) stack.getAttribute(key);

    if (null == stats) {
      stack.push(null);
    } else {
      // Get the map containing all the results and transform it for easier understanding, getting the long values of AtomicLongs
      HashMap<String, HashMap<String, Long>> result = new HashMap<String, HashMap<String, Long>>();
      for (Map.Entry<String, AtomicLong[]> entry: stats.entrySet()) {
        HashMap<String, Long> macro_result = new HashMap<String, Long>();
        macro_result.put("total_time", entry.getValue()[0].longValue());
        macro_result.put("total_calls", entry.getValue()[1].longValue());
        result.put(entry.getKey(), macro_result);
      }
      stack.push(result);
    }

    return stack;
  }

  // Attributes keys. Use stack ID to avoid collision in MT environment
  static String getStartKey(String alias, WarpScriptStack stack) {
    return "chrono_start_" + alias + "_" + stack.getUUID();
  }

  // Attributes keys. Use stack ID to avoid collision in MT environment
  static String getActiveCountKey(String alias, WarpScriptStack stack) {
    return "chrono_active_" + alias + "_" + stack.getUUID();
  }

  // Attributes keys. Use stack ID to avoid collision in MT environment
  static String getTotalCountKey(String alias, WarpScriptStack stack) {
    return "chrono_calls_" + alias + "_" + stack.getUUID();
  }
}
