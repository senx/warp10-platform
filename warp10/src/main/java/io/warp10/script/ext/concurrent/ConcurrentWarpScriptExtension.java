package io.warp10.script.ext.concurrent;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

/**
 * Extension which defines functions around concurrent execution
 * of WarpScript code.
 */
public class ConcurrentWarpScriptExtension extends WarpScriptExtension {
  
  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String, Object>();

    functions.put("CEVAL", new CEVAL("CEVAL",false));
    functions.put("CFOREACH", new CEVAL("FOREACH",true));
    functions.put("SYNC", new SYNC("SYNC"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }

}
