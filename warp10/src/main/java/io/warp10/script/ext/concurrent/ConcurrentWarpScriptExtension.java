package io.warp10.script.ext.concurrent;

import java.util.HashMap;
import java.util.Map;

import io.warp10.script.functions.CEVAL;
import io.warp10.script.functions.SYNC;
import io.warp10.warp.sdk.WarpScriptExtension;

/**
 * Extension which defines functions around concurrent execution
 * of WarpScript code.
 */
public class ConcurrentWarpScriptExtension extends WarpScriptExtension {
  
  private final Map<String,Object> functions;
  
  public ConcurrentWarpScriptExtension() {
    this.functions = new HashMap<String, Object>();
    functions.put("CEVAL", new CEVAL("CEVAL"));
    functions.put("SYNC", new SYNC("SYNC"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return this.functions;
  }

}
