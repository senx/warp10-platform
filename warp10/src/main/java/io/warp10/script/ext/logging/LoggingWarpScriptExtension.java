package io.warp10.script.ext.logging;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class LoggingWarpScriptExtension extends WarpScriptExtension {
  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String, Object>();
    
    functions.put("LOGEVENT->", new LOGEVENTTO("LOGEVENT->"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
  
}
