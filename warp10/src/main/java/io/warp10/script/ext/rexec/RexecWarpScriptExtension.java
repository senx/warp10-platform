package io.warp10.script.ext.rexec;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class RexecWarpScriptExtension extends WarpScriptExtension {
  
  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String,Object>();

    functions.put("REXEC", new REXEC("REXEC"));
    functions.put("REXECZ", new REXEC("REXECZ", true));    
  }
  
  public java.util.Map<String,Object> getFunctions() {
    return functions;
  }
}
