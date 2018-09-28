package io.warp10.script.ext.debug;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class DebugWarpScriptExtension extends WarpScriptExtension {
  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String, Object>();
    
    functions.put("LOGMSG", new LOGMSG("LOGMSG"));
    functions.put("STDOUT", new STDOUT("STDOUT"));
    functions.put("STDERR", new STDERR("STDERR"));
    functions.put("NOLOG", new NOLOG("NOLOG"));
    functions.put("TDESCRIBE", new TDESCRIBE("TDESCRIBE"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
  
}
