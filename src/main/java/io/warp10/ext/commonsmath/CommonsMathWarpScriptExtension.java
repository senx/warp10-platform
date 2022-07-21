package io.warp10.ext.commonsmath;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class CommonsMathWarpScriptExtension extends WarpScriptExtension {
  
  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String,Object>();
    
    functions.put("MICROSPHEREFIT", new MICROSPHEREFIT("MICROSPHEREFIT"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
