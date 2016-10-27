package io.warp10.script.ext.sensision;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

/**
 * WarpScript Extension which exposes functions to
 * set/update/clear/get Sensision metrics
 */
public class SensisionWarpScriptExtension extends WarpScriptExtension {
  
  private static final Map<String, Object> functions;
  
  static {
    functions = new HashMap<String, Object>();
    
    functions.put("SENSISION.EVENT", new SENSISIONEVENT("SENSISION.EVENT"));
    functions.put("SENSISION.UPDATE", new SENSISIONUPDATE("SENSISION.UPDATE"));
    functions.put("SENSISION.SET", new SENSISIONSET("SENSISION.SET"));
    functions.put("SENSISION.GET", new SENSISIONGET("SENSISION.GET"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
