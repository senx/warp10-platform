package io.warp10.script.ext.stackps;

import java.util.HashMap;
import java.util.Map;

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.warp.sdk.WarpScriptExtension;

public class StackPSWarpScriptExtension extends WarpScriptExtension {
  /*
   *  Name of configuration key with the stackps secret. 
   */
  public static final String CONF_STACKPS_SECRET = "stackps.secret";
  
  /**
   * Current StackPS Secret
   */
  public static String STACKPS_SECRET;  

  private static final Map<String,Object> functions;
  
  static {
    WarpScriptStackRegistry.enable();
    
    STACKPS_SECRET = WarpConfig.getProperty(CONF_STACKPS_SECRET);

    functions = new HashMap<String,Object>();
    
    functions.put("STACKPSSECRET", new STACKPSSECRET("STACKPSSECRET"));
    functions.put("WSPS", new WSPS("WSPS"));
    functions.put("WSINFO", new WSINFO("WSINFO"));
    functions.put("WSKILL", new WSKILL("WSKILL"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
