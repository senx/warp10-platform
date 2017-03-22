package io.warp10.script.ext.logging;

import io.warp10.WarpDist;
import io.warp10.continuum.LogUtil;
import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.KeyStore;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class LOGEVENTTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private byte[] aeskey = null;
  
  public LOGEVENTTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (null == aeskey) {
      aeskey = WarpDist.getKeyStore().getKey(KeyStore.AES_LOGGING);
      if (null == aeskey) {
        throw new WarpScriptException(getName() + " logging key not set.");
      }
    }
    
    LoggingEvent event = LogUtil.unwrapLog(aeskey, top.toString());

    stack.push(event.getAttributes());
    
    return stack;
  }
}
