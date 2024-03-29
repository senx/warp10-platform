//
//   Copyright 2018-2023  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
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
    if (event != null) {
      stack.push(event.getAttributes());
    } else {
      throw new WarpScriptException(this.getName() + " cannot decode the input.");
    }
    return stack;
  }
}
