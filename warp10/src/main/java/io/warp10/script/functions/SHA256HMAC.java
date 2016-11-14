//
//   Copyright 2016  Cityzen Data
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

package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;

/**
 * Compute a SipHash of its input converted to String
 */
public class SHA256HMAC extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  /**
   * HHash MAC of a byte array with the cryptographic hash function SHA-256  and a given key
   */
  public SHA256HMAC(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " expects a byte array (key) on top of the stack.");
    }

    byte[] key = (byte[]) top;

    top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }

    byte[] bytes = (byte[]) top;

    HMac m=new HMac(new SHA256Digest());

    m.init(new KeyParameter(key));

    m.update(bytes,0,bytes.length);

    byte[] mac=new byte[m.getMacSize()];

    m.doFinal(mac,0);

    stack.push(mac);

    return stack;
  }
}
