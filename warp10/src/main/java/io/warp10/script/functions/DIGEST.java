//
//   Copyright 2018-2021  SenX S.A.S.
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

import java.lang.reflect.Constructor;

import org.bouncycastle.crypto.Digest;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class DIGEST extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private Class digestAlgo;

  private Integer size = null;

  public DIGEST(String name, Class<? extends Digest> digestAlgo) {
    super(name);
    this.digestAlgo = digestAlgo;
  }

  public DIGEST(String name, Class<? extends Digest> digestAlgo, int size) {
    super(name);
    this.digestAlgo = digestAlgo;
    this.size = size;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }

    byte[] bytes = (byte[]) o;

    try {
      Digest digest;

      if (null == this.size) {
        digest = (Digest) digestAlgo.newInstance();
      } else {
        Constructor c = digestAlgo.getConstructor(new Class[] { int.class });
        digest = (Digest) c.newInstance(this.size);
      }

      byte[] digestOctets = new byte[digest.getDigestSize()];

      digest.update(bytes, 0, bytes.length);

      digest.doFinal(digestOctets, 0);

      stack.push(digestOctets);

      return stack;
    } catch (Exception exp) {
      throw new WarpScriptException(getName() + " unable to instantiate message digest.", exp);
    }
  }
}
