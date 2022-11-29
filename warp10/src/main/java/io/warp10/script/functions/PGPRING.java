//
//   Copyright 2022  SenX S.A.S.
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPRING extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String TYPEOF_KEYRING = "KEYRING";

  static {
    TYPEOF.addResolver(new TYPEOF.TypeResolver() {
      @Override
      public String typeof(Class c) {
        if (c.isAssignableFrom(PGPSecretKeyRing.class) || c.isAssignableFrom(PGPPublicKeyRing.class)) {
          return TYPEOF_KEYRING;
        } else {
          return null;
        }
      }
    });
  }

  public PGPRING(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (top instanceof PGPSecretKeyRing) {
      try {
        stack.push(((PGPSecretKeyRing) top).getEncoded());
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " error while encoding PGP secret key ring.");
      }
      return stack;
    } else if (top instanceof PGPPublicKeyRing) {
      try {
        stack.push(((PGPPublicKeyRing) top).getEncoded());
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " error while encoding PGP public key ring.");
      }
      return stack;
    }

    if (!(top instanceof String) && !(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " invalid PGP ring, expected STRING or BYTES.");
    }

    byte[] blob = top instanceof String ? ((String) top).getBytes(StandardCharsets.UTF_8) : (byte[]) top;
    InputStream in = new ByteArrayInputStream(blob);

    try {
      InputStream decoderstream = PGPUtil.getDecoderStream(in);
      List<Object> rings = new ArrayList<Object>();

      PGPObjectFactory pgpFact = new PGPObjectFactory(decoderstream, new JcaKeyFingerprintCalculator());
      Object obj;

      while ((obj = pgpFact.nextObject()) != null) {
        if (obj instanceof PGPSecretKeyRing || obj instanceof PGPPublicKeyRing) {
          rings.add(obj);
        }
      }

      decoderstream.close();

      stack.push(rings);
    } catch (IOException e) {
      throw new WarpScriptException(getName() + " error decoding PGP key ring.", e);
    }
    return stack;
  }
}
