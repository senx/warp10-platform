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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.bcpg.ArmoredInputStream;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPOnePassSignature;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.bc.BcPGPObjectFactory;
import org.bouncycastle.openpgp.operator.bc.BcPGPContentVerifierBuilderProvider;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPVERIFY extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public PGPVERIFY(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    boolean detached = false;

    Object top = stack.pop();

    if (top instanceof Boolean) {
      detached = Boolean.TRUE.equals(top);
      top = stack.pop();
    }

    PGPPublicKey key = null;

    if (top instanceof PGPPublicKey) {
      key = (PGPPublicKey) top;
    } else if (top instanceof Long || top instanceof String) {
      long keyid = 0L;

      if (top instanceof Long) {
        keyid = ((Long) top).longValue();
      } else if (top instanceof String) {
        byte[] decoded = Hex.decode((String) top);
        for (int i = 8; i >= 1; i--) {
          if (decoded.length - i >= 0) {
            keyid <<= 8;
            keyid |= ((long) decoded[decoded.length - i]) & 0xFFL;
          }
        }
      } else {
        throw new WarpScriptException(getName() + " missing PGP secret key id.");
      }

      top = stack.pop();

      if (top instanceof PGPSecretKeyRing) {
        key = ((PGPSecretKeyRing) top).getPublicKey(keyid);
      } else if (top instanceof PGPPublicKeyRing) {
        key = ((PGPPublicKeyRing) top).getPublicKey(keyid);
      } else {
        throw new WarpScriptException(getName() + " missing PGP key ring.");
      }

      if (null == key) {
        throw new WarpScriptException(getName() + " key with id 0x" + Long.toHexString(keyid) + " not found.");
      }
    } else {
      throw new WarpScriptException(getName() + " expected a PGP public key or key ring and key id.");
    }

    try {
      if (detached) {
        top = stack.pop();

        byte[] signature = null;

        if (top instanceof String) {
          ArmoredInputStream ais = new ArmoredInputStream(new ByteArrayInputStream(((String) top).getBytes(StandardCharsets.UTF_8)));
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          IOUtils.copyLarge(ais, out);
          signature = out.toByteArray();
        } else if (top instanceof byte[]) {
          signature = (byte[]) top;
        } else {
          throw new WarpScriptException(getName() + " expected signature data (STRING or BYTES).");
        }

        BcPGPObjectFactory pgpFact = new BcPGPObjectFactory(signature);

        PGPSignatureList sigList = (PGPSignatureList) pgpFact.nextObject();
        PGPSignature sig = sigList.get(0);

        sig.init(new BcPGPContentVerifierBuilderProvider(), key);

        top = stack.pop();

        byte[] data = null;

        if (top instanceof String) {
          data = ((String) top).getBytes(StandardCharsets.UTF_8);
        } else if (top instanceof byte[]) {
          data = (byte[]) top;
        } else {
          throw new WarpScriptException(getName() + " expected content data (STRING or BYTES).");
        }

        sig.update(data);

        stack.push(sig.verify());
      } else {
        top = stack.pop();

        byte[] data = null;

        if (top instanceof String) {
          ArmoredInputStream ais = new ArmoredInputStream(new ByteArrayInputStream(((String) top).getBytes(StandardCharsets.UTF_8)));
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          IOUtils.copyLarge(ais, out);
          data = out.toByteArray();
        } else if (top instanceof byte[]) {
          data = (byte[]) top;
        } else {
          throw new WarpScriptException(getName() + " expected signed message (STRING or BYTES).");
        }

        BcPGPObjectFactory pgpFact = new BcPGPObjectFactory(data);

        PGPOnePassSignatureList onePassList = (PGPOnePassSignatureList) pgpFact.nextObject();
        PGPOnePassSignature ops = onePassList.get(0);

        PGPLiteralData literalData = (PGPLiteralData) pgpFact.nextObject();

        InputStream dIn = literalData.getInputStream();

        ops.init(new BcPGPContentVerifierBuilderProvider(), key);

        byte[] buf = new byte[1024];

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int len;
        while ((len = dIn.read(buf)) >= 0) {
          ops.update(buf, 0, len);
          out.write(buf, 0, len);
        }

        PGPSignatureList sigList = (PGPSignatureList) pgpFact.nextObject();
        PGPSignature sig = sigList.get(0);
        stack.push(out.toByteArray());
        stack.push(ops.verify(sig));
      }
    } catch (PGPException|IOException e) {
      throw new WarpScriptException(getName() + " encountered an error while verifying signature.", e);
    }

    return stack;
  }
}
