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
import java.util.Iterator;
import java.util.List;

import org.bouncycastle.bcpg.BCPGInputStream;
import org.bouncycastle.bcpg.PacketTags;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.jcajce.JcaPGPSecretKeyRingCollection;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPRING extends NamedWarpScriptFunction implements WarpScriptStackFunction {

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


    // Determine if this is a public of secret key ring

    int tag = 0;

    try {
      BCPGInputStream bIn = new BCPGInputStream(new ByteArrayInputStream(blob));
      tag = bIn.nextPacketTag();
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " error opening key ring.");
    }

    ByteArrayInputStream in = new ByteArrayInputStream(blob);

    try {
      InputStream decoderstream = PGPUtil.getDecoderStream(in);
      if (PacketTags.SECRET_KEY == tag || PacketTags.SECRET_SUBKEY == tag) {
        JcaPGPSecretKeyRingCollection secretKeyRing = new JcaPGPSecretKeyRingCollection(decoderstream);
        decoderstream.close();
        Iterator<PGPSecretKeyRing> iter = secretKeyRing.getKeyRings();
        List<PGPSecretKeyRing> rings = new ArrayList<PGPSecretKeyRing>();
        while(iter.hasNext()) {
          PGPSecretKeyRing kr = iter.next();
          rings.add(kr);
        }
        stack.push(rings);
      } else if (PacketTags.PUBLIC_KEY == tag || PacketTags.PUBLIC_SUBKEY == tag) {
        JcaPGPPublicKeyRingCollection publicKeyRing = new JcaPGPPublicKeyRingCollection(decoderstream);
        decoderstream.close();
        Iterator<PGPPublicKeyRing> iter = publicKeyRing.getKeyRings();
        List<PGPPublicKeyRing> rings = new ArrayList<PGPPublicKeyRing>();
        while(iter.hasNext()) {
          PGPPublicKeyRing kr = iter.next();
          rings.add(kr);
        }
        stack.push(rings);
      } else {
        throw new WarpScriptException(getName() + " invalid key ring.");
      }
    } catch (IOException|PGPException e) {
      throw new WarpScriptException(getName() + " error decoding PGP key ring.", e);
    }
    return stack;
  }
}
