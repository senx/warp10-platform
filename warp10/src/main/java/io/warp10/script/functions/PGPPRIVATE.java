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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPSecretKeyRingCollection;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPPRIVATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public PGPPRIVATE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (top instanceof PGPPrivateKey) {
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArmoredOutputStream aout = new ArmoredOutputStream(out);
        aout.write(((PGPPrivateKey) top).getPrivateKeyDataPacket().getEncoded());
        aout.close();
        stack.push(new String(out.toByteArray(), StandardCharsets.UTF_8));
        return stack;
      } catch (Exception e) {
        throw new WarpScriptException(getName() + " encountered and error while serializing private key.", e);
      }
    }

    if (!(top instanceof String) && !(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " invalid private key, expected STRING or BYTES.");
    }

    ByteArrayInputStream in = top instanceof String ? new ByteArrayInputStream(((String) top).getBytes(StandardCharsets.UTF_8)) : new ByteArrayInputStream((byte[]) top);

    try {
      InputStream decoderstream = PGPUtil.getDecoderStream(in);
      JcaPGPSecretKeyRingCollection secretKeyRing = new JcaPGPSecretKeyRingCollection(decoderstream);
      decoderstream.close();
      Iterator<PGPSecretKeyRing> iter = secretKeyRing.getKeyRings();
      Map<Object,Object> allkeys = new LinkedHashMap<Object,Object>();
      while(iter.hasNext()) {
        PGPSecretKeyRing kr = iter.next();
        Iterator<PGPSecretKey> keys = kr.getSecretKeys();
        while(keys.hasNext()) {
          PGPSecretKey key = keys.next();
          PGPPublicKey pubkey = key.getPublicKey();

          Map<String,Object> keymap = new LinkedHashMap<String,Object>();
          keymap.put(PGPPUBLIC.KEY_KEY, key);

          String hex = "000000000000000" + Long.toHexString(key.getKeyID());
          hex = hex.substring(hex.length() - 16).toUpperCase();
          keymap.put(PGPPUBLIC.KEY_KEYID, hex);
          keymap.put(PGPPUBLIC.KEY_FINGERPRINT, Hex.toHexString(key.getPublicKey().getFingerprint()));
          keymap.put(PGPPUBLIC.KEY_BITS, null != pubkey ? pubkey.getBitStrength() : 0L);
          keymap.put(PGPPUBLIC.KEY_ALG, PGPPUBLIC.getPublicKeyAlgorithmName(key.getKeyEncryptionAlgorithm()));
          Iterator<String> useridsiter = key.getUserIDs();
          List<String> userids = new ArrayList<String>();
          while(useridsiter.hasNext()) {
            userids.add(useridsiter.next());
          }
          keymap.put(PGPPUBLIC.KEY_UID, userids);
          allkeys.put(hex, keymap);
        }
      }
      stack.push(allkeys);
    } catch (IOException|PGPException e) {
      throw new WarpScriptException(getName() + " error decoding secret key.", e);
    }
    return stack;
  }
}
