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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bouncycastle.bcpg.attr.ImageAttribute;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPUserAttributeSubpacketVector;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPINFO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String KEY_MASTER = "master";
  public static final String KEY_SIGNING = "signing";
  public static final String KEY_ENCRYPTION = "encryption";
  public static final String KEY_EXPIRY = "expiry";
  public static final String KEY_PUBKEY = "pubkey";
  public static final String KEY_ATTR = "attributes";

  public PGPINFO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (top instanceof PGPSecretKeyRing || top instanceof PGPPublicKeyRing) {

      Map<Object,Object> keys = new LinkedHashMap<Object,Object>();
      Iterator<PGPPublicKey> pubiter = null;

      if (top instanceof PGPSecretKeyRing) {
        PGPSecretKeyRing keyring = (PGPSecretKeyRing) top;


        Iterator<PGPSecretKey> iter = keyring.getSecretKeys();
        while(iter.hasNext()) {
          PGPSecretKey key = iter.next();

          Map<Object,Object> infos = new LinkedHashMap<Object,Object>();

          String keyid = "000000000000000" + Long.toHexString(key.getKeyID());
          keyid = keyid.substring(keyid.length() - 16, keyid.length()).toUpperCase();
          infos.put(PGPPUBLIC.KEY_KEYID, keyid);
          infos.put(PGPPUBLIC.KEY_FINGERPRINT, Hex.toHexString(key.getPublicKey().getFingerprint()));
          List<byte[]> uids = new ArrayList<byte[]>();
          // We retrieve the raw user ids because we cannot be certain they contain valid UTF-8
          Iterator<byte[]> uiditer = key.getPublicKey().getRawUserIDs();
          while(uiditer.hasNext()) {
            uids.add(uiditer.next());
          }
          infos.put(PGPPUBLIC.KEY_UID, uids);
          infos.put(PGPPUBLIC.KEY_BITS, (long) key.getPublicKey().getBitStrength());
          infos.put(PGPPUBLIC.KEY_ALG, PGPPUBLIC.getPublicKeyAlgorithmName(key.getPublicKey().getAlgorithm()));
          infos.put(KEY_MASTER, key.isMasterKey());
          infos.put(KEY_SIGNING, key.isSigningKey());
          long creation = key.getPublicKey().getCreationTime().getTime();
          infos.put(KEY_ENCRYPTION, key.getPublicKey().isEncryptionKey());
          infos.put(KEY_EXPIRY, 0 == key.getPublicKey().getValidSeconds() ? 0L : (creation + key.getPublicKey().getValidSeconds() * 1000L) * Constants.TIME_UNITS_PER_MS);
          infos.put(KEY_PUBKEY, key.getPublicKey());
          Iterator<PGPUserAttributeSubpacketVector> ater = key.getUserAttributes();
          List<byte[]> attributes = new ArrayList<byte[]>();
          while(ater.hasNext()) {
            PGPUserAttributeSubpacketVector uat = ater.next();
            ImageAttribute img = uat.getImageAttribute();
            if (null != img) {
              attributes.add(img.getImageData());
            }
          }
          infos.put(KEY_ATTR, attributes);
          keys.put(keyid, infos);
        }
        pubiter = keyring.getExtraPublicKeys();
      } else {
        pubiter = ((PGPPublicKeyRing) top).getPublicKeys();
      }

      while(pubiter.hasNext()) {
        PGPPublicKey key = pubiter.next();

        Map<Object,Object> infos = new LinkedHashMap<Object,Object>();

        String keyid = "000000000000000" + Long.toHexString(key.getKeyID());
        keyid = keyid.substring(keyid.length() - 16, keyid.length()).toUpperCase();
        infos.put(PGPPUBLIC.KEY_KEYID, keyid);
        infos.put(PGPPUBLIC.KEY_FINGERPRINT, Hex.toHexString(key.getFingerprint()));
        List<byte[]> uids = new ArrayList<byte[]>();
        Iterator<byte[]> uiditer = key.getRawUserIDs();
        while(uiditer.hasNext()) {
          uids.add(uiditer.next());
        }
        infos.put(PGPPUBLIC.KEY_UID, uids);
        infos.put(PGPPUBLIC.KEY_BITS, (long) key.getBitStrength());
        infos.put(PGPPUBLIC.KEY_ALG, PGPPUBLIC.getPublicKeyAlgorithmName(key.getAlgorithm()));
        infos.put(KEY_MASTER, key.isMasterKey());
        infos.put(KEY_SIGNING, false);
        infos.put(KEY_ENCRYPTION, key.isEncryptionKey());
        long creation = key.getCreationTime().getTime();
        infos.put(KEY_EXPIRY, 0 == key.getValidSeconds() ? 0L : (creation + key.getValidSeconds() * 1000L) * Constants.TIME_UNITS_PER_MS);
        infos.put(KEY_PUBKEY, key);
        Iterator<PGPUserAttributeSubpacketVector> ater = key.getUserAttributes();
        List<byte[]> attributes = new ArrayList<byte[]>();
        while(ater.hasNext()) {
          PGPUserAttributeSubpacketVector uat = ater.next();
          ImageAttribute img = uat.getImageAttribute();
          if (null != img) {
            attributes.add(img.getImageData());
          }
        }
        infos.put(KEY_ATTR, attributes);
        keys.put(keyid, infos);
      }
      stack.push(keys);
    } else if (top instanceof PGPPublicKey) {
      PGPPublicKey key = (PGPPublicKey) top;

      Map<Object,Object> infos = new LinkedHashMap<Object,Object>();

      String keyid = "000000000000000" + Long.toHexString(key.getKeyID());
      keyid = keyid.substring(keyid.length() - 16, keyid.length()).toUpperCase();
      infos.put(PGPPUBLIC.KEY_KEYID, keyid);
      infos.put(PGPPUBLIC.KEY_FINGERPRINT, Hex.toHexString(key.getFingerprint()));
      List<byte[]> uids = new ArrayList<byte[]>();
      Iterator<byte[]> uiditer = key.getRawUserIDs();
      while(uiditer.hasNext()) {
        uids.add(uiditer.next());
      }
      infos.put(PGPPUBLIC.KEY_UID, uids);
      infos.put(PGPPUBLIC.KEY_BITS, (long) key.getBitStrength());
      infos.put(PGPPUBLIC.KEY_ALG, PGPPUBLIC.getPublicKeyAlgorithmName(key.getAlgorithm()));
      infos.put(KEY_MASTER, key.isMasterKey());
      infos.put(KEY_SIGNING, false);
      infos.put(KEY_ENCRYPTION, key.isEncryptionKey());
      long creation = key.getCreationTime().getTime();
      infos.put(KEY_EXPIRY, 0 == key.getValidSeconds() ? 0L : (creation + key.getValidSeconds() * 1000L) * Constants.TIME_UNITS_PER_MS);
      infos.put(KEY_PUBKEY, key);
      stack.push(infos);
    } else {
      throw new WarpScriptException(getName() + " expected a PGP public key or secret key ring.");
    }

    return stack;
  }
}
