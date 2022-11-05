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
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.attr.ImageAttribute;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPUserAttributeSubpacketVector;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPPublicKeyRingCollection;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPPUBLIC extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String KEY_KEY = "key";
  public static final String KEY_KEYID = "keyid";
  public static final String KEY_UID = "uid";
  public static final String KEY_ALG = "algorithm";
  public static final String KEY_BITS = "bits";
  public static final String KEY_FINGERPRINT = "fingerprint";

  public PGPPUBLIC(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (top instanceof PGPPublicKey) {
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArmoredOutputStream aout = new ArmoredOutputStream(out, new Hashtable<String,String>());
        aout.write(((PGPPublicKey) top).getEncoded(false));
        aout.close();
        stack.push(new String(out.toByteArray(), StandardCharsets.UTF_8));
        return stack;
      } catch (Exception e) {
        throw new WarpScriptException(getName() + " encountered and error while serializing public key.", e);
      }
    } else if (top instanceof PGPSecretKeyRing) {
      // Extract ids of public keys
      Set<Long> ids = new LinkedHashSet<Long>();
      PGPSecretKeyRing keyring = (PGPSecretKeyRing) top;
      Iterator<PGPPublicKey> iter = keyring.getPublicKeys();
      while(iter.hasNext()) {
        ids.add(iter.next().getKeyID());
      }
      iter = keyring.getExtraPublicKeys();
      while(iter.hasNext()) {
        ids.add(iter.next().getKeyID());
      }
      List<String> keyids = new ArrayList<String>(ids.size());
      for (Long id: ids) {
        String keyid = "000000000000000" + Long.toHexString(id);
        keyids.add(keyid.substring(keyid.length() - 16, keyid.length()).toUpperCase());
      }
      stack.push(keyids);
      return stack;
    } else if (top instanceof PGPPublicKeyRing) {
      // Extract ids of public keys
      Set<Long> ids = new LinkedHashSet<Long>();
      PGPPublicKeyRing keyring = (PGPPublicKeyRing) top;
      Iterator<PGPPublicKey> iter = keyring.getPublicKeys();
      while(iter.hasNext()) {
        ids.add(iter.next().getKeyID());
      }
      List<String> keyids = new ArrayList<String>(ids.size());
      for (Long id: ids) {
        String keyid = "000000000000000" + Long.toHexString(id);
        keyids.add(keyid.substring(keyid.length() - 16, keyid.length()).toUpperCase());
      }
      stack.push(keyids);
      return stack;
    } else if (top instanceof Long) {
      long keyid = ((Long) top).longValue();
      top = stack.pop();

      PGPPublicKey key = null;

      if (top instanceof PGPPublicKeyRing) {
        key = ((PGPPublicKeyRing) top).getPublicKey(keyid);
      } else if (top instanceof PGPSecretKeyRing) {
        key = ((PGPSecretKeyRing) top).getPublicKey(keyid);
      } else {
        throw new WarpScriptException(getName() + " expected PGP public or secret key ring.");
      }

      if (null == key) {
        throw new WarpScriptException(getName() + " key with id 0x" + Long.toHexString(keyid) + " not found.");
      }

      return stack;
    }

    if (!(top instanceof String) && !(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " invalid public key, expected STRING or BYTES.");
    }

    ByteArrayInputStream in = top instanceof String ? new ByteArrayInputStream(((String) top).getBytes(StandardCharsets.UTF_8)) : new ByteArrayInputStream((byte[]) top);

    try {
      InputStream decoderstream = PGPUtil.getDecoderStream(in);
      JcaPGPPublicKeyRingCollection pgpPub = new JcaPGPPublicKeyRingCollection(decoderstream);
      decoderstream.close();
      Iterator<PGPPublicKeyRing> iter = pgpPub.getKeyRings();
      Map<Object,Object> pubkeys = new LinkedHashMap<Object,Object>();
      while(iter.hasNext()) {
        PGPPublicKeyRing kr = iter.next();
        Iterator<PGPPublicKey> keys = kr.getPublicKeys();
        while(keys.hasNext()) {
          PGPPublicKey key = keys.next();
          Map<String,Object> keymap = getKeyMap(key);
          pubkeys.put((String) keymap.get(KEY_KEYID), keymap);
        }
      }
      stack.push(pubkeys);
    } catch (IOException|PGPException e) {
      throw new WarpScriptException(getName() + " error decoding public key.", e);
    }
    return stack;
  }

  public static String getPublicKeyAlgorithmName(int alg) {
    switch(alg) {
      case 1:
        return "RSA_GENERAL";
      case 2:
        return "RSA_ENCRYPT";
      case 3:
        return "RSA_SIGN";
      case 16:
        return "ELGAMAL_ENCRYPT";
      case 17:
        return "DSA";
      case 18:
        return "ECDH";
      case 19:
        return "ECDSA";
      case 20:
        return "ELGAMAL_GENERAL";
      case 21:
        return "DIFFIE_HELLMAN";
      case 22:
        return "EDDSA";
      default:
        return "UNKNOWN_" + Integer.toString(alg);
    }
  }

  private static Map<String,Object> getKeyMap(PGPPublicKey key) {
    Map<String,Object> keymap = new LinkedHashMap<String,Object>();
    keymap.put(KEY_KEY, key);
    String hex = "000000000000000" + Long.toHexString(key.getKeyID());
    hex = hex.substring(hex.length() - 16).toUpperCase();
    keymap.put(KEY_KEYID, hex);
    keymap.put(KEY_FINGERPRINT, Hex.toHexString(key.getFingerprint()));
    keymap.put(KEY_BITS, (long) key.getBitStrength());
    keymap.put(KEY_ALG, getPublicKeyAlgorithmName(key.getAlgorithm()));
    keymap.put(PGPINFO.KEY_MASTER, key.isMasterKey());
    keymap.put(PGPINFO.KEY_SIGNING, false);
    keymap.put(PGPINFO.KEY_ENCRYPTION, key.isEncryptionKey());
    Iterator<byte[]> useridsiter = key.getRawUserIDs();
    List<byte[]> userids = new ArrayList<byte[]>();
    while(useridsiter.hasNext()) {
      userids.add(useridsiter.next());
    }
    keymap.put(KEY_UID, userids);
    long creation = key.getCreationTime().getTime();
    keymap.put(PGPINFO.KEY_EXPIRY, 0 == key.getValidSeconds() ? 0L : (creation + key.getValidSeconds() * 1000L) * Constants.TIME_UNITS_PER_MS);
    keymap.put(PGPINFO.KEY_PUBKEY, key);
    Iterator<PGPUserAttributeSubpacketVector> ater = key.getUserAttributes();
    List<byte[]> attributes = new ArrayList<byte[]>();
    while(ater.hasNext()) {
      PGPUserAttributeSubpacketVector uat = ater.next();
      ImageAttribute img = uat.getImageAttribute();
      if (null != img) {
        attributes.add(img.getImageData());
      }
    }
    keymap.put(PGPINFO.KEY_ATTR, attributes);
    return keymap;
  }
}
