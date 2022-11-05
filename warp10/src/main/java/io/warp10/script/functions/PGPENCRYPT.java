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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.ContainedPacket;
import org.bouncycastle.bcpg.PublicKeyEncSessionPacket;
import org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.operator.bc.BcPGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyKeyEncryptionMethodGenerator;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPENCRYPT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String KEY_RECIPIENT = "recipient";

  /**
   * Class used to throw aways the keyId, just as GnuPG does when throw_keyid is specified.
   */
  private static class AnonymousBcPublicKeyKeyEncryptionMethodGenerator extends BcPublicKeyKeyEncryptionMethodGenerator {
    private final PGPPublicKey pubKey;

    public AnonymousBcPublicKeyKeyEncryptionMethodGenerator(PGPPublicKey key) {
      super(key);
      this.pubKey = key;
    }

    @Override
    public ContainedPacket generate(int encAlgorithm, byte[] sessionInfo) throws PGPException
    {
        return new PublicKeyEncSessionPacket(0L, pubKey.getAlgorithm(), processSessionInfo(encryptSessionInfo(pubKey, sessionInfo)));
    }
  }

  public PGPENCRYPT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expected a parameter MAP.");
    }

    Map<Object,Object> params = (Map<Object,Object>) top;

    boolean throwKeyId = Boolean.TRUE.equals(params.getOrDefault(PGPSIGN.KEY_THROW_KEYID, true));
    boolean armor = Boolean.TRUE.equals(params.getOrDefault(PGPSIGN.KEY_ARMOR, true));

    PGPPublicKey pubkey = null;

    if (params.get(KEY_RECIPIENT) instanceof PGPPublicKey) {
      pubkey = (PGPPublicKey) params.get(KEY_RECIPIENT);
    } else if (params.get(KEY_RECIPIENT) instanceof Long || params.get(KEY_RECIPIENT) instanceof String) {
      Object k = params.get(KEY_RECIPIENT);
      long keyid = 0L;

      if (k instanceof Long) {
        keyid = ((Long) k).longValue();
      } else if (k instanceof String) {
        byte[] decoded = Hex.decode((String) k);
        for (int i = 8; i >= 1; i--) {
          if (decoded.length - i >= 0) {
            keyid <<= 8;
            keyid |= ((long) decoded[decoded.length - i]) & 0xFFL;
          }
        }
      } else {
        throw new WarpScriptException(getName() + " missing PGP secret key id.");
      }

      if (params.get(PGPSIGN.KEY_KEYRING) instanceof PGPSecretKeyRing) {
        pubkey = ((PGPSecretKeyRing) params.get(PGPSIGN.KEY_KEYRING)).getPublicKey(keyid);
      } else if (params.get(PGPSIGN.KEY_KEYRING) instanceof PGPPublicKeyRing) {
        pubkey = ((PGPPublicKeyRing) params.get(PGPSIGN.KEY_KEYRING)).getPublicKey(keyid);
      } else {
        throw new WarpScriptException(getName() + " missing PGP secret key ring.");
      }

      if (null == pubkey) {
        throw new WarpScriptException(getName() + " key with id 0x" + Long.toHexString(keyid) + " not found.");
      }
    } else {
      throw new WarpScriptException(getName() + " missing recipient PGP public key or key ring and key id.");
    }

    top = stack.pop();

    byte[] data = null;

    if (top instanceof byte[]) {
      data = (byte[]) top;
    } else if (top instanceof String) {
      data = ((String) top).getBytes(StandardCharsets.UTF_8);
    } else {
      throw new WarpScriptException(getName() + " expects data to encrypt to be either STRING or BYTES.");
    }

    try {
      PGPEncryptedDataGenerator edg = new PGPEncryptedDataGenerator(
          new BcPGPDataEncryptorBuilder(getEncryptionAlgorithm(String.valueOf(params.getOrDefault(PGPPUBLIC.KEY_ALG, "AES_256"))))
            .setWithIntegrityPacket(true)
            .setSecureRandom(new SecureRandom()));

      if (throwKeyId) {
        edg.addMethod(new AnonymousBcPublicKeyKeyEncryptionMethodGenerator(pubkey)
            .setSecureRandom(new SecureRandom())
            .setSessionKeyObfuscation(true));
      } else {
        edg.addMethod(new BcPublicKeyKeyEncryptionMethodGenerator(pubkey)
            .setSecureRandom(new SecureRandom())
            .setSessionKeyObfuscation(true));
      }

      ByteArrayOutputStream out = new ByteArrayOutputStream();

      ArmoredOutputStream armored = armor ? new ArmoredOutputStream(out, new Hashtable<String,String>()) : null;

      // create an indefinite length encrypted stream
      OutputStream cOut = edg.open(armor ? armored : out, new byte[1 << 10]);
      // write out the literal data
      PGPLiteralDataGenerator ldata = new PGPLiteralDataGenerator();

      Date date = PGPLiteralData.NOW;

      if (params.get(PGPSIGN.KEY_DATE) instanceof Long) {
        date = new Date(((Long) params.get(PGPSIGN.KEY_DATE)).longValue() / Constants.TIME_UNITS_PER_MS);
      }

      OutputStream pOut = ldata.open(cOut, PGPLiteralData.BINARY, PGPLiteralData.CONSOLE, data.length, date);
      pOut.write(data);
      pOut.close();
      // finish the encryption
      cOut.close();

      if (armor) {
        armored.close();
        stack.push(new String(out.toByteArray(), StandardCharsets.UTF_8));
      } else {
        stack.push(out.toByteArray());
      }
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " error while encrypting data.", e);
    }


    return stack;
  }

  public static int getEncryptionAlgorithm(String name) throws WarpScriptException {
    switch(name) {
      case "AES_128":
        return SymmetricKeyAlgorithmTags.AES_128;
      case "AES_192":
        return SymmetricKeyAlgorithmTags.AES_192;
      case "AES_256":
        return SymmetricKeyAlgorithmTags.AES_256;
      case "BLOWFISH":
        return SymmetricKeyAlgorithmTags.BLOWFISH;
      case "CAMELLIA_128":
        return SymmetricKeyAlgorithmTags.CAMELLIA_128;
      case "CAMELLIA_192":
        return SymmetricKeyAlgorithmTags.CAMELLIA_192;
      case "CAMELLIA_256":
        return SymmetricKeyAlgorithmTags.CAMELLIA_256;
      case "CAST5":
        return SymmetricKeyAlgorithmTags.CAST5;
      case "DES":
        return SymmetricKeyAlgorithmTags.DES;
      case "IDEA":
        return SymmetricKeyAlgorithmTags.IDEA;
      case "SAFER":
        return SymmetricKeyAlgorithmTags.SAFER;
      case "TRIPLE_DES":
      case "3DES":
        return SymmetricKeyAlgorithmTags.TRIPLE_DES;
      case "TWOFISH":
        return SymmetricKeyAlgorithmTags.TWOFISH;
      default:
        throw new WarpScriptException("Invalid encryption algorithm '" + name + "'.");
    }
  }
}
