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

package io.warp10.script.ext.pgp;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Map;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.ContainedPacket;
import org.bouncycastle.bcpg.PublicKeyEncSessionPacket;
import org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPENCRYPT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  /**
   * Class used to throw aways the keyId, just as GnuPG does when throw_keyid is specified.
   */
  private static class AnonymousJcePublicKeyKeyEncryptionMethodGenerator extends JcePublicKeyKeyEncryptionMethodGenerator {
    private final PGPPublicKey pubKey;

    public AnonymousJcePublicKeyKeyEncryptionMethodGenerator(PGPPublicKey key) {
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

    Map<Object, Object> params = (Map<Object,Object>) top;

    boolean throwKeyId = Boolean.TRUE.equals(params.getOrDefault(PGPSIGN.KEY_THROW_KEYID, true));
    boolean armor = Boolean.TRUE.equals(params.getOrDefault(PGPSIGN.KEY_ARMOR, true));;

    if (!(params.get(PGPPUBLIC.KEY_KEY) instanceof PGPPublicKey)) {
      throw new WarpScriptException(getName() + " missing PGP public key.");
    }

    PGPPublicKey pubkey = (PGPPublicKey) params.get(PGPPUBLIC.KEY_KEY);

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
      BouncyCastleProvider provider = new BouncyCastleProvider();

      PGPEncryptedDataGenerator edg = new PGPEncryptedDataGenerator(
          new JcePGPDataEncryptorBuilder(SymmetricKeyAlgorithmTags.AES_256)
            .setWithIntegrityPacket(true)
            .setSecureRandom(new SecureRandom())
            .setProvider(provider));

      if (throwKeyId) {
        edg.addMethod(new AnonymousJcePublicKeyKeyEncryptionMethodGenerator(pubkey)
            .setSecureRandom(new SecureRandom())
            .setProvider(provider).setSessionKeyObfuscation(true));
      } else {
        edg.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(pubkey)
            .setSecureRandom(new SecureRandom())
            .setProvider(provider).setSessionKeyObfuscation(true));
      }

      ByteArrayOutputStream out = new ByteArrayOutputStream();

      ArmoredOutputStream armored = armor ? new ArmoredOutputStream(out) : null;

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
      }

      stack.push(new String(out.toByteArray(), StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " error while encrypting data.", e);
    }


    return stack;
  }


}
