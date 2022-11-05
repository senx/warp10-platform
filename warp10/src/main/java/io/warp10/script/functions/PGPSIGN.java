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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.BCPGOutputStream;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureGenerator;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.bc.BcPBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.bc.BcPGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.bc.BcPGPDigestCalculatorProvider;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPSIGN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String KEY_DETACHED = "detached";
  public static final String KEY_KEYRING = "keyring";
  private static final String KEY_PASSPHRASE = "passphrase";
  private static final String KEY_DIGEST = "digest";
  public static final String KEY_ARMOR = "armor";
  public static final String KEY_DATE = "date";
  public static final String KEY_THROW_KEYID = "throw_keyid";

  public PGPSIGN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expected a parameter MAP.");
    }

    Map<Object,Object> params = (Map<Object,Object>) top;

    boolean detached = Boolean.TRUE.equals(params.getOrDefault(KEY_DETACHED, true));
    boolean armor = Boolean.TRUE.equals(params.getOrDefault(KEY_ARMOR, true));

    if (!(params.get(KEY_PASSPHRASE) instanceof String)) {
      throw new WarpScriptException(getName() + " missing PGP secret key passphrase.");
    }

    String passphrase = (String) params.get(KEY_PASSPHRASE);

    if (!(params.get(KEY_KEYRING) instanceof PGPSecretKeyRing)) {
      throw new WarpScriptException(getName() + " expected a PGP secret key ring.");
    }

    long keyid = 0L;

    Object k = params.get(PGPPUBLIC.KEY_KEYID);

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

    if (!(params.get(KEY_KEYRING) instanceof PGPSecretKeyRing)) {
      throw new WarpScriptException(getName() + " missing PGP secret key ring.");
    }

    PGPSecretKeyRing keyring = (PGPSecretKeyRing) params.get(KEY_KEYRING);

    PGPSecretKey secret = keyring.getSecretKey(keyid);

    if (null == secret) {
      throw new WarpScriptException(getName() + " key with id 0x" + Long.toHexString(keyid) + " not found.");
    }

    PBESecretKeyDecryptor decryptorFactory = new BcPBESecretKeyDecryptorBuilder(new BcPGPDigestCalculatorProvider()).build(passphrase.toCharArray());
    PGPPrivateKey signingKey = null;

    try {
      signingKey = secret.extractPrivateKey(decryptorFactory);
    } catch (PGPException pgpe) {
      throw new WarpScriptException(getName() + " unable to extract private key.", pgpe);
    }

    int digestId = PGPUtil.getDigestIDForName(String.valueOf(params.getOrDefault(KEY_DIGEST, "SHA512")));

    top = stack.pop();

    byte[] data = null;

    if (top instanceof String) {
      data = ((String) top).getBytes(StandardCharsets.UTF_8);
    } else if (top instanceof byte[]) {
      data = (byte[]) top;
    } else {
      throw new WarpScriptException(getName() + " invalid content to sign, expected STRING or BYTES.");
    }


    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    ArmoredOutputStream aOut = armor ? new ArmoredOutputStream(bOut, new Hashtable<String,String>()) : null;

    PGPSignatureGenerator sGen = new PGPSignatureGenerator(new BcPGPContentSignerBuilder(secret.getPublicKey().getAlgorithm(), digestId));

    try {
      sGen.init(PGPSignature.BINARY_DOCUMENT, signingKey);
      if (detached) {
        sGen.update(data);
        sGen.generate().encode(armor ? aOut : bOut);
        if (armor) {
          aOut.close();
        }
      } else {
        BCPGOutputStream bcOut = new BCPGOutputStream(armor ? aOut : bOut);

        sGen.init(PGPSignature.BINARY_DOCUMENT, signingKey);
        sGen.generateOnePassVersion(false).encode(bcOut);

        PGPLiteralDataGenerator lGen = new PGPLiteralDataGenerator();

        Date date = PGPLiteralData.NOW;

        if (params.get(KEY_DATE) instanceof Long) {
          date = new Date(((Long) params.get(KEY_DATE)).longValue() / Constants.TIME_UNITS_PER_MS);
        }
        OutputStream lOut = lGen.open(bcOut, PGPLiteralData.BINARY, PGPLiteralData.CONSOLE, data.length, date);
        lOut.write(data);
        sGen.update(data);
        lGen.close();
        sGen.generate().encode(bcOut);
        if (armor) {
          aOut.close();
        }
      }
    } catch (IOException|PGPException e) {
      throw new WarpScriptException(getName() + " unable to sign content.", e);
    }

    stack.push(bOut.toByteArray());

    return stack;
  }
}
