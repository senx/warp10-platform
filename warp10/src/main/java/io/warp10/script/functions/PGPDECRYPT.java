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
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.bc.BcPGPObjectFactory;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.PublicKeyDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.bc.BcPBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.bc.BcPGPDigestCalculatorProvider;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyDataDecryptorFactory;
import org.bouncycastle.util.encoders.Hex;
import org.bouncycastle.util.io.Streams;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPDECRYPT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public PGPDECRYPT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " missing passphrase.");
    }

    String passphrase = (String) top;

    top = stack.pop();

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

    if (!(top instanceof PGPSecretKeyRing)) {
      throw new WarpScriptException(getName() + " missing PGP secret key ring.");
    }

    PGPSecretKeyRing keyring = (PGPSecretKeyRing) top;

    PGPSecretKey key = keyring.getSecretKey(keyid);

    if (null == key) {
      throw new WarpScriptException(getName() + " key with id 0x" + Long.toHexString(keyid) + " not found.");
    }

    PBESecretKeyDecryptor decryptorFactory = new BcPBESecretKeyDecryptorBuilder(new BcPGPDigestCalculatorProvider()).build(passphrase.toCharArray());
    PGPPrivateKey privateKey = null;

    try {
      privateKey = key.extractPrivateKey(decryptorFactory);
    } catch (PGPException pgpe) {
      throw new WarpScriptException(getName() + " unable to extract private key.", pgpe);
    }

    top = stack.pop();

    byte[] data = null;

    if (top instanceof byte[]) {
      data = (byte[]) top;
    } else if (top instanceof String) {
      try {
        ArmoredInputStream ais = new ArmoredInputStream(new ByteArrayInputStream(((String) top).getBytes(StandardCharsets.UTF_8)));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyLarge(ais, out);
        data = out.toByteArray();
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " error while extracting PGP message.", ioe);
      }
    } else {
      throw new WarpScriptException(getName() + " expected encrypted content as STRING or BYTES.");
    }

    PGPObjectFactory pgpFact = new BcPGPObjectFactory(data);

    try {
      PGPEncryptedDataList encList = (PGPEncryptedDataList) pgpFact.nextObject();
      PGPPublicKeyEncryptedData encData = (PGPPublicKeyEncryptedData) encList.get(0);

      PublicKeyDataDecryptorFactory dataDecryptorFactory = new BcPublicKeyDataDecryptorFactory(privateKey);

      InputStream clear = encData.getDataStream(dataDecryptorFactory);

      byte[] literalData = Streams.readAll(clear);

      if (true || encData.verify()) {
        PGPObjectFactory litFact = new BcPGPObjectFactory(literalData);
        Object next = litFact.nextObject();

        if (next instanceof PGPCompressedData) {
          PGPCompressedData cData = (PGPCompressedData) next;
          pgpFact = new BcPGPObjectFactory(cData.getDataStream());
          next = pgpFact.nextObject();
        }

        if (next instanceof PGPLiteralData) {
          PGPLiteralData litData = (PGPLiteralData) next;
          byte[] cleartext = Streams.readAll(litData.getInputStream());
          stack.push(cleartext);
        } else {
          throw new IllegalStateException("no encrypted content found");
        }
      } else {
        throw new IllegalStateException("modification check failed");
      }
    } catch (IllegalStateException|PGPException|IOException|NullPointerException e) {
      throw new WarpScriptException(getName() + " error during decryption.", e);
    }

    return stack;
  }
}
