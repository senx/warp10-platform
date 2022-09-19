package io.warp10.script.ext.pgp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;


import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPENCRYPT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public PGPENCRYPT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    boolean armor = true;

    if (top instanceof Boolean) {
      armor = Boolean.TRUE.equals(top);
      top = stack.pop();
    }

    if (!(top instanceof PGPPublicKey)) {
      throw new WarpScriptException(getName() + " missing PGP public key.");
    }

    PGPPublicKey pubkey = (PGPPublicKey) top;

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

      edg.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(pubkey)
          .setSecureRandom(new SecureRandom())
          .setProvider(provider).setSessionKeyObfuscation(true));

      ByteArrayOutputStream out = new ByteArrayOutputStream();

      ArmoredOutputStream armored = armor ? new ArmoredOutputStream(out) : null;

      // create an indefinite length encrypted stream
      OutputStream cOut = edg.open(armor ? armored : out, new byte[1 << 10]);
      // write out the literal data
      PGPLiteralDataGenerator ldata = new PGPLiteralDataGenerator();
      OutputStream pOut = ldata.open(cOut, PGPLiteralData.BINARY, PGPLiteralData.CONSOLE, data.length, PGPLiteralData.NOW);
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
