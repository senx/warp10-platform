//
//   Copyright 2020-2024  SenX S.A.S.
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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.EllipticCurve;

import org.bouncycastle.jcajce.provider.asymmetric.util.EC5Util;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Verify an ECC signature
 */
public class ECVERIFY extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ECVERIFY(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof ECPublicKey)) {
      throw new WarpScriptException(getName() + " expects an ECC public key.");
    }

    ECPublicKey pubkey = (ECPublicKey) top;

    byte[] encoded = ((ECPublicKey) top).getQ().getEncoded(false);
    org.bouncycastle.math.ec.ECPoint q = ((ECPublicKey) top).getQ();
    ECPoint w = new ECPoint(q.getXCoord().toBigInteger(), q.getYCoord().toBigInteger());
    org.bouncycastle.jce.spec.ECParameterSpec curve = ((ECPublicKey) top).getParameters();
    EllipticCurve ec = EC5Util.convertCurve(curve.getCurve(),  curve.getSeed());

    final ECParameterSpec spec = new ECParameterSpec(
        ec,
        new ECPoint(curve.getG().getXCoord().toBigInteger(), curve.getG().getYCoord().toBigInteger()),
        curve.getN(),
        curve.getH().intValue());

    java.security.interfaces.ECPublicKey key = new java.security.interfaces.ECPublicKey() {
      public String getFormat() { return "PKCS#8"; }
      public byte[] getEncoded() { return encoded; }
      public String getAlgorithm() { return "EC"; }
      public ECPoint getW() { return w; }
      public ECParameterSpec getParams() { return spec; }
    };

    top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects an algorithm name.");
    }

    String alg = top.toString();

    top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " expects a signature.");
    }

    byte[] sig = (byte[]) top;

    top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }

    byte[] data = (byte[]) top;


    //
    // We don't support signing with Curve25519
    //

    ECNamedCurveParameterSpec crv = (ECNamedCurveParameterSpec) pubkey.getParameters();
    if ("curve25519".equals(crv.getName())) {
      throw new WarpScriptException(getName() + " doesn't support curve " + crv.getName());
    }

    try {
      Signature signature = Signature.getInstance(alg, ECGEN.BCProvider);
      signature.initVerify(key);
      signature.update(data);
      stack.push(signature.verify(sig));
    } catch (SignatureException se) {
      throw new WarpScriptException(getName() + " error while verifying signature.", se);
    } catch (InvalidKeyException ike) {
      throw new WarpScriptException(getName() + " error while verifying signature.", ike);
    } catch (NoSuchAlgorithmException nsae) {
      throw new WarpScriptException(getName() + " error while verifying signature.", nsae);
    }

    return stack;
  }
}
