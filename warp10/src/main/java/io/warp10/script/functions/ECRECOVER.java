//
//   Copyright 2020  SenX S.A.S.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.EllipticCurve;
import java.util.Map;

import org.bouncycastle.jcajce.provider.asymmetric.util.EC5Util;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.math.ec.ECFieldElement;
import org.bouncycastle.math.ec.custom.sec.SecP256R1Curve;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Recover an ECC public key from signature (r,s), curve and hash
 */
public class ECRECOVER extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String KEY_R = "r";
  private static final String KEY_S = "s";
  private static final String KEY_SIG = "sig";
  private static final String KEY_HASH = "hash";

  public ECRECOVER(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    // See https://crypto.stackexchange.com/questions/18105/how-does-recovering-the-public-key-from-an-ecdsa-signature-work

    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a MAP.");
    }

    Map<Object,Object> params = (Map<Object,Object>) top;

    ECNamedCurveParameterSpec spec = ECNamedCurveTable.getParameterSpec(String.valueOf(params.get("curve")));

    System.out.println(spec.getCurve().getClass());

    org.bouncycastle.math.ec.ECPoint G = spec.getG();
    ECFieldElement N = spec.getCurve().fromBigInteger(spec.getN());
    BigInteger H = spec.getH();
    ECFieldElement A = spec.getCurve().getA();
    ECFieldElement B = spec.getCurve().getB();
    BigInteger P = spec.getCurve().getField().getCharacteristic();
    BigInteger r;
    BigInteger s;
    BigInteger z;

    if (!(params.get(KEY_R) instanceof String)) {
      throw new WarpScriptException(getName() + " invalid '" + KEY_R + "', expected STRING.");
    }

    if (!(params.get(KEY_S) instanceof String)) {
      throw new WarpScriptException(getName() + " invalid '" + KEY_R + "', expected STRING.");
    }

    if (!(params.get(KEY_HASH) instanceof byte[])) {
      throw new WarpScriptException(getName() + " invalid '" + KEY_HASH + "', expected BYTES.");
    }

    if (!(params.get(KEY_SIG) instanceof byte[])) {
      throw new WarpScriptException(getName() + " invalid '" + KEY_SIG + "', expected BYTES.");
    }

    String str = ((String) params.get(KEY_R)).toLowerCase();

    if (str.startsWith("0x")) {
      r = new BigInteger("00" + str.substring(2), 16);
    } else {
      r = new BigInteger(str);
    }

    str = ((String) params.get(KEY_S)).toLowerCase();

    if (str.startsWith("0x")) {
      s = new BigInteger("00" + str.substring(2), 16);
    } else {
      s = new BigInteger(str);
    }

    SecP256R1Curve
    byte[] hash = (byte[]) params.get(KEY_HASH);
    byte[] sig = (byte[]) params.get(KEY_SIG);

    for (int j = 0; j < H.intValue(); j++) {
      ECFieldElement je = spec.getCurve().fromBigInteger(BigInteger.valueOf(j));
      ECFieldElement x = spec.getCurve().fromBigInteger(r).add(N.multiply(je));
      ECFieldElement rhs = x.square().add(A).multiply(x).add(B);
      ECFieldElement y = rhs.sqrt();

      if (null == y) {
        throw new IllegalArgumentException("Invalid point compression");
      }

      org.bouncycastle.math.ec.ECPoint R = spec.getCurve().createPoint(x.toBigInteger(), y.toBigInteger());
      org.bouncycastle.math.ec.ECPoint Rprime = spec.getCurve().createPoint(x.toBigInteger(), y.negate().toBigInteger());

      𝑟−1(𝑠𝑅−𝑧𝐺)  and 𝑟−1(𝑠𝑅′−𝑧𝐺)

      ECFieldElement rinv = spec.getCurve().fromBigInteger(r).invert();
      ECFieldElement z = spec.getCurve().fromBigInteger(biz);
      org.bouncycastle.math.ec.ECPoint ecx = spec.getCurve().createPoint(r, BigInteger.ZERO);
      org.bouncycastle.math.ec.ECPoint ecx2 = ecx.multiply(r);
      org.bouncycastle.math.ec.ECPoint ecx3 = ecx2.multiply(r);

      ecx = ecx.multiply(r);

      BigInteger x = r.add(N.multiply(BigInteger.valueOf(j)));
      // Compute y^2 = x^3 + ax + b
      BigInteger y2 = x.pow(3).add(x.multiply(A)).add(B);
      BigDecimal y = new BigDecimal(y2);


    }

    if (params.containsKey("r") && params.containsKey("s")) {
      BigInteger rinv = r.modInverse(N);
    } else if (params.containsKey("rs")) {

    }

    BigInteger rinv = r.modInverse(N);

    if (!(top instanceof ECPublicKey)) {
      throw new WarpScriptException(getName() + " expects an ECC public key.");
    }
/*
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
*/
    return stack;
  }

  private static BigInteger sqrt(BigInteger val) {
    BigInteger half = BigInteger.ZERO.setBit(val.bitLength() / 2);
    BigInteger cur = half;

    while (true) {
      BigInteger tmp = half.add(val.divide(half)).shiftRight(1);

      if (tmp.equals(half) || tmp.equals(cur)) {
        return tmp;
      }
      cur = half;
      half = tmp;
    }
  }
}
