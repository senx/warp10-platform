//
//   Copyright 2021  SenX S.A.S.
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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.math.ec.ECFieldElement;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.util.Arrays;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Recover possible ECC public keys from an ECDSA signature (r,s), curve and hash
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

    final ECNamedCurveParameterSpec spec = ECNamedCurveTable.getParameterSpec(String.valueOf(params.get("curve")));

    ECFieldElement N = spec.getCurve().fromBigInteger(spec.getN());
    BigInteger H = spec.getH();
    ECFieldElement A = spec.getCurve().getA();
    ECFieldElement B = spec.getCurve().getB();
    BigInteger r;
    BigInteger s;
    BigInteger z;

    if (!(params.get(KEY_HASH) instanceof byte[])) {
      throw new WarpScriptException(getName() + " invalid '" + KEY_HASH + "', expected BYTES.");
    }

    if (null != params.get(KEY_SIG)) {
      // Decode DER encoded signature (sequence of 2 integers)
      // Example:
      // 3044 Sequence of 0x44 bytes
      // 0220 Integer on 0x20 bytes
      //      7d97908b4472fd28d9381d795e9da95e07c5fd3eaea532d20a495ef543d51135
      // 0220 Integer on 0x20 bytes
      //      6db3861916bcd7096b9d701b44ef3cd2a74d087405ee36f01a857c1363ccb383
      if (!(params.get(KEY_SIG) instanceof byte[])) {
        throw new WarpScriptException(getName() + " invalid '" + KEY_SIG + "', expected BYTES.");
      }

      byte[] sig = (byte[]) params.get(KEY_SIG);

      int rlen = (int) sig[3];

      int offset = 4;
      r = new BigInteger("00" + Hex.encodeHexString(Arrays.copyOfRange(sig, offset, offset + rlen)), 16);
      offset += rlen;
      offset += 2;
      s = new BigInteger("00" + Hex.encodeHexString(Arrays.copyOfRange(sig, offset, sig.length)), 16);
      if (r.compareTo(spec.getN()) > 0 || r.compareTo(BigInteger.ONE) < 0) {
        throw new WarpScriptException(getName() + " invalid value for r, should be in [1, 0x00" + spec.getN().toString(16) + "] but as 0x" + r.toString(16) + ".");
      }
      if (s.compareTo(spec.getN()) > 0 || s.compareTo(BigInteger.ONE) < 0) {
        throw new WarpScriptException(getName() + " invalid value for s, should be in [1, 0x00" + spec.getN().toString(16) + "] but as 0x" + s.toString(16) + ".");
      }
    } else if (null != params.get(KEY_R) && null != params.get(KEY_S)) {
      if (!(params.get(KEY_R) instanceof String)) {
        throw new WarpScriptException(getName() + " invalid '" + KEY_R + "', expected STRING.");
      }

      if (!(params.get(KEY_S) instanceof String)) {
        throw new WarpScriptException(getName() + " invalid '" + KEY_S + "', expected STRING.");
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
    } else {
      throw new WarpScriptException(getName() + " expects '" + KEY_SIG + "' or '" + KEY_R + "' and '" + KEY_S + "' to be provided.");
    }

    int nbits = spec.getN().bitLength();

    if (nbits % 8 != 0) {
      throw new WarpScriptException(getName() + " only supports ECC curves with a bit length which is a multiple of 8.");
    }

    byte[] hash = (byte[]) params.get(KEY_HASH);

    if (hash.length > nbits / 8) {
      hash = Arrays.copyOf(hash, nbits / 8);
    } else if (hash.length < nbits / 8) {
      throw new WarpScriptException(getName() + " invalid hash length, should be >= " + (nbits / 8) + " bytes.");
    }

    z = new BigInteger("00" + Hex.encodeHexString(hash), 16);

    List<Object> candidates = new ArrayList<Object>();

    for (int j = 0; j < H.intValue(); j++) {

      //
      // Compute y = sqrt(x^3  + ax + b)
      //

      ECFieldElement je = spec.getCurve().fromBigInteger(BigInteger.valueOf(j));
      ECFieldElement x = spec.getCurve().fromBigInteger(r).add(N.multiply(je));
      ECFieldElement rhs = x.square().add(A).multiply(x).add(B);
      ECFieldElement y = rhs.sqrt();

      // No square root, continue with next value of j
      if (null == y) {
        continue;
      }

      ECPoint R = spec.getCurve().createPoint(x.toBigInteger(), y.toBigInteger());

      //
      // if R is not a multiple of G then we skip to the next iteration of the loop
      //

      if (!R.multiply(spec.getN()).isInfinity()) {
        continue;
      }

      ECPoint Rprime = spec.getCurve().createPoint(x.toBigInteger(), y.negate().toBigInteger());

      //𝑟−1(𝑠𝑅−𝑧𝐺)  and 𝑟−1(𝑠𝑅′−𝑧𝐺)

      BigInteger rinv = spec.getCurve().fromBigInteger(r.modInverse(spec.getN())).toBigInteger(); //.invert().toBigInteger();

      // Points MUST be normalized

      // r^(-1) x (sR - zG)
      final ECPoint Q1 = R.multiply(s).subtract(spec.getG().multiply(z)).multiply(rinv).normalize();
      ECPublicKey K1 = new ECPublicKey() {
        public String getFormat() { return "PKCS#8"; }
        public byte[] getEncoded() { return Q1.getEncoded(false); }
        public String getAlgorithm() { return "EC"; }
        public ECPoint getQ() { return Q1; }
        public ECParameterSpec getParameters() { return spec; }
      };

      candidates.add(K1);

      // r^(-1) x (sR' - zG)
      final ECPoint Q2 = Rprime.multiply(s).subtract(spec.getG().multiply(z)).multiply(rinv).normalize();
      ECPublicKey K2 = new ECPublicKey() {
        public String getFormat() { return "PKCS#8"; }
        public byte[] getEncoded() { return Q2.getEncoded(false); }
        public String getAlgorithm() { return "EC"; }
        public ECPoint getQ() { return Q2; }
        public ECParameterSpec getParameters() { return spec; }
      };

      candidates.add(K2);
    }

    stack.push(candidates);

    return stack;
  }
}
