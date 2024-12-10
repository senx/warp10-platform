//
//   Copyright 2021-2024  SenX S.A.S.
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bouncycastle.asn1.x9.X9IntegerConverter;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.util.Arrays;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

/**
 * Recover possible ECC public keys from an ECDSA signature (r,s), curve and hash
 */
public class ECRECOVER extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String KEY_Q = "Q";
  private static final String KEY_PUBKEY = "pubkey";
  private static final String KEY_RX = "R.x";
  private static final String KEY_RY = "R.y";

  private static final String KEY_R = "r";
  private static final String KEY_S = "s";
  private static final String KEY_I = "i";
  private static final String KEY_EVEN = "even";
  private static final String KEY_SIG = "sig";
  private static final String KEY_HASH = "hash";
  private static final String KEY_CURVE = "curve";
  private static final String KEY_COORDS = "coords";

  private static final String CAP_COFACTOR = "ecc.h";

  private static final int MAX_COFACTOR = 10;

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

    if (!(params.get(KEY_CURVE) instanceof String)) {
      throw new WarpScriptException(getName() + " missing ECC curve name under '" + KEY_CURVE + "'.");
    }
    final ECNamedCurveParameterSpec spec = ECNamedCurveTable.getParameterSpec((String) params.get(KEY_CURVE));

    //
    // We don't support signing with Curve25519
    //

    if ("curve25519".equals(spec.getName())) {
      throw new WarpScriptException(getName() + " doesn't support curve " + spec.getName());
    }

    BigInteger H = spec.getH();
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

      int offset = 0;

      if (0 != (sig[1] & 0x80)) { // Length is encoded by sig[1] - 1 bytes
        offset = 2;
        int sbytes = (sig[1] & 0xFF) - 0x80;
        int len = 0;
        while(sbytes > 0) {
          len = len << 8;
          len |= sig[offset++] & 0xFF;
          sbytes--;
        }
      } else {
        offset = 2;
      }

      offset++; // Skip over '0x02'

      // We assume that the size of r and s is less than 128 bytes (1024 bits)

      int rlen = (int) sig[offset++];

      r = new BigInteger(1, Arrays.copyOfRange(sig, offset, offset + rlen));
      offset += rlen;
      offset += 2;
      s = new BigInteger(1, Arrays.copyOfRange(sig, offset, sig.length));
      if (r.compareTo(spec.getN()) > 0 || r.compareTo(BigInteger.ONE) < 0) {
        throw new WarpScriptException(getName() + " invalid value for r, should be in [1, 0x00" + spec.getN().toString(16) + "] but was 0x" + r.toString(16) + ".");
      }
      if (s.compareTo(spec.getN()) > 0 || s.compareTo(BigInteger.ONE) < 0) {
        throw new WarpScriptException(getName() + " invalid value for s, should be in [1, 0x00" + spec.getN().toString(16) + "] but was 0x" + s.toString(16) + ".");
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
    byte[] hash = (byte[]) params.get(KEY_HASH);

    z = new BigInteger(1, hash);

    if (nbits < hash.length * 8) {
      z = z.shiftRight(hash.length * 8 - nbits);
    }

    BigInteger rinv = spec.getCurve().fromBigInteger(r.modInverse(spec.getN())).toBigInteger();

    int minH = 0;
    int maxH = H.intValue();

    if (params.get(KEY_I) instanceof Long) {
      minH = ((Long) params.get(KEY_I)).intValue();
      maxH = minH;
    }

    if (maxH - minH + 1 > MAX_COFACTOR) {
      String hmaxCap = Capabilities.get(stack, CAP_COFACTOR);
      try {
        int hmax = Integer.valueOf(hmaxCap);
        if (maxH > hmax) {
          throw new WarpScriptException(getName() + " cofactor " + maxH + " is above the maximum " + hmax + " allowed by the '" + CAP_COFACTOR + "' capability.");
        }
      } catch (NumberFormatException nfe) {
        throw new WarpScriptException(getName() + " cofactor " + maxH + " is above allowed maximum " + MAX_COFACTOR + ", increase this limit using a token with the '" + CAP_COFACTOR + "' capability.");
      }
    }

    Set<Object> candidates = new LinkedHashSet<Object>();

    int mintype = 0x02;
    int maxtype = 0x03;

    if (params.get(KEY_EVEN) instanceof Boolean) {
      if (Boolean.TRUE.equals(params.get(KEY_EVEN))) {
        mintype = 0x02;
        maxtype = 0x02;
      } else {
        mintype = 0x03;
        maxtype = 0x03;
      }
    }

    boolean coords = Boolean.TRUE.equals(params.get(KEY_COORDS));

    for (int j = minH; j <= maxH; j++) {
      // Iterate over the type of encoded points 0x02 and 0x03
      // 0x02 is for even key
      // 0x03 is for odd key
      for (int type = mintype; type <= maxtype; type++) {
        try {
          //
          // Compute R with x coordinate equal to r from the signature
          //

          BigInteger x = r.add(BigInteger.valueOf(j).multiply(spec.getN()));

          // Compress the point so it can de decompressed by the curve
          //org.bouncycastle.math.ec.custom.sec.SecP256K1Curve
          X9IntegerConverter x9 = new X9IntegerConverter();
          byte[] encoded = x9.integerToBytes(x, 1 + x9.getByteLength(spec.getCurve()));
          encoded[0] = (byte)(type);
          ECPoint R = spec.getCurve().decodePoint(encoded).normalize();

          //
          // if R is not a multiple of G then we skip to the next iteration of the loop
          //

          if (!R.multiply(spec.getN()).isInfinity()) {
            continue;
          }

          ECPoint Rprime = spec.getCurve().createPoint(x, R.getYCoord().negate().toBigInteger()).normalize();

          // Points MUST be normalized

          // 𝑟−1(𝑠𝑅−𝑧𝐺)
          final ECPoint Q1 = R.multiply(s).subtract(spec.getG().multiply(z)).multiply(rinv).normalize();

          if (coords) {
            Map<Object,Object> candidate = new LinkedHashMap<Object,Object>(3);
            candidate.put(KEY_Q, new String(Q1.getEncoded(false), StandardCharsets.ISO_8859_1));
            candidate.put(KEY_RX, "0x" + R.getXCoord().toBigInteger().toString(16));
            candidate.put(KEY_RY, "0x" + R.getYCoord().toBigInteger().toString(16));
            candidates.add(candidate);
          } else {
            candidates.add(new String(Q1.getEncoded(false), StandardCharsets.ISO_8859_1));
          }

          // 𝑟−1(𝑠𝑅′−𝑧𝐺)
          final ECPoint Q2 = Rprime.multiply(s).subtract(spec.getG().multiply(z)).multiply(rinv).normalize();

          if (coords) {
            Map<Object,Object> candidate = new LinkedHashMap<Object,Object>(3);
            candidate.put(KEY_Q, new String(Q2.getEncoded(false), StandardCharsets.ISO_8859_1));
            candidate.put(KEY_RX, "0x" + Rprime.getXCoord().toBigInteger().toString(16));
            candidate.put(KEY_RY, "0x" + Rprime.getYCoord().toBigInteger().toString(16));
            candidates.add(candidate);
          } else {
            candidates.add(new String(Q2.getEncoded(false), StandardCharsets.ISO_8859_1));
          }
        } catch (IllegalArgumentException iae) {
          // May be thrown when x is not valid
          continue;
        } catch (IllegalStateException ise) {
          // May be thrown when an ECPoint is invalid
          continue;
        }
      }
    }

    List<Object> keys = new ArrayList<Object>(candidates.size());

    for (Object candidate: candidates) {

      String qstr;

      if (coords) {
        qstr = (String) ((Map<Object,Object>) candidate).remove(KEY_Q);
      } else {
        qstr = (String) candidate;
      }

      byte[] encoded = qstr.getBytes(StandardCharsets.ISO_8859_1);
      ECPoint Q = spec.getCurve().decodePoint(encoded);

      ECPublicKey pk = new ECPublicKey() {
        public String getFormat() { return "PKCS#8"; }
        public byte[] getEncoded() { return encoded; }
        public String getAlgorithm() { return "EC"; }
        public ECPoint getQ() { return Q; }
        public ECParameterSpec getParameters() { return spec; }
      };

      if (coords) {
        ((Map<Object,Object>) candidate).put(KEY_PUBKEY, pk);
        keys.add(candidate);
      } else {
        keys.add(pk);
      }
    }

    stack.push(keys);

    return stack;
  }
}
