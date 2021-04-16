//
//   Copyright 2020-2021  SenX S.A.S.
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

import java.util.LinkedHashMap;
import java.util.Map;

import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class ECPUBLIC extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ECPUBLIC(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (top instanceof ECPublicKey) {
      ECPublicKey pubkey = (ECPublicKey) top;
      Map<Object,Object> params = new LinkedHashMap<Object,Object>();
      ECNamedCurveParameterSpec curve = (ECNamedCurveParameterSpec) pubkey.getParameters();
      params.put(Constants.KEY_CURVE, curve.getName());
      params.put(Constants.KEY_Q, org.apache.commons.codec.binary.Hex.encodeHexString(pubkey.getEncoded()));
      stack.push(params);
      return stack;
    }

    if (!(top instanceof Map) && !(top instanceof ECPrivateKey)) {
      throw new WarpScriptException(getName() + " expects a parameter map, or a public or private key.");
    }

    if (top instanceof ECPrivateKey) {

      ECPrivateKey privateKey = (ECPrivateKey) top;

      final ECParameterSpec bcSpec = privateKey.getParameters();
      ECPoint q = bcSpec.getG().multiply(privateKey.getD());

      ECPublicKey publicKey = new ECPublicKey() {
        public String getFormat() { return "PKCS#8"; }
        public byte[] getEncoded() { return q.getEncoded(false); }
        public String getAlgorithm() { return "EC"; }
        public ECParameterSpec getParameters() { return bcSpec; }
        public ECPoint getQ() { return q; }
      };

      stack.push(publicKey);

      return stack;
    }

    Map<Object,Object> params = (Map<Object,Object>) top;

    String name = String.valueOf(params.get(Constants.KEY_CURVE));

    final ECNamedCurveParameterSpec curve = ECNamedCurveTable.getParameterSpec(name);

    if (null == curve) {
      throw new WarpScriptException(getName() + " curve name '" + name + "' not in " + ECGEN.getCurves() + ".");
    }

    if (!(params.get(Constants.KEY_Q) instanceof String)) {
      throw new WarpScriptException(getName() + " missing or non-String parameter '" + Constants.KEY_Q + "'.");
    }

    final byte[] encoded = Hex.decode((String) params.get(Constants.KEY_Q));

    final ECPoint q = curve.getCurve().decodePoint(encoded);

    ECPublicKey publicKey = new ECPublicKey() {
      public String getFormat() { return "PKCS#8"; }
      public byte[] getEncoded() { return encoded; }
      public String getAlgorithm() { return "EC"; }
      public ECParameterSpec getParameters() { return curve; }
      public ECPoint getQ() { return q; }
    };

    stack.push(publicKey);

    return stack;
  }

}
