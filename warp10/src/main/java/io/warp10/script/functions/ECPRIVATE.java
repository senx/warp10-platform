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

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.jce.spec.ECParameterSpec;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class ECPRIVATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ECPRIVATE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (top instanceof ECPrivateKey) {
      ECPrivateKey priv = (ECPrivateKey) top;
      Map<Object,Object> params = new LinkedHashMap<Object,Object>();
      ECNamedCurveParameterSpec curve = (ECNamedCurveParameterSpec) priv.getParameters();
      params.put(Constants.KEY_CURVE, curve.getName());
      params.put(Constants.KEY_D, priv.getD().toString());
      stack.push(params);
      return stack;
    }

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a parameter map or an ECC private key instance.");
    }

    Map<Object,Object> params = (Map<Object,Object>) top;

    String name = String.valueOf(params.get(Constants.KEY_CURVE));

    final ECNamedCurveParameterSpec curve = ECNamedCurveTable.getParameterSpec(name);

    if (null == curve) {
      throw new WarpScriptException(getName() + " curve name not in " + ECGEN.getCurves() + ".");
    }

    if (!(params.get(Constants.KEY_D) instanceof String)) {
      throw new WarpScriptException(getName() + " missing or non-String parameter '" + Constants.KEY_D + "'.");
    }

    String dstr = (String) params.get(Constants.KEY_D);
    final BigInteger d;

    if (dstr.startsWith("0x") || dstr.startsWith("0X")) {
      d = new BigInteger(dstr.substring(2), 16);
    } else {
      d = new BigInteger(dstr);
    }

    ECPrivateKey privateKey = new ECPrivateKey() {
      public String getFormat() { return "PKCS#8"; }
      public byte[] getEncoded() { return null; }
      public String getAlgorithm() { return "EC"; }
      public ECParameterSpec getParameters() { return curve; }
      public BigInteger getD() { return d; }
    };

    stack.push(privateKey);

    return stack;
  }

}
