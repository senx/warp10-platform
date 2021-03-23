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

import java.util.LinkedHashMap;
import java.util.Map;

import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.math.ec.ECCurve;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class MSIGINFO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String KEY_SIG = "sig";
  private static final String KEY_KEY = "key";

  public MSIGINFO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a macro.");
    }

    Macro macro = MSIG.getSignature((Macro) top);

    int size = macro.size();

    Map<Object,Object> siginfo = new LinkedHashMap<Object,Object>();

    if (4 != size) {
      stack.push(false);
      return stack;
    }

    ECNamedCurveParameterSpec spec = ECNamedCurveTable.getParameterSpec((String) macro.get(0));
    ECCurve curve = spec.getCurve();
    byte[] encoded = Hex.decode((String) macro.get(1));
    final ECPoint q = curve.decodePoint(encoded);

    ECPublicKey pubkey = new ECPublicKey() {
      public String getFormat() { return "PKCS#8"; }
      public byte[] getEncoded() { return encoded; }
      public String getAlgorithm() { return "EC"; }
      public ECParameterSpec getParameters() { return spec; }
      public ECPoint getQ() { return q; }
    };

    byte[] sig = Hex.decode((String) macro.get(2));

    siginfo.put(KEY_SIG, sig);
    siginfo.put(KEY_KEY, pubkey);

    stack.push(top);
    stack.push(siginfo);

    return stack;
  }
}
