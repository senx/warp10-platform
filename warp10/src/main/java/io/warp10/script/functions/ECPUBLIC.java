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

import java.util.Map;

import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.math.ec.ECPoint;

import com.geoxp.oss.jarjar.org.bouncycastle.util.encoders.Hex;

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
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a parameter map.");
    }
    
    Map<Object,Object> params = (Map<Object,Object>) top;
    
    String name = String.valueOf(params.get(Constants.KEY_CURVE));
    
    final ECNamedCurveParameterSpec curve = ECNamedCurveTable.getParameterSpec(name);

    if (null == curve) {
      throw new WarpScriptException(getName() + " curve name not in " + ECGEN.getCurves() + ".");
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
