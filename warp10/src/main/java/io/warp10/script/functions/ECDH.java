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

import java.math.BigInteger;

import org.bouncycastle.crypto.agreement.ECDHBasicAgreement;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.params.ECPublicKeyParameters;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.math.ec.ECCurve;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Perform an ECC Diffie-Hellman key agreement as per https://en.wikipedia.org/wiki/Elliptic-curve_Diffie%E2%80%93Hellman
 */
public class ECDH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ECDH(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof ECPublicKey)) {
      throw new WarpScriptException(getName() + " expects an ECC public key.");
    }

    ECPublicKey pubkey = (ECPublicKey) top;
    
    top = stack.pop();

    if (!(top instanceof ECPrivateKey)) {
      throw new WarpScriptException(getName() + " expects an ECC private key.");
    }

    ECPrivateKey key = (ECPrivateKey) top;
    
    ECCurve curve = key.getParameters().getCurve();
    ECNamedCurveParameterSpec spec = (ECNamedCurveParameterSpec) key.getParameters();
    ECDomainParameters domainParams = new ECDomainParameters(curve, spec.getG(),spec.getN(), spec.getH(), spec.getSeed());    

    ECPrivateKeyParameters privateKey = new ECPrivateKeyParameters(key.getD(), domainParams);
    ECPublicKeyParameters publicKey = new ECPublicKeyParameters(pubkey.getQ(), domainParams);
    
    ECDHBasicAgreement ka = new ECDHBasicAgreement();
    ka.init(privateKey);      
    BigInteger secret = ka.calculateAgreement(publicKey);
    
    String hex = secret.toString(16);
    
    // Ensure the hex representation of the agreed upon secret is of even size so it can be decoded
    
    if (0 != hex.length() % 2) {
      hex = "0" + hex;
    }
    
    stack.push(hex);
        
    return stack;
  }
}
