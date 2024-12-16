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
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;

import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;

import com.geoxp.oss.CryptoHelper;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Sign data using ECC and a hash algorithm
 */
public class ECSIGN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ECSIGN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof ECPrivateKey)) {
      throw new WarpScriptException(getName() + " expects an ECC private key.");
    }

    PrivateKey key = (PrivateKey) top;

    top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects an algorithm name.");
    }

    //
    // Algorithms are among those supported by BouncyCastle
    // cf http://stackoverflow.com/questions/8778531/bouncycastle-does-not-find-algorithms-that-it-provides
    //

    String alg = (String) top;

    top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }

    byte[] data = (byte[]) top;

    //
    // We don't support signing with Curve25519
    //

    ECNamedCurveParameterSpec curve = (ECNamedCurveParameterSpec) ((ECPrivateKey) key).getParameters();
    if ("curve25519".equals(curve.getName())) {
      throw new WarpScriptException(getName() + " doesn't support curve " + curve.getName());
    }

    //
    // Sign
    //

    try {
      Signature signature = Signature.getInstance(alg, ECGEN.BCProvider);
      signature.initSign(key, CryptoHelper.getSecureRandom());
      signature.update(data);
      stack.push(signature.sign());
    } catch (SignatureException se) {
      throw new WarpScriptException(getName() + " error signing content.", se);
    } catch (InvalidKeyException ike) {
      throw new WarpScriptException(getName() + " error signing content.", ike);
    } catch (NoSuchAlgorithmException nsae) {
      throw new WarpScriptException(getName() + " error signing content.", nsae);
    }

    return stack;
  }
}
