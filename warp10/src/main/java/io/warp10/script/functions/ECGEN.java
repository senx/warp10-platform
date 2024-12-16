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

import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.crypto.AsymmetricCipherKeyPair;
import org.bouncycastle.crypto.generators.ECKeyPairGenerator;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECKeyGenerationParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.math.ec.ECCurve;

import com.geoxp.oss.CryptoHelper;

import io.warp10.continuum.store.Constants;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.SNAPSHOT.SnapshotEncoder;

/**
 * Generate a key pair for Elliptic Curve Cryptography
 */
public class ECGEN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final BouncyCastleProvider BCProvider = new BouncyCastleProvider();

  private static final ECPUBLIC ECPUBLIC = new ECPUBLIC(WarpScriptLib.ECPUBLIC);
  private static final ECPRIVATE ECPRIVATE = new ECPRIVATE(WarpScriptLib.ECPRIVATE);

  static {
    // Add a custom snapshot encoder for EC private and public keys
    SNAPSHOT.addEncoder(new ECSnapshotEncoder());
  }

  private static class ECSnapshotEncoder implements SnapshotEncoder {
    @Override
    public boolean addElement(SNAPSHOT snapshot, StringBuilder sb, Object o, boolean readable) throws WarpScriptException {
      if (o instanceof ECPrivateKey) {
        sb.append("{ '");
        sb.append(Constants.KEY_CURVE);
        sb.append("' '");
        ECNamedCurveParameterSpec curve = (ECNamedCurveParameterSpec) ((ECPrivateKey) o).getParameters();
        sb.append(curve.getName());
        sb.append("' '");
        sb.append(Constants.KEY_D);
        sb.append("' '");
        sb.append(((ECPrivateKey) o).getD());
        sb.append("' } ");
        sb.append(WarpScriptLib.ECPRIVATE);
        sb.append(" ");
        return true;
      } else if (o instanceof ECPublicKey) {
        sb.append("{ '");
        sb.append(Constants.KEY_CURVE);
        sb.append("' '");
        ECNamedCurveParameterSpec curve = (ECNamedCurveParameterSpec) ((ECPublicKey) o).getParameters();
        sb.append(curve.getName());
        sb.append("' '");
        sb.append(Constants.KEY_Q);
        sb.append("' '");
        sb.append(Hex.encodeHex(((ECPublicKey) o).getQ().getEncoded(false)));
        sb.append("' } ");
        sb.append(WarpScriptLib.ECPUBLIC);
        sb.append(" ");
        return true;
      } else {
        return false;
      }
    }
  }

  public ECGEN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a curve name.");
    }

    String name = (String) top;

    ECKeyPairGenerator gen = new ECKeyPairGenerator();

    ECNamedCurveParameterSpec spec = ECNamedCurveTable.getParameterSpec(name);

    if (null == spec) {
      throw new WarpScriptException(getName() + " only supports the following curves: " + getCurves() + ".");
    }

    ECCurve curve = spec.getCurve();
    ECDomainParameters domainParams = new ECDomainParameters(curve, spec.getG(),spec.getN(), spec.getH(), spec.getSeed());
    ECKeyGenerationParameters params = new ECKeyGenerationParameters(domainParams, CryptoHelper.getSecureRandom());

    gen.init(params);

    final AsymmetricCipherKeyPair keypair = gen.generateKeyPair();

    ECPrivateKeyParameters privateKey = (ECPrivateKeyParameters) keypair.getPrivate();

    Map<String,String> keyparams = new LinkedHashMap<String,String>();

    keyparams.put(Constants.KEY_CURVE, name);
    keyparams.put(Constants.KEY_D, privateKey.getD().toString());

    stack.push(keyparams);

    //
    // We rely on ECPRIVATE/ECPUBLIC to generate the public key so the specificity of curve25519 is taken into account
    //

    Object privmap = stack.peek();

    MemoryWarpScriptStack stck = new MemoryWarpScriptStack(null,null);
    stck.maxLimits();
    stck.push(privmap);
    ECPRIVATE.apply(stck);
    ECPUBLIC.apply(stck);
    ECPUBLIC.apply(stck);

    stack.push(stck.pop());

    return stack;
  }

  public static String getCurves() {
    StringBuilder sb = new StringBuilder();
    Enumeration<String> names = ECNamedCurveTable.getNames();
    while (names.hasMoreElements()) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(names.nextElement());
    }
    return sb.toString();
  }
}
