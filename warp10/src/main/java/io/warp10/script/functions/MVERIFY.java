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

import java.nio.charset.StandardCharsets;

import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.math.ec.ECCurve;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WrappedStatement;

public class MVERIFY extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final ECVERIFY ECVERIFY = new ECVERIFY(WarpScriptLib.ECVERIFY);

  private final boolean verify;

  public MVERIFY(String name, boolean verify) {
    super(name);
    this.verify = verify;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a macro.");
    }

    Macro macro = (Macro) top;

    boolean verified = verify(macro);

    stack.push(macro);

    if (!verify) {
      stack.push(verified);
    } else {
      if (!verified) {
        throw new WarpScriptException(getName() + " unable to verify macro.");
      }
    }

    return stack;
  }

  public static boolean verify(Macro macro) throws WarpScriptException {
    //
    // A Macro can be verified if its last 4 statements are a curve name, a public key, a signature
    // and an instance of MSIG
    //

    Macro signature = MSIG.getSignature(macro);

    if (4 != signature.size()) {
      return false;
    }

    ECNamedCurveParameterSpec spec = ECNamedCurveTable.getParameterSpec((String) signature.get(0));
    ECCurve curve = spec.getCurve();
    byte[] encoded = Hex.decode((String) signature.get(1));
    final ECPoint q = curve.decodePoint(encoded);

    ECPublicKey pubkey = new ECPublicKey() {
      public String getFormat() { return "PKCS#8"; }
      public byte[] getEncoded() { return encoded; }
      public String getAlgorithm() { return "EC"; }
      public ECParameterSpec getParameters() { return spec; }
      public ECPoint getQ() { return q; }
    };

    byte[] sig = Hex.decode((String) signature.get(2));

    //
    // Create a copy of the macro without the statements from the signature
    //

    Macro m = new Macro();

    for (int i = 0; i < macro.size() - signature.size(); i++) {
      //
      // If a single statement is a wrapped one, consider that the verification failed.
      // This is to prevent signatures from being successfully verified even though the
      // macro behavior would differ from the original one
      //

      if (WrappedStatement.class.isAssignableFrom(macro.get(i).getClass())) {
        return false;
      }
      m.add(macro.get(i));
    }

    //
    // SNAPSHOT the macro
    //

    String snapshot = m.snapshot(false);
    byte[] data = snapshot.getBytes(StandardCharsets.UTF_8);

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.push(data);
    stack.push(sig);
    stack.push(io.warp10.script.functions.MSIG.SIGALG);
    stack.push(pubkey);
    ECVERIFY.apply(stack);

    return Boolean.TRUE.equals(stack.pop());
  }
}
