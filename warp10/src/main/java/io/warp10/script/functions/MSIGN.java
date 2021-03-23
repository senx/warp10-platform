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

import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class MSIGN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final ECSIGN ECSIGN = new ECSIGN(WarpScriptLib.ECSIGN);
  private static final ECPUBLIC ECPUBLIC = new ECPUBLIC(WarpScriptLib.ECPUBLIC);
  private static final MSIG MSIG = new MSIG(WarpScriptLib.MSIG);

  public MSIGN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof ECPrivateKey)) {
      throw new WarpScriptException(getName() + " expects an ECC private key.");
    }

    ECPrivateKey privateKey = (ECPrivateKey) top;

    top = stack.pop();

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a macro.");
    }

    Macro macro = (Macro) top;

    //
    // Now proceed to the signing of the macro.
    // We first snapshot the macro, then convert the snapshot
    // to its UTF-8 byte representation. We then sign it with
    // SHA256WITHECDSA using the provided private key.
    //

    String snapshot = macro.snapshot(false);

    byte[] data = snapshot.getBytes(StandardCharsets.UTF_8);

    MemoryWarpScriptStack stck = new MemoryWarpScriptStack(null, null);
    stck.push(data);
    stck.push(io.warp10.script.functions.MSIG.SIGALG);
    stck.push(privateKey);
    ECSIGN.apply(stck);
    byte[] sig = (byte[]) stck.pop();

    //
    // Determine public key from private key
    //

    stck.push(privateKey);
    ECPUBLIC.apply(stck);

    ECPublicKey pub = (ECPublicKey) stck.pop();

    Macro sigmacro = new Macro();

    sigmacro.add(((ECNamedCurveParameterSpec) pub.getParameters()).getName());
    sigmacro.add(Hex.encodeHexString(pub.getEncoded()));
    sigmacro.add(Hex.encodeHexString(sig));
    sigmacro.add(MSIG);

    stack.push(macro);
    stack.push(sigmacro);

    return stack;
  }

}
