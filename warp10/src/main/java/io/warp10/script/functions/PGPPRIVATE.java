//
//   Copyright 2022  SenX S.A.S.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PGPPRIVATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public PGPPRIVATE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof PGPSecretKeyRing)) {
      throw new WarpScriptException(getName() + " expected a PGP secret key ring.");
    }

    PGPSecretKeyRing keyring = (PGPSecretKeyRing) top;

    // Extract ids of secret keys
    Set<Long> ids = new LinkedHashSet<Long>();
    Iterator<PGPSecretKey> iter = keyring.getSecretKeys();
    while(iter.hasNext()) {
      ids.add(iter.next().getKeyID());
    }

    List<String> keyids = new ArrayList<String>(ids.size());
    for (Long id: ids) {
      String keyid = "000000000000000" + Long.toHexString(id);
      keyids.add(keyid.substring(keyid.length() - 16, keyid.length()).toUpperCase());
    }

    stack.push(keyids);

    return stack;
  }
}
