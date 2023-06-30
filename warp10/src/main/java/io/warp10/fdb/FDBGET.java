//
//   Copyright 2022-2023  SenX S.A.S.
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

package io.warp10.fdb;

import io.warp10.continuum.store.StoreClient;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

public class FDBGET extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public FDBGET(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, FDBUtils.CAPABILITY_ADMIN) && null == Capabilities.get(stack, FDBUtils.CAPABILITY_GET)) {
      throw new WarpScriptException(getName() + " missing '" + FDBUtils.CAPABILITY_GET + "' or '" + FDBUtils.CAPABILITY_ADMIN + "' capability.");
    }

    StoreClient sc = stack.getStoreClient();

    if (!(sc instanceof FDBStoreClient)) {
      throw new WarpScriptException(getName() + " invalid store, not using FoundationDB!");
    }

    Object top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " expected a byte array as key.");
    }

    FDBStoreClient fsc = (FDBStoreClient) sc;

    FDBPool pool = fsc.getPool();

    stack.push(FDBUtils.getKey(pool.getDatabase(), (byte[]) top));

    return stack;
  }
}
