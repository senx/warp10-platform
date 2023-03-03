//
//   Copyright 2018-2023  SenX S.A.S.
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

package io.warp10.leveldb;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.Warp;
import io.warp10.warp.sdk.Capabilities;

/**
 * Re-open the LevelDB database, with the lock held by the WarpDB thread, LEVELDBCLOSE should have been called before
 */
public class LEVELDBOPEN extends NamedWarpScriptFunction implements WarpScriptStackFunction {


  public LEVELDBOPEN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_ADMIN) && null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_OPEN)) {
      throw new WarpScriptException(getName() + " missing capability '" + WarpScriptStack.CAPABILITY_LEVELDB_ADMIN + "' or '" + WarpScriptStack.CAPABILITY_LEVELDB_OPEN + "'.");
    }

    try {
      WarpDB db = Warp.getDB();

      if (null == db) {
        throw new WarpScriptException(getName() + " can only be called when using LevelDB.");
      }

      db.doOpen();
    } catch (Throwable t) {
      throw new WarpScriptException(getName() + " encountered an error while attempting to open the DB.", t);
    }

    return stack;
  }
}
