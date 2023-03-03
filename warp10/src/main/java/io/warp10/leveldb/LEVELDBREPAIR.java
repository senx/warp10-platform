//
//   Copyright 2020-2023  SenX S.A.S.
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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.iq80.leveldb.Options;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.Warp;
import io.warp10.warp.sdk.Capabilities;

public class LEVELDBREPAIR extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public LEVELDBREPAIR(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_ADMIN) && null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_REPAIR)) {
      throw new WarpScriptException(getName() + " missing capability '" + WarpScriptStack.CAPABILITY_LEVELDB_ADMIN + "' or '" + WarpScriptStack.CAPABILITY_LEVELDB_REPAIR + "'.");
    }

    final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);

    try {
      WarpDB db = Warp.getDB();

      if (null == db) {
        throw new WarpScriptException(getName() + " can only be called when using LevelDB.");
      }

      // Extract DB home
      final String leveldbhome = db.getHome();
      final Options options = db.getOptions();
      final Boolean isJavaDisabled = db.isJavaDisabled();
      final Boolean isNativeDisabled = db.isNativeDisabled();

      //
      // Call repair
      //

      Callable c = new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          try {
            WarpRepair.repair(leveldbhome, options, isJavaDisabled, isNativeDisabled);
            return true;
          } catch (Throwable t) {
            error.set(t);
            throw t;
          }
        }
      };

      Boolean done = (Boolean) db.doOffline(c);

      return stack;
    } catch (Throwable t) {
      if (null != error.get()) {
        t = error.get();
      }
      throw new WarpScriptException(getName() + " encountered an exception while attempting to repair LevelDB.", t);
    }
  }
}
