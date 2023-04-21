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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.Warp;
import io.warp10.warp.sdk.Capabilities;

/**
 * Modify a LevelDB MANIFEST to remove SST files
 */
public class SSTPURGE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  /**
   * Maximum number of SST files we may purge at once
   */
  private final int maxpurge;

  public SSTPURGE(String name) {
    super(name);
    maxpurge = Integer.parseInt(WarpConfig.getProperty(Configuration.LEVELDB_MAXPURGE_KEY, Integer.toString(Constants.LEVELDB_MAXPURGE_DEFAULT)));
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_ADMIN) && null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_PURGE)) {
      throw new WarpScriptException(getName() + " missing capability '" + WarpScriptStack.CAPABILITY_LEVELDB_ADMIN + "' or '" + WarpScriptStack.CAPABILITY_LEVELDB_PURGE + "'.");
    }

    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of SST file ids.");
    }

    final List<Long> sstfiles = new ArrayList<Long>();

    for (Object o: (List<Object>) top) {
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a list of SST file ids (LONGs).");
      }
      sstfiles.add((Long) o);
    }

    //
    // Check that the list has a valid size
    //

    if (sstfiles.size() > maxpurge) {
      throw new WarpScriptException(getName() + " can purge at most " + maxpurge + " SST files at once.");
    }

    final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);

    try {
      WarpDB db = Warp.getDB();

      if (null == db) {
        throw new WarpScriptException(getName() + " can only be called when using LevelDB.");
      }

      final String leveldbhome = db.getHome();

      Callable c = new Callable<List<Long>>() {
        @Override
        public List<Long> call() throws Exception {
          try {
            return WarpPurge.purge(leveldbhome, sstfiles);
          } catch (Throwable t) {
            error.set(t);
            throw t;
          }
        }
      };

      List<Long> deleted = (List<Long>) db.doOffline(c);

      stack.push(deleted);
    } catch (Throwable t) {
      if (null != error.get()) {
        t = error.get();
      }
      throw new WarpScriptException(getName() + " encountered an exception while attempting to purge SST files.", t);
    }

    return stack;
  }
}
