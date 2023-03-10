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

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.iq80.leveldb.impl.Filename;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.Warp;
import io.warp10.warp.sdk.Capabilities;

/**
 * Creates a full or incremental snapshot of the LevelDB data
 */
public class LEVELDBSNAPSHOT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean incremental;

  public LEVELDBSNAPSHOT(String name, boolean incremental) {
    super(name);
    this.incremental = incremental;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_ADMIN) && null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_SNAPSHOT)) {
      throw new WarpScriptException(getName() + " missing capability '" + WarpScriptStack.CAPABILITY_LEVELDB_ADMIN + "' or '" + WarpScriptStack.CAPABILITY_LEVELDB_SNAPSHOT + "'.");
    }

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a snapshot name.");
    }

    final String snapshotName = top.toString();
    String ref = null;

    if (this.incremental) {
      top = stack.pop();

      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects the name of the reference snapshot before the snapshot name.");
      }

      ref = top.toString();
    }

    final String snapshotRef = ref;

    final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);

    try {
      WarpDB db = Warp.getDB();

      if (null == db) {
        throw new WarpScriptException(getName() + " can only be called when using LevelDB.");
      }

      // Extract DB home
      final String leveldbhome = db.getHome();

      Callable c = new Callable<List<Long>>() {
        @Override
        public List<Long> call() throws Exception {
          try {
            return WarpSnapshot.snapshot(leveldbhome, snapshotName, snapshotRef);
          } catch (Throwable t) {
            error.set(t);
            throw t;
          }
        }
      };

      List<Long> commonsst = (List<Long>) db.doOffline(c);

      // Remove the number of links which were created
      long links = commonsst.remove(commonsst.size() - 1);

      if (null == snapshotRef && !commonsst.isEmpty()) {
        throw new WarpScriptException(getName() + " error while creating snapshot, some files were not linked.");
      }

      //
      // Now we must link the files common to the leveldb file dirs and the reference snapshot
      // We do that AFTER leveldb has been re-open
      //

      File dbhome = new File(leveldbhome);
      File snapshotdir = new File(dbhome, WarpSnapshot.SNAPSHOTS_DIR);
      File refdir = null;
      if (null != snapshotRef) {
        refdir = new File(snapshotdir, snapshotRef);
      }
      snapshotdir = new File(snapshotdir, snapshotName);

      for (long number: commonsst) {
        String filename = Filename.tableFileName(number);
        Files.createLink(new File(snapshotdir, filename).toPath(), new File(refdir, filename).toPath());
        links++;
      }

      stack.push(links);
    } catch (Throwable t) {
      if (null != error.get()) {
        t = error.get();
      }
      throw new WarpScriptException(getName() + " encountered an exception while attempting to snapshot LevelDB.", t);
    }

    return stack;
  }
}
