//
//   Copyright 2018-2020  SenX S.A.S.
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

package io.warp10.standalone;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.sensision.Sensision;

public class StandaloneSnapshotManager extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneSnapshotManager.class);

  /**
   * File path to watch for triggering suspension of compactions
   */
  private final String triggerPath;

  /**
   * File path to create to notify the external process that compactions are suspended
   */
  private final String signalPath;

  public StandaloneSnapshotManager(String triggerPath, String signalPath) {
    this.triggerPath = triggerPath;
    this.signalPath = signalPath;

    //
    // Check if any of the files already exists, if so emit a warning
    //
    if (new File(triggerPath).exists()) {
      LOG.warn("Snapshot trigger file '" + triggerPath + "' exists at startup.");
    }
    if (new File(signalPath).exists()) {
      LOG.warn("Snapshot signal file '" + signalPath + "' exists at startup.");
    }
  }

  @Override
  public void run() {
    while (true) {

      //
      // Exit if db is not set
      //

      if (null == Warp.getDB()) {
        break;
      }

      //
      // Sleep for 1s
      //

      LockSupport.parkNanos(1000000000);

      //
      // Check if the trigger file for backup exists
      //

      final File trigger = new File(triggerPath);

      if (!trigger.exists()) {
        continue;
      }

      LOG.info("Snapshot trigger file '" + triggerPath + "' detected, closing LevelDB.");

      final long nanos = System.nanoTime();

      //
      // Trigger path exists, close DB, wait for 
      //

      final WarpDB db = Warp.getDB();

      try {
        db.doOffline(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            //
            // Signal that DB is closed by creating the signalPath
            //

            File signal = new File(signalPath);

            try {
              LOG.info("Creating snapshot signal file '" + signalPath + "'.");
              signal.createNewFile();
              LOG.info("Snapshot signal file '" + signalPath + "' created, waiting for trigger file '" + triggerPath + "' to disappear.");
            } catch (IOException ioe) {
              LOG.error("Error creating snapshot signal file '" + signalPath + "', waiting for trigger file '" + triggerPath + "' to disappear before re-opening LevelDB.", ioe);
            }

            //
            // Wait until the trigger file has vanished
            //

            while (trigger.exists()) {
              LockSupport.parkNanos(100000000L);
            }

            LOG.info("Snapshot trigger file '" + triggerPath + "' disappeared, reopening LevelDB.");

            //
            // Return so we re-open the DB
            //

            long nano = System.nanoTime() - nanos;

            Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_STANDALONE_LEVELDB_SNAPSHOT_REQUESTS, Sensision.EMPTY_LABELS, 1);
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_STANDALONE_LEVELDB_SNAPSHOT_TIME_NS, Sensision.EMPTY_LABELS, nano);

            //
            // Remove the signal file
            //

            LOG.info("Deleting snapshot signal file '" + signalPath + "'.");
            signal.delete();
            LOG.info("Snapshot signal file '" + signalPath + "' deleted.");

            return null;
          }
        });
      } catch (IOException ioe) {
        LOG.error("Error while attempting to close LevelDB.", ioe);
      }
    }
  }
}
