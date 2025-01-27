//
//   Copyright 2022-2025  SenX S.A.S.
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

import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;

import com.apple.foundationdb.Database;

import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.sensision.Sensision;

/**
 * This is a pool of FoundationDB databases.
 * A client can use any of those databases in a non exclusive
 * way.
 * The objective is to spread the load across clients when FoundationDB
 * is launched with multiple clients (clients are affected to Database
 * instances in a round robin manner).
 *
 * So launching FoundationDB with the following environment variables set:
 *
 * export FDB_NETWORK_OPTION_EXTERNAL_CLIENT_LIBRARY=/path/to/libfdb_c.so
 * export FDB_NETWORK_OPTION_CLIENT_THREADS_PER_VERSION=N
 *
 * will start N network threads, if the number of Database instances in the
 * Warp 10 instance (sum of Databases created by all components) is <= N
 * then each Database will have its own client.
 *
 * Experimenting is key as sizing depends on the type of load of each deployment.
 */
public class FDBPool {
  private final FDBContext context;
  private final Database[] databases;
  private AtomicLong lastBusynessUpdate = new AtomicLong(0L);
  private int index = 0;

  public FDBPool(FDBContext context, int maxsize) {
    this.context = context;
    this.databases = new Database[maxsize];
    for (int i = 0; i < maxsize; i++) {
      this.databases[i] = context.getDatabase();
    }
  }

  /**
   * Return a Database instance selected
   * in a round robin manner from the pool instances
   */
  public synchronized Database getDatabase() {
    Database db = databases[index++];
    index = index % databases.length;
    return db;
  }

  public void updateDatabasesBusyness() {
    synchronized (lastBusynessUpdate) {
      if (System.currentTimeMillis() - lastBusynessUpdate.get() < 1000) {
        return;
      }
      lastBusynessUpdate.set(System.currentTimeMillis());
    }

    Map<String,String> labels = new HashMap<String, String>();

    for (int i = 0; i < databases.length; i++) {
        labels.put(SensisionConstants.SENSISION_LABEL_THREAD, Integer.toString(i));
        Sensision.set(SensisionConstants.SENSISION_CLASS_FDB_CLIENT_BUSYNESS, labels, databases[i].getMainThreadBusyness());
    }
  }

  public FDBContext getContext() {
    return this.context;
  }
}
