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

package io.warp10.standalone.datalog;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import io.warp10.WarpConfig;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DatalogRecord;

public class DatalogWorkers {

  static class DatalogJob {
    final DatalogConsumer consumer;
    final String ref;
    final DatalogRecord record;

    public DatalogJob(DatalogConsumer consumer, String ref, DatalogRecord record) {
      this.consumer = consumer;
      this.ref = ref;
      this.record = record;
    }
  }

  private static final LinkedBlockingQueue<DatalogJob> queues[];
  private static final DatalogWorker[] workers;

  private static StoreClient store = null;
  private static DirectoryClient directory = null;

  private static final int NUM_WORKERS;

  static {

    NUM_WORKERS = Integer.parseInt(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_NWORKERS, "1"));

    queues = new LinkedBlockingQueue[NUM_WORKERS];
    workers = new DatalogWorker[NUM_WORKERS];
  }

  public static void init(StoreClient store, DirectoryClient directory) {

    if (null != DatalogWorkers.store || null != DatalogWorkers.directory) {
      throw new RuntimeException("Store/Directory already set.");
    }

    DatalogWorkers.store = store;
    DatalogWorkers.directory = directory;

    for (int i = 0; i < NUM_WORKERS; i++) {
      queues[i] = new LinkedBlockingQueue<DatalogJob>();
      workers[i] = new DatalogWorker(queues[i]);
    }
  }

  public static void offer(DatalogConsumer consumer, String ref, DatalogRecord record) throws IOException {

    //
    // If record is null, force all workers to flush and return
    //

    if (null == record) {
      try {
        for (int i = 0; i < workers.length; i++) {
          workers[i].flush();
        }
      } catch (Throwable e) {
        throw new IOException(e);
      }
      return;
    }

    //
    // Compute partitioning key and partition.
    // Note: this has nothing to do with the shard id
    //

    long classid = record.getMetadata().getClassId();
    long labelsid = record.getMetadata().getLabelsId();

    long partkey = (((classid << 16) & 0xFFFF0000L) | ((labelsid >>> 48) & 0xFFFFL));
    long partition = partkey % NUM_WORKERS;

    DatalogJob job = new DatalogJob(consumer, ref, record);

    if (!queues[(int) partition].offer(job)) {
      throw new IOException("Unable to offer record '" + ref + "'.");
    }
  }

  public static StoreClient getStoreClient() {
    return DatalogWorkers.store;
  }

  public static DirectoryClient getDirectoryClient() {
    return DatalogWorkers.directory;
  }
}
