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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
import io.warp10.continuum.store.thrift.data.DatalogRecordType;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandaloneDirectoryClient;
import io.warp10.standalone.datalog.DatalogWorkers.DatalogJob;

public class DatalogWorker extends Thread {

  private final LinkedBlockingQueue<DatalogJob> queue;

  public DatalogWorker(LinkedBlockingQueue<DatalogJob> queue) {
    this.queue = queue;

    this.setName("[Datalog Worker]");
    this.setDaemon(true);
    this.start();
  }

  @Override
  public void run() {
    StoreClient storeClient = DatalogWorkers.getStoreClient();
    StandaloneDirectoryClient directoryClient = (StandaloneDirectoryClient) DatalogWorkers.getDirectoryClient();

    DatalogManager manager = null;
    if (storeClient instanceof DatalogStoreClient) {
      manager = ((DatalogStoreClient) storeClient).getDatalogManager();
    }
    boolean hasManager = null != manager;

    Map<String,String> labels = new LinkedHashMap<String,String>();

    while(true) {

      DatalogJob job = null;
      DatalogRecord record = null;

      Throwable err = null;

      try {
        job = queue.poll();

        if (null == job) {
          LockSupport.parkNanos(10000000L);
          continue;
        }

        record = job.record;

        if (hasManager) {
          manager.process(record);
          job.consumer.success(job.ref);
        } else {
          Metadata metadata = record.getMetadata();
          switch (job.record.getType()) {
            case UPDATE:
              GTSEncoder enc = null;
              GTSDecoder decoder = new GTSDecoder(record.getBaseTimestamp(), record.bufferForEncoder());
              decoder.next();
              enc = decoder.getEncoder();
              storeClient.store(enc);
              job.consumer.success(job.ref);
              break;
            case REGISTER:
              directoryClient.register(metadata);
              job.consumer.success(job.ref);
              break;
            case UNREGISTER:
              directoryClient.unregister(metadata);
              job.consumer.success(job.ref);
              break;
            case DELETE:
              storeClient.delete(null, metadata, record.getStart(), record.getStop());
              job.consumer.success(job.ref);
              break;
          }
        }
      } catch (Throwable t) {
        err = t;
        if (null != job) {
          job.consumer.failure(job.ref);
        }
      } finally {
        if (null != job) {
          labels.put(SensisionConstants.SENSISION_LABEL_TYPE, job.record.getType().name());
          if (null != err) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_FAILURES, labels, 1);
          } else {
            Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_SUCCESSES, labels, 1);
          }
        }
      }
    }
  }
}
