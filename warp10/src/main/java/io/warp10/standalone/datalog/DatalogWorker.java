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

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
import io.warp10.continuum.store.thrift.data.DatalogRecordType;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandaloneDirectoryClient;
import io.warp10.standalone.datalog.DatalogWorkers.DatalogJob;

public class DatalogWorker extends Thread {

  private final LinkedBlockingQueue<DatalogJob> queue;

  /**
   * How often should we flush the writes (in ms). This is so writes occur even if
   * there is a gap in the incoming data.
   */
  private static final long FLUSH_INTERVAL;

  /**
   * Should we attempt to register the series for every update operation. This can be
   * useful when starting a new consumer which does not know the series for which data
   * points are coming.
   */
  private static final boolean REGISTER_UPDATES;

  private boolean hasManager = false;

  private StoreClient storeClient = null;
  private StandaloneDirectoryClient directoryClient = null;
  private DatalogManager manager = null;

  static {
    FLUSH_INTERVAL = Long.parseLong(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_FLUSH_INTERVAL, "15000"));
    REGISTER_UPDATES = "true".equals(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_REGISTER_UPDATES));
  }

  public DatalogWorker(LinkedBlockingQueue<DatalogJob> queue) {
    this.queue = queue;

    this.setName("[Datalog Worker]");
    this.setDaemon(true);
    this.start();
  }

  @Override
  public void run() {
    storeClient = DatalogWorkers.getStoreClient();
    directoryClient = (StandaloneDirectoryClient) DatalogWorkers.getDirectoryClient();

    manager = null;

    if (storeClient instanceof DatalogStoreClient) {
      manager = ((DatalogStoreClient) storeClient).getDatalogManager();
    }
    this.hasManager = null != manager;

    Map<String,String> labels = new LinkedHashMap<String,String>();

    //
    // Tracking of when data/metadata records were last received
    // in order to force flushes if need be
    //

    long lastUpdateRecord = 0L;
    long lastDeleteRecord = 0L;
    long lastRegisterRecord = 0L;
    long lastUnregisterRecord = 0L;

    while(true) {

      DatalogJob job = null;
      DatalogRecord record = null;

      Throwable err = null;

      try {
        job = queue.poll();

        //
        // We periodically force a flush of the underlying storage
        //

        if (hasManager) {
          long now = System.currentTimeMillis();
          if (now - lastRegisterRecord > FLUSH_INTERVAL
              || now - lastUnregisterRecord > FLUSH_INTERVAL
              || now - lastUpdateRecord > FLUSH_INTERVAL
              || now - lastDeleteRecord > FLUSH_INTERVAL) {
            manager.process(null);
            lastRegisterRecord = now;
            lastUnregisterRecord = now;
            lastUpdateRecord = now;
            lastDeleteRecord = now;
          }
        } else {
          long now = System.currentTimeMillis();
          if (now - lastUpdateRecord > FLUSH_INTERVAL) {
            storeClient.store(null);
            lastUpdateRecord = now;
          }
          if (now - lastDeleteRecord > FLUSH_INTERVAL) {
            storeClient.delete(null, null, Long.MAX_VALUE, Long.MAX_VALUE);
            lastDeleteRecord = now;
          }
          if (now - lastRegisterRecord > FLUSH_INTERVAL) {
            directoryClient.register(null);
            lastRegisterRecord = now;
          }
          if (now - lastUnregisterRecord > FLUSH_INTERVAL) {
            directoryClient.unregister(null);
            lastUnregisterRecord = now;
          }
        }

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
              enc.setMetadata(record.getMetadata());
              WriteToken token = null;
              try {
                token = (WriteToken) WarpConfig.getThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN);
                WarpConfig.setThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN, record.getToken());
                storeClient.store(enc);
                if (REGISTER_UPDATES) {
                  enc.getMetadata().setSource(Configuration.INGRESS_METADATA_SOURCE);
                  directoryClient.register(enc.getMetadata());
                }
              } finally {
                if (null != token) {
                  WarpConfig.setThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN, token);
                } else {
                  WarpConfig.removeThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN);
                }
              }
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
              storeClient.delete(record.getToken(), metadata, record.getStart(), record.getStop());
              job.consumer.success(job.ref);
              break;
          }
        }
        switch(job.record.getType()) {
          case UPDATE:
            lastUpdateRecord = System.currentTimeMillis();
            break;
          case DELETE:
            lastDeleteRecord = System.currentTimeMillis();
            break;
          case REGISTER:
            lastRegisterRecord = System.currentTimeMillis();
            break;
          case UNREGISTER:
            lastUnregisterRecord = System.currentTimeMillis();
            break;
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

  public void flush() throws Exception {
    if (hasManager) {
      manager.process(null);
    } else {
      storeClient.store(null);
      storeClient.delete(null, null, Long.MAX_VALUE, Long.MAX_VALUE);
      directoryClient.register(null);
      directoryClient.unregister(null);
    }
  }
}
