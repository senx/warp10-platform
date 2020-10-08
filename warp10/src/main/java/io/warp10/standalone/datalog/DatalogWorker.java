//
//   Copyright 2020  SenX S.A.S.
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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;

import io.warp10.continuum.egress.EgressExecHandler;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
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
    StoreClient storeClient = EgressExecHandler.getExposedStoreClient();
    StandaloneDirectoryClient directoryClient = (StandaloneDirectoryClient) EgressExecHandler.getExposedDirectoryClient();
    
    while(true) {
      
      DatalogJob job = null;
      DatalogRecord record = null;

      try {
        job = queue.poll();
        
        if (null == job) {
          LockSupport.parkNanos(10000000L);
          continue;
        }
        
        record = job.record;
        
        System.out.println(job.record);
/*
        switch (job.record.getType()) {
          case UPDATE:
            GTSDecoder decoder = new GTSDecoder(record.getBaseTimestamp(), record.bufferForEncoder());
            decoder.next();
            storeClient.store(decoder.getEncoder());
            job.consumer.success(job.ref);
            break;
          case REGISTER:
            directoryClient.register(record.getMetadata());
            job.consumer.success(job.ref);
            break;
          case UNREGISTER:
            directoryClient.unregister(record.getMetadata());
            job.consumer.success(job.ref);
            break;
          case DELETE:
            storeClient.delete(null, record.getMetadata(), record.getStart(), record.getStop());
            job.consumer.success(job.ref);
            break;
        }
*/
      } catch (Throwable t) {
        if (null != job) {
          job.consumer.failure(job.ref);
        }
      } finally {        
      }
    }
  }
}
