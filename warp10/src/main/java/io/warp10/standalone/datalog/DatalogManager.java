//
//   Copyright 2020-2022  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandaloneDirectoryClient;

/**
 * A DatalogManager as its name indicates will manage a Datalog file that can then be used
 * to update replicas or shards.
 *
 * The basic idea is to wrap a pair of StoreClient and DirectoryClient instances and record in
 * a file the modifications (UPDATE/DELETE/META) which were performed. A separated process will take
 * care of forwarding the changes to downstream instances.
 */
public abstract class DatalogManager {

  protected abstract void register(Metadata metadata) throws IOException;
  protected abstract void unregister(Metadata metadata) throws IOException;
  protected abstract void store(GTSEncoder encoder) throws IOException;
  protected abstract void delete(WriteToken token, Metadata metadata, long start, long end) throws IOException;

  /**
   * Method to process a record retrieved from a feeder. This is different from locally generated
   * events.
   * @param record
   * @throws IOException
   */
  protected abstract void process(DatalogRecord record) throws IOException;

  protected abstract StandaloneDirectoryClient wrap(StandaloneDirectoryClient sdc);
  protected abstract StoreClient wrap(StoreClient scc);

  protected abstract void replay(StandaloneDirectoryClient sdc, StoreClient scc);

  public static StandaloneDirectoryClient wrap(DatalogManager manager, StandaloneDirectoryClient sdc) {
    if (null == manager) {
      return sdc;
    } else {
      return manager.wrap(sdc);
    }
  }

  public static StoreClient wrap(DatalogManager manager, StoreClient scc) {
    if (null == manager) {
      return scc;
    } else {
      return manager.wrap(scc);
    }
  }

  public static void replay(DatalogManager manager, StandaloneDirectoryClient sdc, StoreClient scc) {
    if (null != manager) {
      manager.replay(sdc, scc);
    }
  }
}
