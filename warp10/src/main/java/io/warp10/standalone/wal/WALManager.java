package io.warp10.standalone.wal;

import java.io.IOException;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandaloneDirectoryClient;

public abstract class WALManager {
  
  protected abstract void register(Metadata metadata) throws IOException;
  protected abstract void unregister(Metadata metadata) throws IOException; 
  protected abstract void store(GTSEncoder encoder) throws IOException;
  protected abstract void delete(WriteToken token, Metadata metadata, long start, long end) throws IOException;  
  protected abstract StandaloneDirectoryClient wrap(StandaloneDirectoryClient sdc);
  protected abstract StoreClient wrap(StoreClient scc);
  protected abstract void replay(StandaloneDirectoryClient sdc, StoreClient scc);
  
  public static StandaloneDirectoryClient wrap(WALManager manager, StandaloneDirectoryClient sdc) {
    if (null == manager) {
      return sdc;
    } else {
      return manager.wrap(sdc);
    }
  }
  
  public static StoreClient wrap(WALManager manager, StoreClient scc) {
    if (null == manager) {
      return scc;
    } else {
      return manager.wrap(scc);
    }
  }
  
  public static void replay(WALManager manager, StandaloneDirectoryClient sdc, StoreClient scc) {
    manager.replay(sdc, scc);
  }
}
