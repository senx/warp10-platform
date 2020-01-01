package io.warp10.standalone.wal;

import java.io.IOException;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandaloneDirectoryClient;

public class WALManager {
  
  public void register(Metadata metadata) throws IOException {
  }
  
  public void unregister(Metadata metadata) {    
  }
  
  public void store(GTSEncoder encoder) throws IOException {
  }
  
  public void delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
  }
  
  public static StandaloneDirectoryClient wrap(WALManager manager, StandaloneDirectoryClient sdc) {
    if (null == manager) {
      return sdc;
    } else {
      return new WALStandaloneDirectoryClient(manager, sdc);
    }
  }
  public static StoreClient wrap(WALManager manager, StoreClient scc) {
    if (null == manager) {
      return scc;
    } else {
      return new WALStoreClient(manager, scc);
    }
  }
}
