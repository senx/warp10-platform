package io.warp10.standalone.wal;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.standalone.StandaloneDirectoryClient;

public class WALStandaloneDirectoryClient extends StandaloneDirectoryClient {
  
  private final WALManager manager;
  private final StandaloneDirectoryClient directory;
  
  public WALStandaloneDirectoryClient(WALManager manager, StandaloneDirectoryClient sdc) {
    this.manager = manager;
    this.directory = sdc;
  }

  @Override
  public List<Metadata> find(DirectoryRequest request) {
    return directory.find(request);
  }

  @Override
  public void register(Metadata metadata) throws IOException {
    manager.register(metadata);
    directory.register(metadata);
  }

  @Override
  public synchronized void unregister(Metadata metadata) {
    manager.unregister(metadata);
    directory.unregister(metadata);
  }

  @Override
  public Metadata getMetadataById(BigInteger id) {
    return directory.getMetadataById(id);
  }

  @Override
  public Map<String, Object> stats(DirectoryRequest dr) throws IOException {
    return directory.stats(dr);
  }

  @Override
  public Map<String, Object> stats(DirectoryRequest dr, ShardFilter filter) throws IOException {
    return directory.stats(dr, filter);
  }

  @Override
  public MetadataIterator iterator(DirectoryRequest request) throws IOException {
    return directory.iterator(request);
  }

  @Override
  public void setActivityWindow(long activityWindow) {
    directory.setActivityWindow(activityWindow);
  }
}
