package io.warp10.standalone.datalog;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.standalone.StandaloneDirectoryClient;

public class DatalogStandaloneDirectoryClient extends StandaloneDirectoryClient {
  
  private final DatalogManager manager;
  private final StandaloneDirectoryClient directory;
  
  public DatalogStandaloneDirectoryClient(DatalogManager manager, StandaloneDirectoryClient sdc) {
    this.manager = manager;
    this.directory = sdc;
  }

  @Override
  public boolean register(Metadata metadata) throws IOException {
    // Make a copy of Metadata so we have the original attributes
    Metadata original = new Metadata(metadata);
    boolean stored = directory.register(metadata);
    // Only record the metadata update in the WAL if the directory
    // actually persisted it
    if (stored) {
      // Copy class/labels Id and lastactivity
      original.setClassId(metadata.getClassId());
      original.setLabelsId(metadata.getLabelsId());
      if (metadata.isSetLastActivity()) {
        original.setLastActivity(metadata.getLastActivity());
      }
      manager.register(original);
    }
    return stored;
  }

  @Override
  public List<Metadata> find(DirectoryRequest request) {
    return directory.find(request);
  }

  @Override
  public synchronized void unregister(Metadata metadata) throws IOException {
    //
    // The class id and labels id are not always recomputed by the unregister method
    // of the Directory but as the Metadata instances come from a Directory find
    // request they are already set.
    //
    directory.unregister(metadata);
    manager.unregister(metadata);
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
