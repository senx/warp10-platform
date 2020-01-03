package io.warp10.standalone.wal;

import java.io.IOException;
import java.util.List;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandalonePlasmaHandlerInterface;

public class WALStoreClient implements StoreClient {

  private final WALManager manager;
  private final StoreClient store;
  
  public WALStoreClient(WALManager manager, StoreClient store) {
    this.manager = manager;
    this.store = store;
  }
  
  @Override
  public GTSDecoderIterator fetch(ReadToken token, List<Metadata> metadatas, long now, long then, long count, long skip, double sample, boolean writeTimestamp, int preBoundary, int postBoundary) throws IOException {
    return store.fetch(token, metadatas, now, then, count, skip, sample, writeTimestamp, preBoundary, postBoundary);
  }

  @Override
  public void store(GTSEncoder encoder) throws IOException {
    // CAUTION, StoreClient#store assumes that class and labels ids
    // HAVE BEEN computed
    store.store(encoder);
    manager.store(encoder);
  }

  @Override
  public void archive(int chunk, GTSEncoder encoder) throws IOException {
    store.archive(chunk, encoder);
  }

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    long result = store.delete(token, metadata, start, end);
    manager.delete(token, metadata, start, end);
    return result;
  }

  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface plasmaHandler) {
    store.addPlasmaHandler(plasmaHandler);
  }
}
