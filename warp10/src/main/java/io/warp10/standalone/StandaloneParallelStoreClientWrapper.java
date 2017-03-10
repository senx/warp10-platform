package io.warp10.standalone;

import java.io.IOException;
import java.util.List;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.ParallelGTSDecoderIteratorWrapper;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;

public class StandaloneParallelStoreClientWrapper implements StoreClient {
  private final StoreClient parent;
  
  public StandaloneParallelStoreClientWrapper(StoreClient parent) {
    this.parent = parent;
  }
  
  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {
    parent.addPlasmaHandler(handler);
  }
  
  @Override
  public void archive(int chunk, GTSEncoder encoder) throws IOException {
    parent.archive(chunk, encoder);
  }
  
  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    return parent.delete(token, metadata, start, end);
  }
  
  @Override
  public GTSDecoderIterator fetch(ReadToken token, List<Metadata> metadatas, long now, long timespan, boolean fromArchive, boolean writeTimestamp) throws IOException {
    if (ParallelGTSDecoderIteratorWrapper.useParallelScanners()) {
      return new ParallelGTSDecoderIteratorWrapper(parent, token, now, timespan, metadatas);
    } else {
      return parent.fetch(token, metadatas, now, timespan, fromArchive, writeTimestamp);
    }
  }
  
  @Override
  public void store(GTSEncoder encoder) throws IOException {
    parent.store(encoder);
  }
}
