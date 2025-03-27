//
//   Copyright 2018-2025  SenX S.A.S.
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

package io.warp10.standalone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.KVFetchRequest;
import io.warp10.continuum.store.thrift.data.KVStoreRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;

public class NullStoreClient implements StoreClient {

  @Override
  public GTSDecoderIterator fetch(FetchRequest req) {
    return new GTSDecoderIterator() {
      @Override
      public void close() throws Exception {}
      @Override
      public GTSDecoder next() { throw new NoSuchElementException(); }
      @Override
      public boolean hasNext() { return false; }
    };
  }

  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {}

  @Override
  public void store(GTSEncoder encoder) throws IOException {}

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException { return 0L; }

  @Override
  public void kvstore(KVStoreRequest request) throws IOException {}

  @Override
  public KVIterator<Entry<byte[], byte[]>> kvfetch(KVFetchRequest request) throws IOException {
    return new KVIterator<Entry<byte[],byte[]>>() {
      @Override
      public boolean hasNext() {
        return false;
      }
      @Override
      public Entry<byte[], byte[]> next() {
        return null;
      }
      @Override
      public void close() throws Exception {
      }
    };
  }
}
