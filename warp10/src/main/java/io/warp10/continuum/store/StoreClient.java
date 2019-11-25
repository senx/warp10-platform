//
//   Copyright 2018  SenX S.A.S.
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

package io.warp10.continuum.store;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandalonePlasmaHandlerInterface;

import java.io.IOException;
import java.util.List;

public interface StoreClient {
  public void store(GTSEncoder encoder) throws IOException;
  public void archive(int chunk, GTSEncoder encoder) throws IOException;
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException;
  public GTSDecoderIterator fetch(ReadToken token, final List<Metadata> metadatas, final long now, final long timespan, boolean fromArchive, boolean writeTimestamp, final int preBoundary, final int postBoundary) throws IOException;
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler);
}
