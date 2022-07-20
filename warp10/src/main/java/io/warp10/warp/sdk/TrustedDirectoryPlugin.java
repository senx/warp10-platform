//
//   Copyright 2022  SenX S.A.S.
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

package io.warp10.warp.sdk;

import java.util.Iterator;
import java.util.Map;

import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;

public abstract class TrustedDirectoryPlugin extends DirectoryPlugin {
  @Override
  public boolean delete(GTS gts) { throw new RuntimeException("Unsupported operation."); }
  @Override
  public GTSIterator find(int shard, String classSelector, Map<String, String> labelsSelectors) {  throw new RuntimeException("Unsupported operation."); }
  @Override
  public boolean store(String source, GTS gts) { throw new RuntimeException("Unsupported operation."); }

  public static abstract class MetadataIterator implements Iterator<Metadata>,AutoCloseable {}

  public abstract boolean store(Metadata metadata);
  public abstract MetadataIterator find(int shard, DirectoryRequest request);
  public abstract boolean delete(Metadata metadata);
  public boolean known(Metadata metadata) {
    return false;
  }
  public abstract void initialized();
}
