//
//   Copyright 2021  SenX S.A.S.
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

package io.warp10.continuum.egress;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;

public class MergeSortStreamingMetadataIterator extends MetadataIterator {

  private final StreamingMetadataIterator[] iterators;
  private final Metadata[] metadatas;
  private boolean done = false;

  public MergeSortStreamingMetadataIterator(long[] SIPHASH_PSK, DirectoryRequest request, List<URL> urls, boolean noProxy) {
    //
    // Check that the DirectoryRequest contains a single selector
    //

    if (1 != request.getClassSelectorsSize() || 1 != request.getLabelsSelectorsSize()) {
      throw new RuntimeException("Invalid number of selectors.");
    }

    iterators = new StreamingMetadataIterator[urls.size()];
    metadatas = new Metadata[urls.size()];

    for (int i = 0; i < iterators.length; i++) {
      List<URL> url = new ArrayList<URL>(1);
      url.add(urls.get(i));
      iterators[i] = new StreamingMetadataIterator(SIPHASH_PSK, request, url, noProxy);
    }
  }

  @Override
  public boolean hasNext() {

    if (done) {
      return false;
    }

    //
    // Ensure we have a pending Metadata for each iterator which is not done
    //

    boolean hasNext = false;

    for (int i = 0; i < iterators.length; i++) {
      if (null != iterators[i]) {
        if (null == metadatas[i]) {
          if (iterators[i].hasNext()) {
            metadatas[i] = iterators[i].next();
            hasNext = true;
          } else {
            // Iterator has no additional entry, close it and nullify it
            // so it is skipped in further calls to hasNext
            try {
              iterators[i].close();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            iterators[i] = null;
          }
        } else {
          hasNext = true;
        }
      }
    }

    done = !hasNext;

    return hasNext;
  }

  @Override
  public Metadata next() {
    //
    // Iterate over the metadatas array and return the entry with the lowest id
    //

    int idx = -1;

    for (int i = 0; i < metadatas.length; i++) {
      if (null == metadatas[i]) {
        continue;
      }
      if (-1 == idx || MetadataUtils.compare(metadatas[i], metadatas[idx]) < 0) {
        idx = i;
      }
    }

    if (-1 != idx) {
      Metadata metadata = metadatas[idx];
      metadatas[idx] = null;
      return metadata;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void close() throws Exception {
    Exception error = null;

    for (int i = 0; i < iterators.length; i++) {
      try {
        iterators[i].close();
      } catch (Exception e) {
        error = e;
      }
    }

    if (null != error) {
      throw error;
    }
  }
}
