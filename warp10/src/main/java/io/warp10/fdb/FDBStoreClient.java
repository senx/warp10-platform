//
//   Copyright 2022-2023  SenX S.A.S.
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

package io.warp10.fdb;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.MetadataIdComparator;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.MultiScanGTSDecoderIterator;
import io.warp10.continuum.store.ParallelGTSDecoderIteratorWrapper;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandalonePlasmaHandlerInterface;

public class FDBStoreClient implements StoreClient {

  private final FDBPool pool;

  public final KeyStore keystore;
  public final boolean FDBUseTenantPrefix;

  private static final String EGRESS_FDB_POOLSIZE_DEFAULT = Integer.toString(1);

  public FDBStoreClient(KeyStore keystore, Properties properties) throws IOException {
    this.keystore = keystore;
    this.FDBUseTenantPrefix = "true".equals(properties.getProperty(Configuration.FDB_USE_TENANT_PREFIX));

    if (FDBUseTenantPrefix && null != properties.getProperty(Configuration.EGRESS_FDB_TENANT)) {
      throw new RuntimeException("Cannot set '" + Configuration.EGRESS_FDB_TENANT + "' when '" + Configuration.FDB_USE_TENANT_PREFIX + "' is true.");
    }

    FDBContext context = FDBUtils.getContext(properties.getProperty(Configuration.EGRESS_FDB_CLUSTERFILE), properties.getProperty(Configuration.EGRESS_FDB_TENANT));
    int maxsize = Integer.parseInt(properties.getProperty(Configuration.EGRESS_FDB_POOLSIZE, EGRESS_FDB_POOLSIZE_DEFAULT));
    this.pool = new FDBPool(context, maxsize);
  }

  @Override
  public GTSDecoderIterator fetch(FetchRequest req) throws IOException {
    long preBoundary = req.getPreBoundary();
    long postBoundary = req.getPostBoundary();
    long step = req.getStep();
    long timestep = req.getTimestep();
    double sample = req.getSample();
    long skip = req.getSkip();
    long count = req.getCount();
    long then = req.getThents();
    List<Metadata> metadatas = req.getMetadatas();

    if (preBoundary < 0) {
      preBoundary = 0;
    }

    if (postBoundary < 0) {
      postBoundary = 0;
    }

    if (step < 1L) {
      step = 1L;
    }

    if (timestep < 1L) {
      timestep = 1L;
    }

    if (sample <= 0.0D || sample > 1.0D) {
      sample = 1.0D;
    }

    if (skip < 0L) {
      skip = 0L;
    }

    if (count < -1L) {
      count = -1L;
    }

    //
    // If we are fetching up to Long.MIN_VALUE, then don't fetch a pre boundary
    //
    if (Long.MIN_VALUE == then) {
      preBoundary = 0;
    }

    //
    // Determine the execution plan given the metadatas of the GTS we will be retrieving.
    // Some hints to choose the best plan:
    // # of different classes
    // # of different instances among each class
    // # among those instances, common labels
    //
    // TODO(hbs): we might have to gather statistics to better determine the exec plan


    //
    // Sort metadatas to optimize the range scans
    //

    Collections.sort(metadatas, MetadataIdComparator.COMPARATOR);

    // Remove Metadatas from FetchRequest otherwise new FetchRequest(req) will do a deep copy
    List<Metadata> lm = req.getMetadatas();
    req.unsetMetadatas();
    FetchRequest freq = new FetchRequest(req);
    req.setMetadatas(lm);
    freq.setMetadatas(lm);
    freq.setCount(count);
    freq.setSkip(skip);
    freq.setStep(step);
    freq.setTimestep(timestep);
    freq.setSample(sample);
    freq.setPreBoundary(preBoundary);
    freq.setPostBoundary(postBoundary);

    if (!freq.isParallelScanners() || metadatas.size() < ParallelGTSDecoderIteratorWrapper.getMinGTSPerScanner() || !ParallelGTSDecoderIteratorWrapper.useParallelScanners()) {
      return new MultiScanGTSDecoderIterator(this.FDBUseTenantPrefix, freq, this.pool, this.keystore);
    } else {
      return new ParallelGTSDecoderIteratorWrapper(this.FDBUseTenantPrefix, freq, this.pool, this.keystore);
    }
  }


  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public void store(GTSEncoder encoder) throws IOException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    throw new RuntimeException("Not Implemented.");
  }

  public FDBPool getPool() {
    return this.pool;
  }
}
