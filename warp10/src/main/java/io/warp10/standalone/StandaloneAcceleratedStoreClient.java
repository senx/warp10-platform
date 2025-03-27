//
//   Copyright 2020-2025  SenX S.A.S.
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.CustomThreadFactory;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.KVFetchRequest;
import io.warp10.continuum.store.thrift.data.KVStoreRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;

public class StandaloneAcceleratedStoreClient implements StoreClient {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneAcceleratedStoreClient.class);

  private final StoreClient persistent;
  private final StandaloneChunkedMemoryStore cache;
  private final boolean ephemeral;

  private static StandaloneAcceleratedStoreClient instance = null;

  public StandaloneAcceleratedStoreClient(DirectoryClient dir, StoreClient persistentStore) {

    if (null != instance) {
      throw new RuntimeException(StandaloneAcceleratedStoreClient.class.getName() + " can only be instantiated once.");
    }

    //
    // Extract strategies from configuration
    //

    String defaultStrategy = WarpConfig.getProperty(Configuration.ACCELERATOR_DEFAULT_WRITE, "");

    if (defaultStrategy.contains(AcceleratorConfig.NOCACHE)) {
      AcceleratorConfig.defaultWriteNocache = true;
    } else if (defaultStrategy.contains(AcceleratorConfig.CACHE)) {
      AcceleratorConfig.defaultWriteNocache = false;
    }
    if (defaultStrategy.contains(AcceleratorConfig.NOPERSIST)) {
      AcceleratorConfig.defaultWriteNopersist = true;
    } else if (defaultStrategy.contains(AcceleratorConfig.PERSIST)) {
      AcceleratorConfig.defaultWriteNopersist = false;
    }

    defaultStrategy = WarpConfig.getProperty(Configuration.ACCELERATOR_DEFAULT_DELETE, "");

    if (defaultStrategy.contains(AcceleratorConfig.NOCACHE)) {
      AcceleratorConfig.defaultDeleteNocache = true;
    } else if (defaultStrategy.contains(AcceleratorConfig.CACHE)) {
      AcceleratorConfig.defaultDeleteNocache = false;
    }
    if (defaultStrategy.contains(AcceleratorConfig.NOPERSIST)) {
      AcceleratorConfig.defaultDeleteNopersist = true;
    } else if (defaultStrategy.contains(AcceleratorConfig.PERSIST)) {
      AcceleratorConfig.defaultDeleteNopersist = false;
    }

    defaultStrategy = WarpConfig.getProperty(Configuration.ACCELERATOR_DEFAULT_READ, "");

    if (defaultStrategy.contains(AcceleratorConfig.NOCACHE)) {
      AcceleratorConfig.defaultReadNocache = true;
    } else if (defaultStrategy.contains(AcceleratorConfig.CACHE)) {
      AcceleratorConfig.defaultReadNocache = false;
    }
    if (defaultStrategy.contains(AcceleratorConfig.NOPERSIST)) {
      AcceleratorConfig.defaultReadNopersist = true;
    } else if (defaultStrategy.contains(AcceleratorConfig.PERSIST)) {
      AcceleratorConfig.defaultReadNopersist = false;
    }

    //
    // Force accelerator parameters to be replicated on inmemory ones and clear other in memory params
    //

    WarpConfig.setProperty(Configuration.IN_MEMORY_CHUNK_COUNT, WarpConfig.getProperty(Configuration.ACCELERATOR_CHUNK_COUNT));
    WarpConfig.setProperty(Configuration.IN_MEMORY_CHUNK_LENGTH, WarpConfig.getProperty(Configuration.ACCELERATOR_CHUNK_LENGTH));
    WarpConfig.setProperty(Configuration.IN_MEMORY_EPHEMERAL, WarpConfig.getProperty(Configuration.ACCELERATOR_EPHEMERAL));
    WarpConfig.setProperty(Configuration.STANDALONE_MEMORY_GC_PERIOD, WarpConfig.getProperty(Configuration.ACCELERATOR_GC_PERIOD));
    WarpConfig.setProperty(Configuration.STANDALONE_MEMORY_GC_MAXALLOC, WarpConfig.getProperty(Configuration.ACCELERATOR_GC_MAXALLOC));

    WarpConfig.setProperty(Configuration.STANDALONE_MEMORY_STORE_LOAD, null);
    WarpConfig.setProperty(Configuration.STANDALONE_MEMORY_STORE_DUMP, null);

    this.ephemeral = "true".equals(WarpConfig.getProperty(Configuration.IN_MEMORY_EPHEMERAL));

    if (!this.ephemeral && (null == WarpConfig.getProperty(Configuration.ACCELERATOR_CHUNK_COUNT)
        || null == WarpConfig.getProperty(Configuration.ACCELERATOR_CHUNK_LENGTH))) {
      throw new RuntimeException("Missing configuration key '" + Configuration.ACCELERATOR_CHUNK_COUNT + "' or '" + Configuration.ACCELERATOR_CHUNK_LENGTH + "'");
    }

    this.persistent = persistentStore;
    this.cache = new StandaloneChunkedMemoryStore(WarpConfig.getProperties(), Warp.getKeyStore());

    //
    // Preload the cache
    //

    long nanos = System.nanoTime();

    DirectoryRequest request = new DirectoryRequest();
    request.addToClassSelectors("~.*");
    Map<String,String> labelselectors = new HashMap<String,String>();
    labelselectors.put(Constants.APPLICATION_LABEL, "~.*");
    labelselectors.put(Constants.PRODUCER_LABEL, "~.*");
    labelselectors.put(Constants.OWNER_LABEL, "~.*");
    request.addToLabelsSelectors(labelselectors);

    long end;
    long start;
    long n = -1L;

    if (this.ephemeral) {
      end = Long.MAX_VALUE;
      start = Long.MIN_VALUE;
      n = 1L;
    } else {
      end = InMemoryChunkSet.chunkEnd(TimeSource.getTime(), this.cache.getChunkSpan());
      start = end - this.cache.getChunkCount() * this.cache.getChunkSpan() + 1;
      n = -1L;
    }

    final long now = end;
    final long then = start;
    final long count = n;

    boolean preload = "true".equals(WarpConfig.getProperty(Configuration.ACCELERATOR_PRELOAD));

    if ("true".equals(WarpConfig.getProperty(Configuration.ACCELERATOR_PRELOAD_ACTIVITY))) {
      long activityWindow = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_ACTIVITY_WINDOW, "-1"));
      if (activityWindow > 0) {
        request.setActiveAfter(then / Constants.TIME_UNITS_PER_MS - activityWindow - 1L);
      }
    }

    if (preload) {
      try {
        final AtomicLong datapoints = new AtomicLong();

        MetadataIterator iter = dir.iterator(request);

        int BATCH_SIZE = Integer.parseInt(WarpConfig.getProperty(Configuration.ACCELERATOR_PRELOAD_BATCHSIZE, "1000"));
        List<Metadata> batch = new ArrayList<Metadata>(BATCH_SIZE);

        int nthreads = Integer.parseInt(WarpConfig.getProperty(Configuration.ACCELERATOR_PRELOAD_POOLSIZE, "8"));

        ThreadPoolExecutor exec = new ThreadPoolExecutor(nthreads, nthreads, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(nthreads), new CustomThreadFactory("Warp StandaloneAcceleratedStoreClient Thread"));
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

        while(iter.hasNext()) {
          batch.add(iter.next());

          if (null != error.get()) {
            throw new RuntimeException("Error populating the accelerator", error.get());
          }

          if (BATCH_SIZE == batch.size() || !iter.hasNext()) {

            final List<Metadata> fbatch = batch;

            Runnable runnable = new Runnable() {
              @Override
              public void run() {
                GTSDecoderIterator decoders = null;
                try {
                  FetchRequest req = new FetchRequest();
                  req.setMetadatas(fbatch);
                  req.setCount(count);
                  req.setNow(now);
                  req.setThents(then);
                  decoders = persistent.fetch(req);

                  while(decoders.hasNext()) {
                    GTSDecoder decoder = decoders.next();
                    decoder.next();
                    GTSEncoder encoder = decoder.getEncoder(true);
                    cache.store(encoder);
                    datapoints.addAndGet(decoder.getCount());
                  }
                } catch (Exception e) {
                  error.set(e);
                  throw new RuntimeException(e);
                } finally {
                  if (null != decoders) {
                    try {
                      decoders.close();
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
                }
              }
            };

            boolean submitted = false;
            while(!submitted) {
              try {
                exec.execute(runnable);
                submitted = true;
              } catch (RejectedExecutionException re) {
                LockSupport.parkNanos(100000000L);
              }
            }

            batch = new ArrayList<Metadata>(BATCH_SIZE);
          }
        }

        exec.shutdown();

        while(true) {
          try {
            if (exec.awaitTermination(30, TimeUnit.SECONDS)) {
              break;
            }
          } catch (InterruptedException ie) {
          }
        }

        LOG.info("Preloaded accelerator with " + datapoints + " datapoints from " + this.cache.getGTSCount() + " Geo Time Series in " + ((System.nanoTime() - nanos) / 1000000.0D) + " ms.");
      } catch (IOException ioe) {
        throw new RuntimeException("Error populating cache.", ioe);
      }
    } else {
      LOG.info("Skipping accelerator preloading.");
    }

    instance = this;
    AcceleratorConfig.setChunkCount(instance.cache.getChunkCount());
    AcceleratorConfig.setChunkSpan(instance.cache.getChunkSpan());
    AcceleratorConfig.instantiated();
  }

  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {
    this.persistent.addPlasmaHandler(handler);
  }

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    if (!AcceleratorConfig.nocache.get()) {
      cache.delete(token, metadata, start, end);
    }
    if (!AcceleratorConfig.nopersist.get()) {
      persistent.delete(token, metadata, start, end);
    }
    return 0;
  }

  @Override
  public GTSDecoderIterator fetch(FetchRequest req) throws IOException {
    //
    // If the fetch has both a time range that is larger than the cache range, we will only use
    // the persistent backend to ensure a correct fetch. Same goes with boundaries which could extend outside the
    // cache.
    //
    // Note that this is a heuristic which could still lead to missing datapoints as data with timestamps within
    // the current cache time range could very well have been written to the persistent store and not preloaded
    // at cache startup if they were not in an active chunk of the cache.
    //

    long cacheend = InMemoryChunkSet.chunkEnd(TimeSource.getTime(), this.cache.getChunkSpan());
    long cachestart = cacheend - this.cache.getChunkCount() * this.cache.getChunkSpan() + 1;

    //
    // If fetching a single value from Long.MAX_VALUE with an ephemeral cache, always use the cache
    // unless ACCEL.NOCACHE was called.
    //

    if (this.ephemeral && 1L == req.getCount() && Long.MAX_VALUE == req.getNow() && !AcceleratorConfig.nocache.get()) {
      AcceleratorConfig.accelerated.set(Boolean.TRUE);
      return this.cache.fetch(req);
    }

    // Use the persistent store if the accelerator is in ephemeral mode,
    // if the requested time range is larger than the accelerated range or
    // if boundaries were requested, unless ACCEL.NOPERSIST was called
    if ((this.ephemeral || (req.getNow() > cacheend || req.getThents() < cachestart) || req.getPreBoundary() > 0 || req.getPostBoundary() > 0 || AcceleratorConfig.nocache.get()) && !AcceleratorConfig.nopersist.get()) {
      AcceleratorConfig.accelerated.set(Boolean.FALSE);
      return this.persistent.fetch(req);
    }

    // Last resort, use the cache, unless it is disabled in which case an exception is thrown
    if (AcceleratorConfig.nocache.get()) {
      throw new IOException("Cache and persistent store access disabled.");
    }

    AcceleratorConfig.accelerated.set(Boolean.TRUE);
    return this.cache.fetch(req);
  }

  @Override
  public void store(GTSEncoder encoder) throws IOException {
    if (!AcceleratorConfig.nopersist.get()) {
      persistent.store(encoder);
    }

    if (!AcceleratorConfig.nocache.get()) {
      cache.store(encoder);
    }
  }

  @Override
  public void kvstore(KVStoreRequest request) throws IOException {
    persistent.kvstore(request);
  }

  @Override
  public KVIterator<Entry<byte[], byte[]>> kvfetch(KVFetchRequest request) throws IOException {
    return persistent.kvfetch(request);
  }
}
