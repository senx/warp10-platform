package io.warp10.standalone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;

public class StandaloneAcceleratedStoreClient implements StoreClient {
  
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneAcceleratedStoreClient.class);
  
  private final StoreClient persistent;
  private final StandaloneChunkedMemoryStore cache;
  private final boolean ephemeral;

  public static final String ATTR_REPORT = "accel.report";
  public static final String ATTR_NOCACHE = "accel.nocache";
  public static final String ATTR_NOPERSIST = "accel.nopersist";
  
  public static final String NOCACHE = "nocache";
  public static final String NOPERSIST = "nopersist";
  
  public static final String ACCELERATOR_HEADER = "X-Warp10-Accelerator";
  
  /**
   * Was the last FETCH accelerated for the given Thread?
   */
  private static final ThreadLocal<Boolean> accelerated = new ThreadLocal<Boolean>() {
    protected Boolean initialValue() {
      return Boolean.FALSE;
    };
  };
  
  private static final ThreadLocal<Boolean> nocache = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  private static final ThreadLocal<Boolean> nopersist = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  private static final AtomicBoolean instantiated = new AtomicBoolean(false);
  
  public StandaloneAcceleratedStoreClient(DirectoryClient dir, StoreClient persistentStore) {
    
    instantiated.set(true);
    
    this.persistent = persistentStore;
    this.cache = new StandaloneChunkedMemoryStore(WarpConfig.getProperties(), Warp.getKeyStore());

    this.ephemeral = "true".equals(WarpConfig.getProperty(Configuration.IN_MEMORY_EPHEMERAL)); 
    
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
    
    if ("true".equals(WarpConfig.getProperty(Configuration.ACCELERATOR_PRELOAD_ACTIVITY))) {
      long activityWindow = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_ACTIVITY_WINDOW, "-1"));
      if (activityWindow > 0) {
        request.setActiveAfter(then / Constants.TIME_UNITS_PER_MS - activityWindow - 1L);
      }
    }
    
    try {
      final AtomicLong datapoints = new AtomicLong();
      
      MetadataIterator iter = dir.iterator(request);
      
      int BATCH_SIZE = Integer.parseInt(WarpConfig.getProperty(Configuration.ACCELERATOR_PRELOAD_BATCHSIZE, "1000"));
      List<Metadata> batch = new ArrayList<Metadata>(BATCH_SIZE);
            
      int nthreads = Integer.parseInt(WarpConfig.getProperty(Configuration.ACCELERATOR_PRELOAD_POOLSIZE, "8"));
      
      ThreadPoolExecutor exec = new ThreadPoolExecutor(nthreads, nthreads, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(nthreads));
      final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
      
      while(iter.hasNext()) {
        batch.add(iter.next());
        
        if (null != error.get()) {
          throw new RuntimeException("Error populating the accelerator", error.get());
        }
        
        if (BATCH_SIZE == batch.size()) {
          
          final List<Metadata> fbatch = batch;
          
          Runnable runnable = new Runnable() {            
            @Override
            public void run() {
              try {
                GTSDecoderIterator decoders = persistent.fetch(null, fbatch, now, then, count, 0, 1.0D, false, 0, 0);
                
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

      if (!batch.isEmpty()) {          
        GTSDecoderIterator decoders = this.persistent.fetch(null, batch, now, then, count, 0, 0.0, false, 0, 0);
        
        while(decoders.hasNext()) {
          GTSDecoder decoder = decoders.next();
          decoder.next();
          GTSEncoder encoder = decoder.getEncoder(true);
          this.cache.store(encoder);
          datapoints.addAndGet(decoder.getCount());
        }
      }

      exec.shutdown();
      
      System.out.println("Preloaded accelerator with " + datapoints + " datapoints from " + this.cache.getGTSCount() + " Geo Time Series in " + ((System.nanoTime() - nanos) / 1000000.0D) + " ms.");
      LOG.info("Preloaded accelerator with " + datapoints + " datapoints from " + this.cache.getGTSCount() + " Geo Time Series in " + ((System.nanoTime() - nanos) / 1000000.0D) + " ms.");
    } catch (IOException ioe) {
      throw new RuntimeException("Error populating cache.", ioe);
    }
  }
  
  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {
    this.persistent.addPlasmaHandler(handler);
  }
  
  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    cache.delete(token, metadata, start, end);
    persistent.delete(token, metadata, start, end);
    return 0;
  }
  
  @Override
  public GTSDecoderIterator fetch(ReadToken token, List<Metadata> metadatas, long now, long then, long count, long skip, double sample, boolean writeTimestamp, int preBoundary, int postBoundary) throws IOException {
    //
    // If the fetch has both a time range that is larger than the cache range, we will only use
    // the persistent backend to ensure a correct fetch. Same goes with boundaries which could extend outside the
    // cache.
    //
    // Note that this is a heuristic which could still lead to missing datapoints as data with timestamps within
    // the current cache time range could very well have been written to the persistent store and not preloaded
    // at cache startup if they were not in an active chunk of the cache.
    //
    
    long cacheend = InMemoryChunkSet.chunkEnd(now, this.cache.getChunkSpan());
    long cachestart = cacheend - this.cache.getChunkCount() * this.cache.getChunkSpan() + 1;

    //
    // If fetching a single value from Long.MAX_VALUE with an ephemeral cache, always use the cache
    // unless ACCEL.NOCACHE was called.
    //
    if (this.ephemeral && 1 == count && Long.MAX_VALUE == now && !nocache.get()) {
      accelerated.set(Boolean.TRUE);
      return this.cache.fetch(token, metadatas, now, then, count, skip, sample, writeTimestamp, preBoundary, postBoundary);      
    }
    
    // Use the persistent store unless ACCEL.NOPERSIST was called 
    if (((now > cacheend || then < cachestart) || preBoundary > 0 || postBoundary > 0 || nocache.get()) && !nopersist.get()) {
      accelerated.set(Boolean.FALSE);
      return this.persistent.fetch(token, metadatas, now, then, count, skip, sample, writeTimestamp, preBoundary, postBoundary);
    }
    
    // Last resort, use the cache
    accelerated.set(Boolean.TRUE);
    return this.cache.fetch(token, metadatas, now, then, count, skip, sample, writeTimestamp, preBoundary, postBoundary);
  }
  
  @Override
  public void store(GTSEncoder encoder) throws IOException {    
    if (!nopersist.get()) {
      persistent.store(encoder);
    }
    
    if (!nocache.get()) {
      cache.store(encoder);
    }
  }
  
  @Override
  public void archive(int chunk, GTSEncoder encoder) throws IOException {
    throw new IOException("Not Implemented");
  }
  
  public static final void nocache() {
    if (instantiated.get()) {
      nocache.set(Boolean.TRUE);
    }
  }
  
  public static final void cache() {
    if (instantiated.get()) {
      nocache.set(Boolean.FALSE);
    }
  }
  
  public static final void nopersist() {
    if (instantiated.get()) {
      nopersist.set(Boolean.TRUE);
    }
  }
  
  public static final void persist() {
    if (instantiated.get()) {   
      nopersist.set(Boolean.FALSE);
    }
  }
  
  public static final boolean accelerated() {
    if (instantiated.get()) {
      return accelerated.get();
    } else {
      return false;
    }
  }
}
