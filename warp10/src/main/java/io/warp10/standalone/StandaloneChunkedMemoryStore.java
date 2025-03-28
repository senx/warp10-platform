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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.bouncycastle.util.Arrays;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

import io.warp10.BytesUtils;
import io.warp10.CapacityExtractorOutputStream;
import io.warp10.ThriftUtils;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.StoreClient.KVIterator;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.KVFetchRequest;
import io.warp10.continuum.store.thrift.data.KVStoreRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.functions.KVSTORE;
import io.warp10.sensision.Sensision;

/**
 * This class implements an in memory store which handles data expiration
 * using chunks which can be discarded when they no longer belong to the
 * active data period.
 * <p>
 * Chunks can optionally be dumped to disk when discarded.
 */
public class StandaloneChunkedMemoryStore extends Thread implements StoreClient {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneChunkedMemoryStore.class);

  private final Map<BigInteger,InMemoryChunkSet> series;

  private final SortedMap<String,byte[]> kv;
  private final long MAXKVSIZE;

  private final AtomicLong kvsize = new AtomicLong(0L);

  private final List<StandalonePlasmaHandlerInterface> plasmaHandlers = new ArrayList<StandalonePlasmaHandlerInterface>();

  private StandaloneDirectoryClient directoryClient = null;

  /**
   * Length of chunks in time units
   */
  private final long chunkspan;

  /**
   * Number of chunks
   */
  private final int chunkcount;

  private final Properties properties;

  private final boolean ephemeral;

  private final long[] classKeyLongs;
  private final long[] labelsKeyLongs;

  public StandaloneChunkedMemoryStore(Properties properties, KeyStore keystore) {
    this.properties = properties;

    this.series = new MapMaker().concurrencyLevel(64).makeMap();
    this.kv = Collections.synchronizedSortedMap(new TreeMap<String, byte[]>());

    this.MAXKVSIZE = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.IN_MEMORY_MAXKVSIZE, "10000000"));
    if ("true".equals(properties.getProperty(io.warp10.continuum.Configuration.IN_MEMORY_EPHEMERAL))) {
      this.chunkcount = 1;
      this.chunkspan = Long.MAX_VALUE;
      this.ephemeral = true;
    } else {
      this.chunkcount = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.IN_MEMORY_CHUNK_COUNT, "3"));
      this.chunkspan = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.IN_MEMORY_CHUNK_LENGTH, Long.toString(Long.MAX_VALUE)));
      this.ephemeral = false;
    }

    this.labelsKeyLongs = SipHashInline.getKey(keystore.getKey(KeyStore.SIPHASH_LABELS));
    this.classKeyLongs = SipHashInline.getKey(keystore.getKey(KeyStore.SIPHASH_CLASS));

    //
    // Add a shutdown hook to dump the memory store on exit
    //

    if (null != properties.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_STORE_DUMP)) {

      final StandaloneChunkedMemoryStore self = this;
      final String path = properties.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_STORE_DUMP);
      Thread dumphook = new Thread() {
        @Override
        public void run() {
          try {
            self.dump(path);
          } catch (IOException ioe) {
            LOG.error("Error dumping the memory store.", ioe);
            throw new RuntimeException(ioe);
          }
        }
      };

      Runtime.getRuntime().addShutdownHook(dumphook);

      //
      // Make sure ShutdownHookManager is initialized, otherwise it will try to
      // register a shutdown hook during the shutdown hook we just registered...
      //

      ShutdownHookManager.get();
    }

    this.setDaemon(true);
    this.setName("[StandaloneChunkedMemoryStore Janitor]");
    this.setPriority(Thread.MIN_PRIORITY);
    this.start();
  }

  @Override
  public GTSDecoderIterator fetch(FetchRequest req) {
    final List<Metadata> metadatas = req.getMetadatas();
    final long now = req.getNow();
    final long then = req.getThents();
    final long count = req.getCount();
    final long skip = req.getSkip();
    long step = req.getStep();
    long timestep = req.getTimestep();
    final double sample = req.getSample();
    final long preBoundary = req.getPreBoundary();
    final long postBoundary = req.getPostBoundary();

    if (step > 1L || timestep > 1L) {
      throw new RuntimeException("Parameters 'step' and 'timestep' are not supported by the in-memory store.");
    }

    GTSDecoderIterator iterator = new GTSDecoderIterator() {

      private int idx = 0;

      private GTSDecoder decoder = null;

      private final CapacityExtractorOutputStream extractor = new CapacityExtractorOutputStream();

      @Override
      public void close() throws Exception {
      }

      @Override
      public void remove() {
      }

      @Override
      public GTSDecoder next() {
        GTSDecoder dec = this.decoder;
        this.decoder = null;
        return dec;
      }

      @Override
      public boolean hasNext() {

        if (null != this.decoder) {
          return true;
        }

        // 128 bits
        byte[] bytes = new byte[16];

        while (true) {
          if (idx >= metadatas.size()) {
            return false;
          }

          while (idx < metadatas.size()) {
            GTSHelper.fillGTSIds(bytes, 0, metadatas.get(idx).getClassId(), metadatas.get(idx).getLabelsId());

            BigInteger clslbls = new BigInteger(bytes);

            if (idx < metadatas.size() && series.containsKey(clslbls)) {
              InMemoryChunkSet chunkset = series.get(clslbls);

              try {
                GTSDecoder dec = chunkset.fetch(now, then, count, skip, sample, extractor, preBoundary, postBoundary);

                if (0 == dec.getCount()) {
                  idx++;
                  continue;
                }

                this.decoder = dec;
              } catch (IOException ioe) {
                this.decoder = null;
                return false;
              }

              // We force metadatas as they might not be set in the encoder (case when we consume data off Kafka)
              this.decoder.setMetadata(metadatas.get(idx));

              idx++;
              return true;
            }

            idx++;
          }

          idx++;
        }
      }
    };

    return iterator;
  }

  /**
   * CAUTION, this method assumes that class and labels Id HAVE BEEN
   * computed for 'encoder'
   */
  public void store(GTSEncoder encoder) throws IOException {

    if (null == encoder) {
      return;
    }

    byte[] bytes = new byte[16];

    Metadata meta = encoder.getMetadata();

    GTSHelper.fillGTSIds(bytes, 0, meta.getClassId(), meta.getLabelsId());

    BigInteger clslbls = new BigInteger(bytes);

    //
    // Retrieve the chunk for the current GTS
    //

    InMemoryChunkSet chunkset;

    synchronized (this.series) {
      chunkset = this.series.get(clslbls);

      //
      // We need to allocate a new chunk
      //

      if (null == chunkset) {
        chunkset = new InMemoryChunkSet(this.chunkcount, this.chunkspan, this.ephemeral);
        this.series.put(clslbls, chunkset);
      }
    }

    //
    // Store data
    //

    chunkset.store(encoder);

    //
    // Forward data to Plasma
    //

    for (StandalonePlasmaHandlerInterface plasmaHandler: this.plasmaHandlers) {
      if (plasmaHandler.hasSubscriptions()) {
        plasmaHandler.publish(encoder);
      }
    }
  }

  @Override
  public void run() {
    //
    // Loop endlessly over the series, cleaning them as they grow
    //

    long gcperiod = this.chunkspan;

    if (null != properties.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_GC_PERIOD)) {
      gcperiod = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_GC_PERIOD));
    }

    long maxalloc = Long.MAX_VALUE;

    if (null != properties.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_GC_MAXALLOC)) {
      maxalloc = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_GC_MAXALLOC));
    }

    long delayns = 1000000L * Math.min(Long.MAX_VALUE / 1000000L, gcperiod / Constants.TIME_UNITS_PER_MS);

    while (true) {
      // Do not reclaim data for ephemeral setups
      if (this.ephemeral) {
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GTS, Sensision.EMPTY_LABELS, this.series.size());
        LockSupport.parkNanos(30 * 1000000000L);
        continue;
      }

      LockSupport.parkNanos(delayns);

      List<BigInteger> metadatas = new ArrayList<BigInteger>(this.series.keySet());

      if (0 == metadatas.size()) {
        continue;
      }

      long now = TimeSource.getTime();

      long datapoints = 0L;
      long datapointsdelta = 0L;
      long bytes = 0L;
      long bytesdelta = 0L;

      CapacityExtractorOutputStream extractor = new CapacityExtractorOutputStream();

      long reclaimed = 0L;

      AtomicLong allocation = new AtomicLong(0L);

      boolean doreclaim = true;

      for (BigInteger key: metadatas) {
        InMemoryChunkSet chunkset = this.series.get(key);

        if (null == chunkset) {
          continue;
        }

        long beforeBytes = chunkset.getSize();

        datapointsdelta += chunkset.clean(now);

        //
        // Optimize the chunkset until we've reclaimed
        // so many bytes.
        //

        if (doreclaim) {
          try {
            reclaimed += chunkset.optimize(extractor, now, allocation);
          } catch (OutOfMemoryError oome) {
            // We encountered an OOM, this probably means that the GC has not yet
            // managed to free up some space, so we stop reclaiming data for this
            // run.
            doreclaim = false;
          }

          if (allocation.get() > maxalloc) {
            doreclaim = false;
          }
        }

        long count = chunkset.getCount();
        datapoints += count;

        long size = chunkset.getSize();
        bytesdelta += beforeBytes - size;
        bytes += size;

        //
        // If count is zero check in a safe manner if this is
        // still the case and if it is, remove the chunkset for
        // this GTS.
        // Note that it does not remove the GTS from the directory
        // as we do not hold a lock opening a critical section
        // where we can guarantee that no GTS is added to the
        // directory.
        //

        if (0 == count) {
          synchronized (this.series) {
            if (0 == chunkset.getCount()) {
              this.series.remove(key);
            }
          }
        }
      }

      //
      // Update the number of GC runs just before updating the number of bytes, so we reduce the probability that the two don't
      // change at the same time when polling the metrics (note that the probability still exists though)
      //

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_RUNS, Sensision.EMPTY_LABELS, 1);

      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_BYTES, Sensision.EMPTY_LABELS, bytes);
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_DATAPOINTS, Sensision.EMPTY_LABELS, datapoints);
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GTS, Sensision.EMPTY_LABELS, this.series.size());

      if (datapointsdelta > 0) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_DATAPOINTS, Sensision.EMPTY_LABELS, datapointsdelta);
      }

      if (bytesdelta > 0) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_BYTES, Sensision.EMPTY_LABELS, bytesdelta);
      }

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_RECLAIMED, Sensision.EMPTY_LABELS, reclaimed);
    }
  }

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    // Do nothing if Metadata is null, this is simply a marker to instruct some StoreClient (namely FDB) to
    // flush their mutations
    if (null == metadata) {
      return 0L;
    }

    //
    // Regen classId/labelsId
    //

    // 128BITS
    metadata.setLabelsId(GTSHelper.labelsId(this.labelsKeyLongs, metadata.getLabels()));
    metadata.setClassId(GTSHelper.classId(this.classKeyLongs, metadata.getName()));

    byte[] bytes = new byte[16];

    GTSHelper.fillGTSIds(bytes, 0, metadata.getClassId(), metadata.getLabelsId());

    BigInteger clslbls = new BigInteger(bytes);

    InMemoryChunkSet set = null;

    synchronized (this.series) {
      if (Long.MIN_VALUE == start && Long.MAX_VALUE == end) {
        this.series.remove(clslbls);
      } else {
        set = this.series.get(clslbls);
      }
    }

    if (null != set) {
      return set.delete(start, end);
    }

    return 0L;
  }

  public void addPlasmaHandler(StandalonePlasmaHandlerInterface plasmaHandler) {
    this.plasmaHandlers.add(plasmaHandler);
  }

  public void dump(String path) throws IOException {

    if (null == this.directoryClient) {
      return;
    }

    long nano = System.nanoTime();
    int gts = 0;
    long chunks = 0;
    long bytes = 0L;
    long datapoints = 0;

    Configuration conf = new Configuration();

    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();

    CompressionCodec Codec = new DefaultCodec();

    SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(new Path(path));
    SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(key.getClass());
    SequenceFile.Writer.Option optVal = SequenceFile.Writer.valueClass(value.getClass());
    SequenceFile.Writer.Option optCom = SequenceFile.Writer.compression(CompressionType.RECORD, Codec);

    SequenceFile.Writer writer = SequenceFile.createWriter(conf, optPath, optKey, optVal, optCom);

    TSerializer serializer = ThriftUtils.getTSerializer();

    System.out.println("Dumping memory to '" + path + "'.");

    try {
      for (Entry<BigInteger,InMemoryChunkSet> entry: this.series.entrySet()) {
        gts++;
        Metadata metadata = this.directoryClient.getMetadataById(entry.getKey());

        List<GTSDecoder> decoders = entry.getValue().getDecoders();

        //GTSEncoder encoder = entry.getValue().fetchEncoder(now, this.chunkcount * this.chunkspan);

        for (GTSDecoder decoder: decoders) {
          chunks++;
          GTSWrapper wrapper = new GTSWrapper();
          wrapper.setMetadata(metadata);

          wrapper.setBase(decoder.getBaseTimestamp());
          wrapper.setCount(decoder.getCount());
          datapoints += wrapper.getCount();

          byte[] data = serializer.serialize(wrapper);
          key.set(data, 0, data.length);

          ByteBuffer bb = decoder.getBuffer();

          ByteBuffer rwbb = ByteBuffer.allocate(bb.remaining());
          rwbb.put(bb);
          rwbb.rewind();
          value.set(rwbb.array(), rwbb.arrayOffset(), rwbb.remaining());

          bytes += key.getLength() + value.getLength();

          writer.append(key, value);
        }
      }
    } catch (IOException ioe) {
      LOG.error("Error writing the dump.", ioe);
      throw ioe;
    } catch (Exception e) {
      LOG.error("Error writing the dump.", e);
      throw new IOException(e);
    }

    writer.close();

    nano = System.nanoTime() - nano;

    System.out.println("Dumped " + gts + " GTS (" + chunks + " chunks, " + datapoints + " datapoints, " + bytes + " bytes) in " + (nano / 1000000.0D) + " ms.");
  }

  public void load() {
    //
    // Load data from the specified file
    //

    if (null != properties.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_STORE_LOAD)) {
      try {
        load(properties.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_STORE_LOAD));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  private void load(String path) throws IOException {

    long nano = System.nanoTime();
    long chunks = 0;
    long datapoints = 0;
    long bytes = 0L;

    Configuration conf = new Configuration();

    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();

    TDeserializer deserializer = ThriftUtils.getTDeserializer();

    SequenceFile.Reader.Option optPath = SequenceFile.Reader.file(new Path(path));

    SequenceFile.Reader reader = null;

    boolean failsafe = "true".equals(properties.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_STORE_LOAD_FAILSAFE));

    try {
      reader = new SequenceFile.Reader(conf, optPath);

      System.out.println("Loading '" + path + "' back in memory.");

      while (reader.next(key, value)) {
        chunks++;
        GTSWrapper wrapper = new GTSWrapper();
        deserializer.deserialize(wrapper, key.copyBytes());
        GTSEncoder encoder = new GTSEncoder(wrapper.getBase(), null, value.copyBytes());
        encoder.setCount(wrapper.getCount());
        datapoints += wrapper.getCount();
        bytes += value.getLength() + key.getLength();
        if (wrapper.isSetMetadata()) {
          encoder.safeSetMetadata(wrapper.getMetadata());
        } else {
          encoder.safeSetMetadata(new Metadata());
        }
        store(encoder);
        if (null != this.directoryClient) {
          this.directoryClient.register(encoder.getMetadata());
        }
      }
    } catch (FileNotFoundException fnfe) {
      LOG.warn("File '" + path + "' was not found, skipping.");
      return;
    } catch (IOException ioe) {
      if (!failsafe) {
        throw ioe;
      } else {
        LOG.error("Ignoring exception " + ioe.getMessage() + ".");
      }
    } catch (Exception e) {
      if (!failsafe) {
        throw new IOException(e);
      } else {
        LOG.error("Ignoring exception " + e.getMessage() + ".");
      }
    }

    if (reader != null) {
      reader.close();
    }

    nano = System.nanoTime() - nano;

    System.out.println("Loaded " + chunks + " chunks (" + datapoints + " datapoints, " + bytes + " bytes) in " + (nano / 1000000.0D) + " ms.");
  }

  public void setDirectoryClient(StandaloneDirectoryClient directoryClient) {
    this.directoryClient = directoryClient;
  }

  public long getChunkSpan() {
    return this.chunkspan;
  }

  public int getChunkCount() {
    return this.chunkcount;
  }

  public int getGTSCount() {
    return this.series.size();
  }

  @Override
  public void kvstore(KVStoreRequest request) throws IOException {

    if (null == request) {
      return;
    }

    WriteToken token = request.getToken();

    //
    // Extract kvprefix from token attributes
    //

    byte[] kvprefix = KVSTORE.KVPREFIX;

    if (token.getAttributesSize() > 0 && null != token.getAttributes().get(KVSTORE.ATTR_KVPREFIX)) {
      if (!token.getAttributes().get(KVSTORE.ATTR_KVPREFIX).isEmpty()) {
        byte[] tkvprefix = OrderPreservingBase64.decode(token.getAttributes().get(KVSTORE.ATTR_KVPREFIX));
        kvprefix = Arrays.copyOf(KVSTORE.KVPREFIX, KVSTORE.KVPREFIX.length + tkvprefix.length);
        System.arraycopy(tkvprefix, 0, kvprefix, KVSTORE.KVPREFIX.length, tkvprefix.length);
      }
    } else {
      throw new IOException("Token is missing attribute '" + KVSTORE.ATTR_KVPREFIX + "'.");
    }

    // Iterate over the keys, missing or empty values will remove the entries
    TreeMap<String,byte[]> kvs = new TreeMap<String,byte[]>();

    for (int i = 0; i < request.getKeysSize(); i++) {
      ByteBuffer kbb = request.getKeys().get(i);
      kbb.mark();
      byte[] key = Arrays.copyOf(kvprefix, kvprefix.length + kbb.remaining());
      kbb.get(key, kvprefix.length, kbb.remaining());
      kbb.reset();
      if (i < request.getValuesSize()) {
        ByteBuffer vbb = request.getValues().get(i);
        vbb.mark();
        // If the associated value is empty, delete the entry under 'key'
        if (0 == vbb.remaining()) {
          byte[] old = this.kv.remove(OrderPreservingBase64.encodeToString(key));
          if (null != old) {
            kvsize.addAndGet(-old.length-key.length);
          }
        } else {
          byte[] value = new byte[vbb.remaining()];
          vbb.get(value);
          kvs.put(OrderPreservingBase64.encodeToString(key), value);
        }
        vbb.reset();
      } else {
        byte[] old = this.kv.remove(OrderPreservingBase64.encodeToString(key));
        if (null != old) {
          kvsize.addAndGet(-old.length-key.length);
        }
      }
    }
    if (!kvs.isEmpty()) {
      for (Entry<String,byte[]> entry: kvs.entrySet()) {
        byte[] old = kv.put(entry.getKey(), entry.getValue());
        if (null != old) {
          kvsize.addAndGet(entry.getValue().length-old.length);
        } else {
          kvsize.addAndGet(OrderPreservingBase64.decode(entry.getKey()).length + entry.getValue().length);
        }
        if (kvsize.get() > MAXKVSIZE) {
          throw new IOException("Some K/V pairs could not be added as it would have exceeded the configured total size limit. All deletions were performed though.");
        }
      }
    }
  }

  @Override
  public KVIterator<Entry<byte[], byte[]>> kvfetch(KVFetchRequest request) throws IOException {

    ReadToken token = request.getToken();

    //
    // Extract kvprefix from token attributes
    //

    byte[] kvprefix = KVSTORE.KVPREFIX;

    if (token.getAttributesSize() > 0 && null != token.getAttributes().get(KVSTORE.ATTR_KVPREFIX)) {
      if (!token.getAttributes().get(KVSTORE.ATTR_KVPREFIX).isEmpty()) {
        byte[] tkvprefix = OrderPreservingBase64.decode(token.getAttributes().get(KVSTORE.ATTR_KVPREFIX));
        kvprefix = Arrays.copyOf(KVSTORE.KVPREFIX, KVSTORE.KVPREFIX.length + tkvprefix.length);
        System.arraycopy(tkvprefix, 0, kvprefix, KVSTORE.KVPREFIX.length, tkvprefix.length);
      }
    } else {
      throw new IOException("Token is missing attribute '" + KVSTORE.ATTR_KVPREFIX + "'.");
    }

    final byte[] fkvprefix = kvprefix;

    ReadOptions options = new ReadOptions().fillCache(false);

    if (request.isSetStart() && request.isSetStop()) {
      byte[] start = Arrays.copyOf(kvprefix, kvprefix.length + request.getStart().length);
      System.arraycopy(request.getStart(), 0, start, kvprefix.length, request.getStart().length);
      byte[] stop = Arrays.copyOf(kvprefix, kvprefix.length + request.getStop().length);
      System.arraycopy(request.getStop(), 0, stop, kvprefix.length, request.getStop().length);

      SortedMap range = this.kv.subMap(OrderPreservingBase64.encodeToString(start), OrderPreservingBase64.encodeToString(stop));
      TreeSet keys = null;

      synchronized(this.kv) {
        keys = new TreeSet<String>(range.keySet());
      }

      final Iterator<String> iterator = keys.iterator();

      return new KVIterator<Entry<byte[],byte[]>>() {

        Entry<byte[],byte[]> element = null;
        boolean done = false;

        @Override
        public boolean hasNext() {
          if (null != element) {
            return true;
          }
          if (done) {
            return false;
          }
          while(null == element && iterator.hasNext()) {
            String key = iterator.next();
            element = new AbstractMap.SimpleEntry(OrderPreservingBase64.decode(key), kv.get(key));

            if (null == element.getValue()) {
              element = null;
            } else {
              // Strip prefixes from key
              element = new AbstractMap.SimpleEntry(Arrays.copyOfRange(element.getKey(), fkvprefix.length, element.getKey().length), element.getValue());
            }
          }

          if (null == element) {
            done = true;
          }
          return null != element;
        }

        @Override
        public Entry<byte[], byte[]> next() {
          if (done || null == element) {
            throw new IllegalStateException();
          }
          Entry<byte[],byte[]> elt = element;
          element = null;
          return elt;
        }

        @Override
        public void close() throws Exception {
          done = true;
        }
      };

    } else if (request.isSetKeys()) {
      // Fetch explicit keys
      return new KVIterator<Map.Entry<byte[],byte[]>>() {

        Entry<byte[],byte[]> element = null;
        boolean done = false;
        int idx = 0;

        @Override
        public boolean hasNext() {
          if (done) {
            return false;
          }
          if (null != element) {
            return true;
          }
          if (idx >= request.getKeysSize()) {
            done = true;
            return false;
          }

          while (idx < request.getKeysSize()) {
            ByteBuffer bb = request.getKeys().get(idx++);
            byte[] k = Arrays.copyOf(fkvprefix, fkvprefix.length + bb.remaining());
            bb.get(k, fkvprefix.length, bb.remaining());
            byte[] value = kv.get(OrderPreservingBase64.encodeToString(k));
            if (null != value) {
              // Strip prefix
              element = new AbstractMap.SimpleEntry<byte[],byte[]>(Arrays.copyOfRange(k, fkvprefix.length, k.length),value);
              break;
            }
          }

          if (null == element) {
            done = true;
          }
          return null != element;
        }

        @Override
        public void close() throws Exception {
          done = true;
        }

        @Override
        public Entry<byte[], byte[]> next() {
          if (done || null == element) {
            throw new IllegalStateException();
          }
          Entry<byte[],byte[]> elt = element;
          element = null;
          return elt;
        }
      };
    } else {
      return new KVIterator<Map.Entry<byte[],byte[]>>() {
        @Override
        public void close() throws Exception {}
        @Override
        public boolean hasNext() {
          return false;
        }
        @Override
        public Entry<byte[], byte[]> next() {
          throw new IllegalStateException();
        }
      };
    }
  }
}
