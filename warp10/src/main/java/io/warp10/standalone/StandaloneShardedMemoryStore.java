//
//   Copyright 2017  Cityzen Data
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

import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.sensision.Sensision;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
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
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.collect.MapMaker;

/**
 * This class implements an in memory store which handles data expiration
 * using shards which can be discarded when they no longer belong to the
 * active data period.
 * 
 * Shards can optionally be dumped to disk when discarded.
 */
public class StandaloneShardedMemoryStore extends Thread implements StoreClient {
  
  private static final String STANDALONE_MEMORY_STORE_LOAD = "in.memory.load";
  private static final String STANDALONE_MEMORY_STORE_DUMP = "in.memory.dump";
  private static final String STANDALONE_MEMORY_GC_PERIOD = "in.memory.gcperiod";
  
  private final Map<BigInteger,InMemoryShardSet> series;
  
  private List<StandalonePlasmaHandlerInterface> plasmaHandlers = new ArrayList<StandalonePlasmaHandlerInterface>();

  private StandaloneDirectoryClient directoryClient = null;

  /**
   * Length of shards in time units
   */
  private final long shardspan;
  
  /**
   * Number of shards
   */
  private final int shardcount;

  private final Properties properties;

  private final long[] classKeyLongs;
  private final long[] labelsKeyLongs;
  
  public StandaloneShardedMemoryStore(Properties properties, KeyStore keystore) {
    this.properties = properties;

    this.series = new MapMaker().concurrencyLevel(64).makeMap();

    this.shardcount = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.IN_MEMORY_SHARD_COUNT, "3"));
    this.shardspan = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.IN_MEMORY_SHARD_LENGTH, Long.toString(Long.MAX_VALUE)));
  
    this.labelsKeyLongs = SipHashInline.getKey(keystore.getKey(KeyStore.SIPHASH_LABELS));
    this.classKeyLongs = SipHashInline.getKey(keystore.getKey(KeyStore.SIPHASH_CLASS));
    
    //
    // Add a shutdown hook to dump the memory store on exit
    //
    
    if (null != properties.getProperty(STANDALONE_MEMORY_STORE_DUMP)) {
      
      final StandaloneShardedMemoryStore self = this;
      final String path = properties.getProperty(STANDALONE_MEMORY_STORE_DUMP); 
      Thread dumphook = new Thread() {
        @Override
        public void run() {
          try {
            self.dump(path);
          } catch (IOException ioe) {
            ioe.printStackTrace();
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
    this.setName("[StandaloneShardedMemoryStore Janitor]");
    this.setPriority(Thread.MIN_PRIORITY);
    this.start();
  }
  
  @Override
  public GTSDecoderIterator fetch(final ReadToken token, final List<Metadata> metadatas, final long now, final long timespan, boolean fromArchive, boolean writeTimestamp) {
  
    GTSDecoderIterator iterator = new GTSDecoderIterator() {

      private int idx = 0;
      
      private GTSDecoder decoder = null;
      
      private long nvalues = Long.MAX_VALUE;
      
      @Override
      public void close() throws Exception {}
      
      @Override
      public void remove() {}
      
      @Override
      public GTSDecoder next() {
        return this.decoder;
      }
      
      @Override
      public boolean hasNext() {  
        
        byte[] bytes = new byte[16];
        
        while(true) {
          if (idx >= metadatas.size()) {
            return false;
          }
          
          while(idx < metadatas.size()) {
            long id = metadatas.get(idx).getClassId();
            
            int bidx = 0;
            
            bytes[bidx++] = (byte) ((id >> 56) & 0xff);
            bytes[bidx++] = (byte) ((id >> 48) & 0xff);
            bytes[bidx++] = (byte) ((id >> 40) & 0xff);
            bytes[bidx++] = (byte) ((id >> 32) & 0xff);
            bytes[bidx++] = (byte) ((id >> 24) & 0xff);
            bytes[bidx++] = (byte) ((id >> 16) & 0xff);
            bytes[bidx++] = (byte) ((id >> 8) & 0xff);
            bytes[bidx++] = (byte) (id & 0xff);
            
            id = metadatas.get(idx).getLabelsId();

            bytes[bidx++] = (byte) ((id >> 56) & 0xff);
            bytes[bidx++] = (byte) ((id >> 48) & 0xff);
            bytes[bidx++] = (byte) ((id >> 40) & 0xff);
            bytes[bidx++] = (byte) ((id >> 32) & 0xff);
            bytes[bidx++] = (byte) ((id >> 24) & 0xff);
            bytes[bidx++] = (byte) ((id >> 16) & 0xff);
            bytes[bidx++] = (byte) ((id >> 8) & 0xff);
            bytes[bidx++] = (byte) (id & 0xff);

            BigInteger clslbls = new BigInteger(bytes);
            
            if (idx < metadatas.size() && series.containsKey(clslbls)) {
              InMemoryShardSet shardset = series.get(clslbls);
              
              try {
                this.decoder = shardset.fetch(now, timespan);
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
  
  public void store(GTSEncoder encoder) throws IOException {
    
    if (null == encoder) {
      return;
    }

    byte[] bytes = new byte[16];
    
    Metadata meta = encoder.getMetadata();

    // 128BITS
    long id = null != meta ? meta.getClassId() : encoder.getClassId();
    
    int bidx = 0;
    
    bytes[bidx++] = (byte) ((id >> 56) & 0xff);
    bytes[bidx++] = (byte) ((id >> 48) & 0xff);
    bytes[bidx++] = (byte) ((id >> 40) & 0xff);
    bytes[bidx++] = (byte) ((id >> 32) & 0xff);
    bytes[bidx++] = (byte) ((id >> 24) & 0xff);
    bytes[bidx++] = (byte) ((id >> 16) & 0xff);
    bytes[bidx++] = (byte) ((id >> 8) & 0xff);
    bytes[bidx++] = (byte) (id & 0xff);
    
    id = null != meta ? meta.getLabelsId() : encoder.getLabelsId();

    bytes[bidx++] = (byte) ((id >> 56) & 0xff);
    bytes[bidx++] = (byte) ((id >> 48) & 0xff);
    bytes[bidx++] = (byte) ((id >> 40) & 0xff);
    bytes[bidx++] = (byte) ((id >> 32) & 0xff);
    bytes[bidx++] = (byte) ((id >> 24) & 0xff);
    bytes[bidx++] = (byte) ((id >> 16) & 0xff);
    bytes[bidx++] = (byte) ((id >> 8) & 0xff);
    bytes[bidx++] = (byte) (id & 0xff);

    BigInteger clslbls = new BigInteger(bytes);

    //
    // Retrieve the shard for the current GTS
    //
    
    InMemoryShardSet shardset = null;
    
    //
    // WARNING(hbs): the following 2 synchronized blocks MUST stay sequential (cf run())
    //
    
    synchronized (this.series) {
      shardset = this.series.get(clslbls);
      
      //
      // We need to allocate a new shard
      //
      
      if (null == shardset) {
        shardset = new InMemoryShardSet(this.shardcount, this.shardspan);
        this.series.put(clslbls,  shardset);
      }
    }

    //
    // Store data
    //
    
    shardset.store(encoder);

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
  public void archive(int chunk, GTSEncoder encoder) throws IOException {
    throw new IOException("in-memory platform does not support archiving.");
  }
  
  @Override
  public void run() {
    //
    // Loop endlessly over the series, cleaning them as they grow
    //
    
    long gcperiod = this.shardspan;
    
    if (null != properties.getProperty(STANDALONE_MEMORY_GC_PERIOD)) {
      gcperiod = Long.valueOf(properties.getProperty(STANDALONE_MEMORY_GC_PERIOD));
    }
    
    List<BigInteger> metadatas = new ArrayList<BigInteger>();
    
    while(true) {
      LockSupport.parkNanos(1000000L * (gcperiod / Constants.TIME_UNITS_PER_MS));

      metadatas.clear();
      metadatas.addAll(this.series.keySet());

      if (0 == metadatas.size()) {
        continue;
      }

      long now = TimeSource.getTime();
      
      long datapoints = 0L;
      long bytes = 0L;
      
      for (int idx = 0 ; idx < metadatas.size(); idx++) {
        InMemoryShardSet shardset = this.series.get(metadatas.get(idx));

        if (null == shardset) {
          continue;
        }
        
        shardset.clean(now);
        
        datapoints += shardset.getCount();
        bytes += shardset.getSize();        
      }
      
      //
      // Update the number of GC runs just before updating the number of bytes, so we reduce the probability that the two don't
      // change at the same time when polling the metrics (note that the probability still exists though)
      //
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_RUNS, Sensision.EMPTY_LABELS, 1);
      
      // We set the number of bytes but update the number of points (since we can't reliably determine the number of
      // datapoints in an encoder returned by decoder.getEncoder().
      
      Long oldbytes = (Long) Sensision.getValue(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_BYTES, Sensision.EMPTY_LABELS);     
      
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_BYTES, Sensision.EMPTY_LABELS, bytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_DATAPOINTS, Sensision.EMPTY_LABELS, datapoints);
      if (null != oldbytes && oldbytes > bytes) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_BYTES, Sensision.EMPTY_LABELS, oldbytes - bytes);
      }
    }
  }
  
  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    if (Long.MIN_VALUE != start || Long.MAX_VALUE != end) {
      throw new IOException("MemoryStore only supports deleting complete Geo Time Series.");
    }
    
    //
    // Regen classId/labelsId
    //
    
    // 128BITS
    metadata.setLabelsId(GTSHelper.labelsId(this.labelsKeyLongs, metadata.getLabels()));
    metadata.setClassId(GTSHelper.classId(this.classKeyLongs, metadata.getName()));

    byte[] bytes = new byte[16];
    
    long id = metadata.getClassId();
    
    int bidx = 0;
    
    bytes[bidx++] = (byte) ((id >> 56) & 0xff);
    bytes[bidx++] = (byte) ((id >> 48) & 0xff);
    bytes[bidx++] = (byte) ((id >> 40) & 0xff);
    bytes[bidx++] = (byte) ((id >> 32) & 0xff);
    bytes[bidx++] = (byte) ((id >> 24) & 0xff);
    bytes[bidx++] = (byte) ((id >> 16) & 0xff);
    bytes[bidx++] = (byte) ((id >> 8) & 0xff);
    bytes[bidx++] = (byte) (id & 0xff);
    
    id = metadata.getLabelsId();

    bytes[bidx++] = (byte) ((id >> 56) & 0xff);
    bytes[bidx++] = (byte) ((id >> 48) & 0xff);
    bytes[bidx++] = (byte) ((id >> 40) & 0xff);
    bytes[bidx++] = (byte) ((id >> 32) & 0xff);
    bytes[bidx++] = (byte) ((id >> 24) & 0xff);
    bytes[bidx++] = (byte) ((id >> 16) & 0xff);
    bytes[bidx++] = (byte) ((id >> 8) & 0xff);
    bytes[bidx++] = (byte) (id & 0xff);

    BigInteger clslbls = new BigInteger(bytes);

    synchronized(this.series) {
      this.series.remove(clslbls);
    }
    
    return 0L;
  }
  
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface plasmaHandler) {
    this.plasmaHandlers.add(plasmaHandler);
  } 
  
  public void dump(String path) throws IOException {
    
    long nano = System.nanoTime();
    int gts = 0;
    long bytes = 0L;
    
    Configuration conf = new Configuration();
        
    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    
    CompressionCodec Codec = new DefaultCodec();
    SequenceFile.Writer writer = null;
    SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(new Path(path));
    SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(key.getClass());
    SequenceFile.Writer.Option optVal = SequenceFile.Writer.valueClass(value.getClass());
    SequenceFile.Writer.Option optCom = SequenceFile.Writer.compression(CompressionType.RECORD,  Codec);
    
    writer = SequenceFile.createWriter(conf, optPath, optKey, optVal, optCom);

    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    long now = TimeSource.getTime();
    
    try {
      for (Entry<BigInteger,InMemoryShardSet> entry: this.series.entrySet()) {
        gts++;
        Metadata metadata = this.directoryClient.getMetadataById(entry.getKey());

        GTSWrapper wrapper = new GTSWrapper(metadata);        
        
        GTSEncoder encoder = entry.getValue().fetchEncoder(now, this.shardcount * this.shardspan);

        wrapper.setBase(encoder.getBaseTimestamp());
        wrapper.setCount(encoder.getCount());
        
        byte[] data = serializer.serialize(wrapper);
        key.set(data, 0, data.length);
        
        data = encoder.getBytes();
        value.set(data, 0, data.length);

        bytes += key.getLength() + value.getLength();
        
        writer.append(key, value);
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw ioe;
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    
    writer.close();

    nano = System.nanoTime() - nano;
    
    System.out.println("Dumped " + gts + " GTS (" + bytes + " bytes) in " + (nano / 1000000.0D) + " ms.");
  }
  
  public void load() {
    //
    // Load data from the specified file
    //
    
    if (null != properties.getProperty(STANDALONE_MEMORY_STORE_LOAD)) {
      try {
        load(properties.getProperty(STANDALONE_MEMORY_STORE_LOAD));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }    
  }
  
  private void load(String path) throws IOException {
    
    long nano = System.nanoTime();
    int gts = 0;
    long bytes = 0L;
    
    Configuration conf = new Configuration();
        
    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    
    SequenceFile.Reader.Option optPath = SequenceFile.Reader.file(new Path(path));    
    
    SequenceFile.Reader reader = null;
    
    try {
      reader = new SequenceFile.Reader(conf, optPath);

      System.out.println("Loading '" + path + "' back in memory.");

      while(reader.next(key, value)) {
        gts++;
        GTSWrapper wrapper = new GTSWrapper();
        deserializer.deserialize(wrapper, key.copyBytes());
        GTSEncoder encoder = new GTSEncoder(0L, null, value.copyBytes());
        encoder.setCount(wrapper.getCount());
        
        bytes += value.getLength() + key.getLength();
        encoder.safeSetMetadata(wrapper.getMetadata());
        store(encoder);
        if (null != this.directoryClient) {
          this.directoryClient.register(wrapper.getMetadata());
        }
      }
    } catch (FileNotFoundException fnfe) {
      System.err.println("File '" + path + "' was not found, skipping.");
      return;
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    reader.close();    
    
    nano = System.nanoTime() - nano;
    
    System.out.println("Loaded " + gts + " GTS (" + bytes + " bytes) in " + (nano / 1000000.0D) + " ms.");
  }
  
  public void setDirectoryClient(StandaloneDirectoryClient directoryClient) {
    this.directoryClient = directoryClient;
  }
}
