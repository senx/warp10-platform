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

package io.warp10.standalone;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.MetadataIdComparator;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.Store;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.Bytes;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

public class StandaloneStoreClient implements StoreClient {
  
  /**
   * This determines how often we will perform merges when retrieving
   */
  private final long MAX_ENCODER_SIZE;
  
  private static final String DEFAULT_MAX_ENCODER_SIZE = "1000000";
  
  private final int MAX_DELETE_BATCHSIZE;
  private static final int DEFAULT_MAX_DELETE_BATCHSIZE = 10000;
  
  private final DB db;
  private final KeyStore keystore;
  private final Properties properties;
  
  private final List<StandalonePlasmaHandlerInterface> plasmaHandlers;

  private final boolean syncwrites;
  private final double syncrate;
  
  public StandaloneStoreClient(DB db, KeyStore keystore, Properties properties) {
    this.db = db;
    this.keystore = keystore;
    this.properties = properties;
    this.plasmaHandlers = new ArrayList<StandalonePlasmaHandlerInterface>();
    
    MAX_ENCODER_SIZE = Long.valueOf(properties.getProperty(Configuration.STANDALONE_MAX_ENCODER_SIZE, DEFAULT_MAX_ENCODER_SIZE));
    MAX_DELETE_BATCHSIZE = Integer.parseInt(properties.getProperty(Configuration.STANDALONE_MAX_DELETE_BATCHSIZE, Integer.toString(DEFAULT_MAX_DELETE_BATCHSIZE)));
    
    syncrate = Math.min(1.0D, Math.max(0.0D, Double.parseDouble(properties.getProperty(Configuration.LEVELDB_DATA_SYNCRATE, "1.0"))));
    syncwrites = 0.0 < syncrate && syncrate < 1.0 ;
  }
  
  @Override
  public GTSDecoderIterator fetch(final ReadToken token, final List<Metadata> metadatas, final long now, final long timespan, final boolean fromArchive, boolean writeTimestamp) {
    
    if (timespan < 0 && fromArchive) {
      throw new RuntimeException("No support for negative timespans when fetching from archive.");      
    }

    if (writeTimestamp) {
      throw new RuntimeException("No support for write timestamp retrieval.");
    }
    
    final DBIterator iterator = db.iterator();

    Map<String,String> labels = new HashMap<String,String>();
    
    if (null != token && null != token.getAppName()) {
      labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, token.getAppName());
    }
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_COUNT, labels, 1);

    //
    // Sort metadatas by increasing classId,labelsId so as to optimize the range scans
    //
    
    Collections.sort(metadatas, MetadataIdComparator.COMPARATOR);
        
    return new GTSDecoderIterator() {
    
      int idx = -1;
       
      // First row of current scan      
      byte[] startrow = null;
      // Last raw (included) of current scan
      byte[] stoprow = null;
      
      /**
       * Number of values yet to retrieve for the current GTS
       */ 
      long nvalues = Long.MAX_VALUE;
      
      @Override
      public void close() throws Exception {
        iterator.close();
      }
      
      @Override
      public void remove() {        
      }
      
      @Override
      public GTSDecoder next() {
                
        GTSEncoder encoder = new GTSEncoder(0L);

        long keyBytes = 0L;
        long valueBytes = 0L;
        long datapoints = 0L;
        
        do {
          Entry<byte[], byte[]> kv = iterator.next();
          
          if (Bytes.compareTo(kv.getKey(), stoprow) > 0) {
            startrow = null;
            break;
          }
          
          ByteBuffer bb = ByteBuffer.wrap(kv.getKey()).order(ByteOrder.BIG_ENDIAN);
          
          if (fromArchive) {
            bb.position(Store.HBASE_ARCHIVE_DATA_KEY_PREFIX.length + 8 + 8);                        
          } else {
            bb.position(Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8);            
          }
          
          long basets = fromArchive ? 0L : (Long.MAX_VALUE - bb.getLong());
          
          byte[] v = kv.getValue();
          
          //
          // Update statistics
          //
          
          valueBytes += v.length;
          keyBytes += kv.getKey().length;          
          datapoints++;
          
          nvalues--;
                    
          if (fromArchive) {
            // When reading from the archive, create an encoder with the chunk data
            encoder = new GTSEncoder(0L, keystore.getKey(KeyStore.AES_LEVELDB_DATA), kv.getValue());
            // Exit after the chunk
            break;
          } else {
            GTSDecoder decoder = new GTSDecoder(basets, keystore.getKey(KeyStore.AES_LEVELDB_DATA), ByteBuffer.wrap(kv.getValue()));
            decoder.next();
            try {
              encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }            
          }
        } while(iterator.hasNext() && encoder.size() < MAX_ENCODER_SIZE && nvalues > 0);

        encoder.setMetadata(metadatas.get(idx));

        //
        // Update Sensision
        //

        Map<String,String> labels = new HashMap<String,String>();
        
        Map<String,String> metadataLabels = metadatas.get(idx).getLabels();
        
        String billedCustomerId = null != token ? Tokens.getUUID(token.getBilledId()) : null;

        if (null != billedCustomerId) {
          labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, billedCustomerId);
        }
        
        if (metadataLabels.containsKey(Constants.APPLICATION_LABEL)) {
          labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, metadataLabels.get(Constants.APPLICATION_LABEL));
        }
        
        if (metadataLabels.containsKey(Constants.OWNER_LABEL)) {
          labels.put(SensisionConstants.SENSISION_LABEL_OWNER, metadataLabels.get(Constants.OWNER_LABEL));
        }
        
        if (null != token && null != token.getAppName()) {
          labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERAPP, token.getAppName());
        }
        
        //
        // Update per owner statistics, use a TTL for those
        //
        
        if (fromArchive) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_AFETCH_BYTES_VALUES_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, valueBytes);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_AFETCH_BYTES_KEYS_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, keyBytes);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_AFETCH_DATAPOINTS_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, datapoints);                    
        } else {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_VALUES_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, valueBytes);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_KEYS_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, keyBytes);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_DATAPOINTS_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, datapoints);          
        }
               
        //
        // Update summary statistics
        //

        // Remove 'owner' label
        labels.remove(SensisionConstants.SENSISION_LABEL_OWNER);

        if (fromArchive) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_AFETCH_BYTES_VALUES, labels, valueBytes);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_AFETCH_BYTES_KEYS, labels, keyBytes);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_AFETCH_DATAPOINTS, labels, datapoints);          
        } else {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_VALUES, labels, valueBytes);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_KEYS, labels, keyBytes);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_DATAPOINTS, labels, datapoints);          
        }

        return encoder.getDecoder();
      }
      
      @Override
      public boolean hasNext() {
        
        if (idx >= metadatas.size()) {
          return false;
        }

        //
        // If idx is non null, peek the next key and determine if it is in the current range or not
        //
        
        while(true) {
          if (idx >= 0 && iterator.hasNext()) {
            Entry<byte[], byte[]> kv = iterator.peekNext();

            // If the next key is over the range, nullify startrow
            if (Bytes.compareTo(kv.getKey(), stoprow) > 0) {
              startrow = null;
            } else {
              //
              // If we are time based or value count based with values left to read, return true
              if (timespan >= 0 || (timespan < 0 && nvalues > 0)) {
                return true;
              } else {
                startrow = null;
              }
            }
          } else {
            startrow = null;
          }
          
          // We need to reseek if startrow is null
          if (null == startrow) {
            idx++;

            if (idx >= metadatas.size()) {
              return false;
            }
            
            if (fromArchive) {
              startrow = new byte[Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8];
              ByteBuffer bb = ByteBuffer.wrap(startrow).order(ByteOrder.BIG_ENDIAN);
              bb.put(Store.HBASE_ARCHIVE_DATA_KEY_PREFIX);
              bb.putLong(metadatas.get(idx).getClassId());
              bb.putLong(metadatas.get(idx).getLabelsId());
              
              stoprow = new byte[startrow.length + 8]; // Provision for 2**64 chunks (!)
              bb = ByteBuffer.wrap(stoprow).order(ByteOrder.BIG_ENDIAN);
              bb.put(Store.HBASE_ARCHIVE_DATA_KEY_PREFIX);
              bb.putLong(metadatas.get(idx).getClassId());
              bb.putLong(metadatas.get(idx).getLabelsId());              
              bb.putLong(0xffffffffffffffffL);
            } else {
              startrow = new byte[Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];
              ByteBuffer bb = ByteBuffer.wrap(startrow).order(ByteOrder.BIG_ENDIAN);
              bb.put(Constants.HBASE_RAW_DATA_KEY_PREFIX);
              bb.putLong(metadatas.get(idx).getClassId());
              bb.putLong(metadatas.get(idx).getLabelsId());
              bb.putLong(Long.MAX_VALUE - now);
              
              stoprow = new byte[startrow.length];
              bb = ByteBuffer.wrap(stoprow).order(ByteOrder.BIG_ENDIAN);
              bb.put(Constants.HBASE_RAW_DATA_KEY_PREFIX);
              bb.putLong(metadatas.get(idx).getClassId());
              bb.putLong(metadatas.get(idx).getLabelsId());              
              
              //
              // If 'timespan' is negative, we are interested in retrieving -'timespan' values,
              // regardless of how long ago they were measured
              //
              
              if (timespan >= 0) {
                bb.putLong(Long.MAX_VALUE - (now - timespan + 1));
              } else {
                bb.putLong(0xffffffffffffffffL);
              }
            }            
          }

          //
          // Reset number of values retrieved since we just skipped to a new GTS.
          // If 'timespan' is negative this is the opposite of the number of values to retrieve
          // otherwise use Long.MAX_VALUE
          //
          
          nvalues = (!fromArchive && (timespan < 0)) ? -timespan : Long.MAX_VALUE;
          
          iterator.seek(startrow);          
        }
      }
    };
  }
  
  private ThreadLocal<WriteBatch> perThreadWriteBatch = new ThreadLocal<WriteBatch>() {
    protected WriteBatch initialValue() {      
      return db.createWriteBatch();
    };    
  };
  
  private ThreadLocal<AtomicLong> perThreadWriteBatchSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong(0L);
    };
  };
  
  private void store(List<byte[][]> kvs) throws IOException {
  
    //WriteBatch batch = this.db.createWriteBatch();
    
    WriteBatch batch = perThreadWriteBatch.get();

    AtomicLong size = perThreadWriteBatchSize.get();
    
    boolean written = false;
    
    try {
      if (null != kvs) {
        for (byte[][] kv: kvs) {
          batch.put(kv[0], kv[1]);
          size.addAndGet(kv[0].length + kv[1].length);
        }        
      }
      
      if (null == kvs || size.get() > MAX_ENCODER_SIZE) {
        
        WriteOptions options = new WriteOptions().sync(null == kvs || 1.0 == syncrate);
        
        if (syncwrites && !options.sync()) {
          options = new WriteOptions().sync(Math.random() < syncrate);
        }

        this.db.write(batch, options);
        size.set(0L);
        perThreadWriteBatch.remove();
        written = true;
      }
      //this.db.write(batch);
    } finally {
      if (written) {
        batch.close();
      }
    }
  }
  
  public void store(GTSEncoder encoder) throws IOException {
    
    if (null == encoder) {
      store((List<byte[][]>) null);
      return;
    }
    
    GTSDecoder decoder = encoder.getDecoder();
    
    List<byte[][]> kvs = new ArrayList<byte[][]>();
    
    while(decoder.next()) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8]).order(ByteOrder.BIG_ENDIAN);
      bb.put(Constants.HBASE_RAW_DATA_KEY_PREFIX);
      bb.putLong(encoder.getClassId());
      bb.putLong(encoder.getLabelsId());
      bb.putLong(Long.MAX_VALUE - decoder.getTimestamp());
      
      GTSEncoder enc = new GTSEncoder(decoder.getTimestamp(), this.keystore.getKey(KeyStore.AES_LEVELDB_DATA));
      
      enc.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
      
      byte[] value = enc.getBytes();
    
      kvs.add(new byte[][] { bb.array(), value });
    }
    
    store(kvs);
    
    for (StandalonePlasmaHandlerInterface plasmaHandler: this.plasmaHandlers) {
      if (plasmaHandler.hasSubscriptions()) {
        plasmaHandler.publish(encoder);
      }
    }
  }
  
  @Override
  public void archive(int chunk, GTSEncoder encoder) throws IOException {
    
    if (null == encoder || chunk < 0) {
      store((List<byte[][]>) null);
      return;
    }
    
    //
    // If the basetimestamp is not 0, throw an error
    //
    
    if (0 != encoder.getBaseTimestamp()) {
      throw new IOException("Invalid base timestamp.");
    }
    
    //
    // Add the wrapping key
    //
    
    encoder.setWrappingKey(this.keystore.getKey(KeyStore.AES_LEVELDB_DATA));
    
    //
    // If chunk is 0, remove the archived data first
    //
    
    long count = 0;
    
    if (0 == chunk) {
      DBIterator iterator = this.db.iterator();
      
      byte[] seekto = new byte[Store.HBASE_ARCHIVE_DATA_KEY_PREFIX.length + 8 + 8];
      ByteBuffer bb = ByteBuffer.wrap(seekto).order(ByteOrder.BIG_ENDIAN);
      bb.put(Store.HBASE_ARCHIVE_DATA_KEY_PREFIX);
      bb.putLong(encoder.getClassId());
      bb.putLong(encoder.getLabelsId());
      
      iterator.seek(seekto);

      WriteBatch batch = this.db.createWriteBatch();
      int batchsize = 0;
      
      WriteOptions options = new WriteOptions().sync(1.0 == syncrate);
      
      while (iterator.hasNext()) {
        Entry<byte[],byte[]> entry = iterator.next();
        
        if (0 == Bytes.compareTo(entry.getKey(), 0, seekto.length, seekto, 0, seekto.length)) {
          batch.delete(entry.getKey());
          batchsize++;
          
          if (MAX_DELETE_BATCHSIZE <= batchsize) {
            if (syncwrites) {
              options = new WriteOptions().sync(Math.random() < syncrate);
            }
            this.db.write(batch, options);
            batch.close();
            batch = this.db.createWriteBatch();
            batchsize = 0;
          }
          //this.db.delete(entry.getKey());
          count++;
        } else {
          break;
        }
      }

      if (batchsize > 0) {
        if (syncwrites) {
          options = new WriteOptions().sync(Math.random() < syncrate);
        }
        this.db.write(batch, options);
      }
      iterator.close();
      batch.close();
    }
    
    int v = chunk;
    int vbytes = 1;
    
    //
    // Compute the number of bytes needed to represent the chunk
    //
    
    while(0 != v) {
      vbytes++;
      v = v >>> 8;
    }
    
    byte[] key = new byte[Store.HBASE_ARCHIVE_DATA_KEY_PREFIX.length + 8 + 8 + vbytes + vbytes - 1];
    ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
    bb.put(Store.HBASE_ARCHIVE_DATA_KEY_PREFIX);
    bb.putLong(encoder.getClassId());
    bb.putLong(encoder.getLabelsId());
    
    //
    // Fill the key with vbytes - 1 0xff
    //
    
    for (int i = 0; i < vbytes - 1; i++) {
      bb.put((byte) 0xff);
    }
    
    //
    // Output chunk id
    //
    
    for (int i = vbytes - 1; i >= 0; i--) {
      bb.put((byte) ((chunk >>> i) & 0xff));
    }

    List<byte[][]> kvs = new ArrayList<byte[][]>();
    
    kvs.add(new byte[][] { key, encoder.getBytes() });
    
    store(kvs);
    
    //
    // We don't propagate data to the plasma handler when archiving
    //    
  }
  
  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    
    //
    // Regen classId/labelsId
    //
    
    // 128BITS
    metadata.setLabelsId(GTSHelper.labelsId(this.keystore.getKey(KeyStore.SIPHASH_LABELS), metadata.getLabels()));
    metadata.setClassId(GTSHelper.classId(this.keystore.getKey(KeyStore.SIPHASH_CLASS), metadata.getName()));

    //
    // Retrieve an iterator
    //
    
    DBIterator iterator = this.db.iterator();
    //
    // Seek the most recent key
    //
    
    // 128BITS
    byte[] bend = new byte[Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];
    ByteBuffer bb = ByteBuffer.wrap(bend).order(ByteOrder.BIG_ENDIAN);
    bb.put(Constants.HBASE_RAW_DATA_KEY_PREFIX);
    bb.putLong(metadata.getClassId());
    bb.putLong(metadata.getLabelsId());
    bb.putLong(Long.MAX_VALUE - end);

    iterator.seek(bend);
    
    byte[] bstart = new byte[bend.length];
    bb = ByteBuffer.wrap(bstart).order(ByteOrder.BIG_ENDIAN);
    bb.put(Constants.HBASE_RAW_DATA_KEY_PREFIX);
    bb.putLong(metadata.getClassId());
    bb.putLong(metadata.getLabelsId());
    bb.putLong(Long.MAX_VALUE - start);
    
    //
    // Scan the iterator, deleting keys if they are between start and end
    //
    
    long count = 0L;
    
    WriteBatch batch = this.db.createWriteBatch();
    int batchsize = 0;
    
    WriteOptions options = new WriteOptions().sync(1.0 == syncrate);
                
    while (iterator.hasNext()) {
      Entry<byte[],byte[]> entry = iterator.next();

      if (Bytes.compareTo(entry.getKey(), bend) >= 0 && Bytes.compareTo(entry.getKey(), bstart) <= 0) {
        batch.delete(entry.getKey());
        batchsize++;
        
        if (MAX_DELETE_BATCHSIZE <= batchsize) {
          if (syncwrites) {
            options = new WriteOptions().sync(Math.random() < syncrate);
          }
          this.db.write(batch, options);
          batch.close();
          batch = this.db.createWriteBatch();
          batchsize = 0;
        }
        //this.db.delete(entry.getKey());
        count++;
      } else {
        break;
      }
    }
    
    if (batchsize > 0) {
      if (syncwrites) {
        options = new WriteOptions().sync(Math.random() < syncrate);
      }
      this.db.write(batch, options);
    }

    iterator.close();
    batch.close();
    
    return count;
  }
  
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface plasmaHandler) {
    this.plasmaHandlers.add(plasmaHandler);
  }
}
