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

import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class MultiScanGTSDecoderIterator extends GTSDecoderIterator {

  int idx = 0;

  long nvalues = Long.MAX_VALUE;
  
  private final Table htable;
  
  private ResultScanner scanner = null;
  
  private Iterator<Result> scaniter = null;
  
  long resultCount = 0;
  
  long cellCount = 0;      

  private final List<Metadata> metadatas;
  
  private final long now;
  
  private final long timespan;
  
  private final boolean useBlockcache;
  
  private final ReadToken token;
  
  private final byte[] colfam;
  
  private final boolean writeTimestamp;
  
  private byte[] hbaseKey;
  
  private final boolean fromArchive;
  
  public MultiScanGTSDecoderIterator(boolean fromArchive, ReadToken token, long now, long timespan, List<Metadata> metadatas, Connection conn, TableName tableName, byte[] colfam, boolean writeTimestamp, KeyStore keystore, boolean useBlockcache) throws IOException {
    this.htable = conn.getTable(tableName);
    this.metadatas = metadatas;
    this.now = now;
    this.timespan = timespan;
    this.useBlockcache = useBlockcache;
    this.token = token;
    this.colfam = colfam;
    this.writeTimestamp = writeTimestamp;
    this.hbaseKey = keystore.getKey(KeyStore.AES_HBASE_DATA);
    this.fromArchive = fromArchive;
  }
      
  /**
   * Check whether or not there are more GeoTimeSerie instances.
   * 
   * The initial implementation is rather dumb, it loops over the metadatas and
   * spawns a new Scanner for each.
   * 
   * This works great for a single or a few time series but might prove underperforming for
   * a large number.
   * 
   * Other strategies could be attempted.
   * 
   * Fewer scans but with filters.
   * AsyncHBase
   * Managing a queue of metadatas handled by an executor service which would produce GTS in parallel
   * in the background and push them on a result queue. This strategy will need to throttle itself so
   * as to not overconsume memory.
   */
  @Override
  public boolean hasNext() {
    
    //
    // If scanner has not been nullified, it means there are more results, except if nvalues is 0
    // in which case it means we've read enough data
    //
    
    boolean scanHasMore = ((null == scaniter) || nvalues <= 0) ? false : scaniter.hasNext();
    
    if (scanHasMore) {
      return true;
    }
    
    //
    // If scanner is not null let's close it as it does not have any more results
    //
    
    if (null != scaniter) {
      this.scanner.close();
      this.scanner = null;
      this.scaniter = null;
    }
            
    //
    // If there are no more metadatas then there won't be any more data
    //
    
    if (idx >= metadatas.size()) {
      return false;
    }
    
    //
    // Scanner is either exhausted or had not yet been initialized, do so now
    //
    
    // TODO(hbs): determine best index and thus key structure to use
    // Determine if we should use filters on a fewer scanners instead of multiple scanners
    // which may prove inefficient.
    
    
    Metadata metadata = metadatas.get(idx++);
    
    //
    // Build start / end key
    //
    // CAUTION, the following code might seem wrong, but remember, timestamp
    // are reversed so the most recent (end) appears first (startkey)
    //
    
    byte[] startkey = new byte[Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];
    byte[] endkey = new byte[startkey.length];
    
    ByteBuffer bb = ByteBuffer.wrap(startkey).order(ByteOrder.BIG_ENDIAN);
    bb.put(Constants.HBASE_RAW_DATA_KEY_PREFIX);
    bb.putLong(metadata.getClassId());
    bb.putLong(metadata.getLabelsId());
    // FIXME(hbs): modulus should be extracted from metadata as it depends on GTS and auth
    long modulus = now - (now % Constants.DEFAULT_MODULUS);
    
    bb.putLong(Long.MAX_VALUE - modulus);
    
    bb = ByteBuffer.wrap(endkey).order(ByteOrder.BIG_ENDIAN);
    bb.put(Constants.HBASE_RAW_DATA_KEY_PREFIX);
    bb.putLong(metadata.getClassId());
    bb.putLong(metadata.getLabelsId());
    //
    // We need to stop on the modulus boundary that precedes the last valid boundary
    //
    
    if (timespan >= 0) {
      modulus = (now - timespan);
      modulus = (modulus - (modulus % Constants.DEFAULT_MODULUS)) - Constants.DEFAULT_MODULUS;
    
      bb.putLong(Long.MAX_VALUE - modulus);
    } else {
      bb.putLong(0xffffffffffffffffL);
    }
    
    //
    // Reset number of values retrieved since we just skipped to a new GTS.
    // If 'timespan' is negative this is the opposite of the number of values to retrieve
    // otherwise use Long.MAX_VALUE
    //
    
    nvalues = timespan < 0 ? -timespan : Long.MAX_VALUE;

    Scan scan = new Scan();
    // Retrieve the whole column family
    scan.addFamily(colfam);
    scan.setStartRow(startkey);
    scan.setStopRow(endkey);

    //
    // Set batch/cache parameters
    //
    // FIXME(hbs): when using the HBase >= 0.96, use setMaxResultSize instead, and use setPrefetching
    
    if (timespan > 0) {
      scan.setMaxResultSize(1000000L);
    }
    // Setting 'batch' too high when DEFAULT_MODULUS is != 1 will decrease performance when no filter is in use as extraneous cells may be fetched per row
    // Setting it too low will increase the number of roundtrips. A good heuristic is to set it to -timespan if timespan is < 0
    scan.setBatch((int) (timespan < 0 ? Math.min(-timespan, 100000) : 100000));
    
    // Number of rows to cache can be set arbitrarly high as the end row will stop the scanner caching anyway
    scan.setCaching((int) (timespan < 0 ? Math.min(-timespan, 100000) : 100000));
    
    if (this.useBlockcache) {
      scan.setCacheBlocks(true);
    } else {
      scan.setCacheBlocks(false);
    }
    
    try {
      this.scanner = htable.getScanner(scan);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_SCANNERS, Sensision.EMPTY_LABELS, 1);
      this.scaniter = this.scanner.iterator();          
    } catch (IOException ioe) {
      //
      // If we caught an exception, we skip to the next metadata
      // FIXME(hbs): log exception somehow
      //
      this.scanner = null;
      this.scaniter = null;
      return hasNext();
    }

    //
    // If the current scanner has no more entries, call hasNext recursively so it takes
    // care of all the dirty details.
    //

    if (this.scaniter.hasNext()) {
      return true;
    } else {
      return hasNext();
    }
  }
  
  @Override
  public GTSDecoder next() {
    
    if (null == scaniter) {
      return null;
    }

    long datapoints = 0L;
    long keyBytes = 0L;
    long valueBytes = 0L;
    
    //
    // Create a new GTSEncoder for the results
    //
    
    GTSEncoder encoder = new GTSEncoder(0L);

    while(encoder.size() < Constants.MAX_ENCODER_SIZE && nvalues > 0 && scaniter.hasNext()) {
      
      //
      // Extract next result from scaniter
      //
      
      Result result = scaniter.next();
                
      resultCount++;
      
      CellScanner cscanner = result.cellScanner();
      
      try {
        while(nvalues > 0 && cscanner.advance()) {
          Cell cell = cscanner.current();
      
          cellCount++;
          
          //
          // Extract timestamp base from column qualifier
          // This is true even for packed readings, those have a base timestamp of 0L
          //

          long basets = Long.MAX_VALUE;
          
          if (1 == Constants.DEFAULT_MODULUS) {
            byte[] data = cell.getRowArray();
            int offset = cell.getRowOffset();
            offset += Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8; // Add 'prefix' + 'classId' + 'labelsId' to row key offset
            long delta = data[offset] & 0xFF;
            delta <<= 8; delta |= (data[offset + 1] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 2] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 3] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 4] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 5] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 6] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 7] & 0xFFL);
            basets -= delta;              
          } else {
            byte[] data = cell.getQualifierArray();
            int offset = cell.getQualifierOffset();
            long delta = data[offset] & 0xFFL;
            delta <<= 8; delta |= (data[offset + 1] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 2] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 3] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 4] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 5] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 6] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 7] & 0xFFL);
            basets -= delta;                            
          }
          
          byte[] value = cell.getValueArray();
          int valueOffset = cell.getValueOffset();
          int valueLength = cell.getValueLength();
          
          ByteBuffer bb = ByteBuffer.wrap(value,valueOffset,valueLength).order(ByteOrder.BIG_ENDIAN);

          GTSDecoder decoder = new GTSDecoder(basets, hbaseKey, bb);
                    
          while(nvalues > 0 && decoder.next()) {
            long timestamp = decoder.getTimestamp();
            if (timestamp <= now && (timespan < 0 || (timestamp > (now - timespan)))) {
              try {
                if (writeTimestamp) {
                  encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), cell.getTimestamp() * Constants.TIME_UNITS_PER_MS);
                } else {
                  encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
                }
                
                //
                // Update statistics
                //
                
                valueBytes += valueLength;
                keyBytes += cell.getRowLength() + cell.getFamilyLength() + cell.getQualifierLength();
                datapoints++;

                nvalues--;
              } catch (IOException ioe) {
                // FIXME(hbs): LOG?
              }
            }
          }
        }          
      } catch(IOException ioe) {
        // FIXME(hbs): LOG?
      }

      /*
      NavigableMap<byte[], byte[]> colfams = result.getFamilyMap(colfam);
              
      for (byte[] qualifier: colfams.keySet()) {
        //
        // Extract timestamp base from column qualifier
        // This is true even for packed readings, those have a base timestamp of 0L
        //
     
        long basets = Long.MAX_VALUE - Longs.fromByteArray(qualifier);
        byte[] value = colfams.get(qualifier);
        
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);

        GTSDecoder decoder = new GTSDecoder(basets, keystore.getKey(KeyStore.AES_HBASE_DATA), bb);
                  
        while(decoder.next() && nvalues > 0) {
          long timestamp = decoder.getTimestamp();
          if (timestamp <= now && (timespan < 0 || (timestamp > (now - timespan)))) {
            try {
              encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());
              nvalues--;
            } catch (IOException ioe) {
              // FIXME(hbs): LOG?
            }
          }
        }
      } 
      */
      
    }
    
    encoder.setMetadata(metadatas.get(idx-1));

    //
    // Update Sensision
    //

    //
    // Null token can happen when retrieving data from GTSSplit instances
    //
    
    if (null != token) {
      Map<String,String> labels = new HashMap<String,String>();
      
      Map<String,String> metadataLabels = metadatas.get(idx-1).getLabels();
      
      String billedCustomerId = Tokens.getUUID(token.getBilledId());

      if (null != billedCustomerId) {
        labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, billedCustomerId);
      }
      
      if (metadataLabels.containsKey(Constants.APPLICATION_LABEL)) {
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, metadataLabels.get(Constants.APPLICATION_LABEL));
      }
      
      if (metadataLabels.containsKey(Constants.OWNER_LABEL)) {
        labels.put(SensisionConstants.SENSISION_LABEL_OWNER, metadataLabels.get(Constants.OWNER_LABEL));
      }
      
      if (null != token.getAppName()) {
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
    }
    
    return encoder.getDecoder();
  }
  
  @Override
  public void remove() {
  }
  
  @Override
  public void close() throws Exception {
    //
    // Update Sensision metrics
    //
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_RESULTS, Sensision.EMPTY_LABELS, resultCount);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_CELLS, Sensision.EMPTY_LABELS, cellCount);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_ITERATORS, Sensision.EMPTY_LABELS, 1);
    if (null != this.scanner) {
      this.scanner.close();
    }
    if (null != this.htable) {
      this.htable.close();
    }
  }

}
