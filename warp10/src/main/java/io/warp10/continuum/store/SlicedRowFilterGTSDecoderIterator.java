//
//   Copyright 2018-2021  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SlicedRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

/**
 * GTSIterator using RowSliceFilters to select rows
 * 
 */
public class SlicedRowFilterGTSDecoderIterator extends GTSDecoderIterator implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SlicedRowFilterGTSDecoderIterator.class);
  
  private Table htable;
  private ResultScanner scanner;
  private Iterator<Result> iter;
  
  private final long now;
  //private final long timespan;
  private final long then;
  private final long count;
  
  private final FetchRequest request;
  
  private long cellCount = 0;
  private long resultCount = 0;
  
  private final byte[] hbaseAESKey;

  /**
   * Array to hold the class & labels ids of the current GTS
   */
  // 128bits
  private final byte[] currentGTS = new byte[8 + 8];
  private String currentGTSString = new String(currentGTS, StandardCharsets.ISO_8859_1);
  
  /**
   * Number of values left to read
   */
  private long nvalues = Long.MAX_VALUE;
  
  /**
   * Map of classId/labelsId to Metadata, used to retrieve name/labels.
   * We can't use byte[] as keys as they use object identity for equals and hashcode 
   */
  private Map<String, Metadata> metadatas = new HashMap<String, Metadata>();
  
  /**
   * Pending Result
   */
  private Result pendingresult = null;
  
  private static byte[] prefix = Constants.HBASE_RAW_DATA_KEY_PREFIX;

  private final boolean writeTimestamp;
  private final boolean fetchTTL;
  private final boolean hasStep;
  private final boolean hasTimestep;
  private final boolean hasSample;
  private long skip = 0L;
  private long steps = 0L;
  private final long step;
  private final long timestep;
  private long nextTimestamp = Long.MAX_VALUE;
  private final double sample;
  
  private Random prng = null;

  public SlicedRowFilterGTSDecoderIterator(FetchRequest req, Connection conn, TableName tableName, byte[] colfam, KeyStore keystore, boolean useBlockCache) {
      
    this.request = req;
    this.now = req.getNow();
    this.then = req.getThents();
    this.count = req.getCount();
    this.sample = req.getSample();
    this.hasSample = this.sample < 1.0D;
    this.prng = hasSample ? new Random() : null;
    this.hasStep = req.getStep() > 1L;
    this.steps = 0L;
    this.step = hasStep ? req.getStep() : 1L;
    this.hasTimestep = req.getTimestep() > 1L;
    this.timestep = this.hasTimestep ? req.getTimestep() : 1L;
    this.nextTimestamp = Long.MAX_VALUE;
    this.skip = req.getSkip();
    this.nvalues = this.count >= 0 ? this.count : Long.MAX_VALUE;
    this.hbaseAESKey = keystore.getKey(KeyStore.AES_HBASE_DATA);
    this.writeTimestamp = req.isWriteTimestamp();
    this.fetchTTL = req.isTTL();
    List<Metadata> metadatas = req.getMetadatas();
    
    //
    // Create a SlicedRowFilter for the prefix, class id, labels id and ts
    // We include the prefix so we exit the filter early when the last
    // matching row has been reached
    //
    
    // 128BITS
    
    int[] bounds = { 0, prefix.length + 23 };
    
    //
    // Create singleton for each classId/labelsId combo
    //
    // TODO(hbs): we should really create multiple scanner, one per class Id for example,
    // 
    
    List<Pair<byte[], byte[]>> ranges = new ArrayList<Pair<byte[], byte[]>>();
    
    for (Metadata metadata: metadatas) {
      byte[][] keys = getKeys(metadata, now, then);
      byte[] lower = keys[0];
      byte[] upper = keys[1];
      
      // Store the Metadata under a key containing both class and labels id in a 16 character string
      this.metadatas.put(new String(lower, prefix.length, 16, StandardCharsets.ISO_8859_1), metadata);
      
      Pair<byte[],byte[]> range = new Pair<byte[],byte[]>(lower, upper);
      
      ranges.add(range);
    }
                
    SlicedRowFilter filter = new SlicedRowFilter(bounds, ranges, count >= 0 ? count : Long.MAX_VALUE);

    //
    // Create scanner. The start key is the lower bound of the first range
    //
    
    Scan scan = new Scan();
    scan.addFamily(colfam); // (HBaseStore.GTS_COLFAM, Longs.toByteArray(Long.MAX_VALUE - modulus));
    scan.setStartRow(filter.getStartKey());
    byte[] filterStopKey = filter.getStopKey();
    // Add one byte at the end (we can do that because we know the slice is the whole key)
    byte[] stopRow = Arrays.copyOf(filterStopKey, filterStopKey.length + 1);
    scan.setStopRow(stopRow);
    scan.setFilter(filter);
    
    scan.setMaxResultSize(1000000L);
    scan.setBatch(50000);
    scan.setCaching(50000);
    
    scan.setCacheBlocks(useBlockCache);

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_FILTERED_SCANNERS, Sensision.EMPTY_LABELS, 1);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_FILTERED_SCANNERS_RANGES, Sensision.EMPTY_LABELS, ranges.size());

    try {
      this.htable = conn.getTable(tableName);
      this.scanner = this.htable.getScanner(scan);
      iter = scanner.iterator();          
    } catch (IOException ioe) {
      LOG.error("",ioe);
      this.iter = null;
    }
  }
  
  public static byte[][] getKeys(Metadata metadata, long now, long then) {
    // 128BITS
    byte[] lower = new byte[24 + prefix.length];
    byte[] upper = new byte[lower.length];
    
    System.arraycopy(prefix, 0, lower, 0, prefix.length);
    
    System.arraycopy(Longs.toByteArray(metadata.getClassId()), 0, lower, prefix.length, 8);
    System.arraycopy(Longs.toByteArray(metadata.getLabelsId()), 0, lower, prefix.length + 8, 8);

    System.arraycopy(lower, 0, upper, 0, prefix.length + 16);

    //
    // Set lower/upper timestamps
    //
    
    System.arraycopy(Longs.toByteArray(Long.MAX_VALUE - now), 0, lower, prefix.length + 8 + 8, 8);        
    // SlicedRowFilter upper bound is included
    System.arraycopy(Longs.toByteArray(Long.MAX_VALUE - then), 0, upper, prefix.length + 8 + 8, 8);        

    byte[][] keys = new byte[2][];
    keys[0] = lower;
    keys[1] = upper;
    return keys;
  }
  
  @Override
  public boolean hasNext() {
    // There is a Result instance we have not yet looked at so return true
    // now
    if (null != this.pendingresult) {
      return true;
    }
    //
    // There is no iterator to use, so return false
    //
    if (null == this.iter) {
      return false;
    }
    return this.iter.hasNext();
  }
  
  @Override
  public GTSDecoder next() {
    
    GTSEncoder encoder = new GTSEncoder(0L);

    while(encoder.size() < Constants.MAX_ENCODER_SIZE && (null != this.pendingresult || this.iter.hasNext())) {
      
      //
      // Extract next result from scan iterator, unless there is a current pending Result
      //
    
      Result result = null;
      
      if (null != this.pendingresult) {
        result = this.pendingresult;
        this.pendingresult = null;
      } else {
        result = this.iter.next();
        resultCount++;
      }

      //
      // Compare the class and labels id with those in currentGTS
      // If they differ, recompute currentGTSString
      //
      
      if (0 != Bytes.compareTo(currentGTS, 0, 16, result.getRow(), Constants.HBASE_RAW_DATA_KEY_PREFIX.length, 16)) {
        System.arraycopy(result.getRow(), Constants.HBASE_RAW_DATA_KEY_PREFIX.length, currentGTS, 0, currentGTS.length);
        currentGTSString = new String(currentGTS, StandardCharsets.ISO_8859_1);
      }

      String classlabelsid = currentGTSString;
    
      //
      // Extract the Metadata associated with the current row
      //
      Metadata metadata = this.metadatas.get(classlabelsid);

      //
      // The current row is for a different GTS.
      // If the current encoder has data, return it and record the current result for the next call to 'next'
      // If the current encoder has no data (which could happen if we have reached the requested number of results),
      // update the metadata
      //      
      if ((encoder.getClassId() != metadata.getClassId() || encoder.getLabelsId() != metadata.getLabelsId())) {
        //
        // Reset fetch parameters
        //
        this.nvalues = this.count >= 0 ? this.count : Long.MAX_VALUE;
        this.skip = this.request.getSkip();
        this.steps = 0L;
        this.nextTimestamp = Long.MAX_VALUE;
        if (encoder.size() > 0) {
          // Save result in pendingresult as we have not yet read the associated cells
          this.pendingresult = result;
          return encoder.getDecoder();
        }
      }
      
      //
      // Set metadata
      //
      
      if (0 == encoder.size()) {
        encoder.setMetadata(metadata);
      }
      
      CellScanner cscanner = result.cellScanner();

      try {
        while(cscanner.advance()) {
          Cell cell = cscanner.current();
          cellCount++;
          
          // Simply ignore the values if we already collected enough
          if (nvalues <= 0) {
            continue;
          }
          
          //
          // Extract timestamp from row key
          //

          // 128BITS
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
          long basets = Long.MAX_VALUE - delta;              
          
          byte[] value = cell.getValueArray();
          int valueOffset = cell.getValueOffset();
          int valueLength = cell.getValueLength();
          
          ByteBuffer bb = ByteBuffer.wrap(value,valueOffset,valueLength).order(ByteOrder.BIG_ENDIAN);

          GTSDecoder decoder = new GTSDecoder(basets, hbaseAESKey, bb);
                    
          while(decoder.next() && nvalues > 0) {
            long timestamp = decoder.getTimestamp();
            
            //
            // Only consider values if they are within the requested range
            // This should always be the case but better err on the side of caution sometimes
            //
            if (timestamp <= now && timestamp >= then) {

              //
              // Skip datapoints
              //
              
              if (this.skip > 0) {
                this.skip--;
                continue;
              }
              
              //
              // Check that the datapoint timestamp is compatible with the timestep parameter, i.e. it is at least
              // 'timestep' time units before the previous one we selected
              //
              
              if (timestamp > nextTimestamp) {
                continue;
              }

              //
              // Compute the new value of nextTimestamp if timestep is set
              //
              if (hasTimestep) {
                try {
                  nextTimestamp = Math.subtractExact(timestamp, this.timestep);
                } catch (ArithmeticException ae) {
                  nextTimestamp = Long.MIN_VALUE;
                  nvalues = 0L;
                }
              }
                          
              //
              // Check that the data point should not be stepped over
              //
              
              if (this.steps > 0) {
                this.steps--;
                continue;
              }
              
              if (hasStep) {
                this.steps = this.step - 1L;
              }

              //
              // Sample datapoints
              //
              
              if (hasSample && prng.nextDouble() > sample) {
                continue;
              }

              try {
                if (writeTimestamp) {
                  encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), cell.getTimestamp() * Constants.TIME_UNITS_PER_MS);
                } else if (fetchTTL) {
                  Tag tag = Tag.getTag(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength(), TagType.TTL_TAG_TYPE);
                  if (null != tag && Bytes.SIZEOF_LONG == tag.getTagLength()) {
                    long ttl = Bytes.toLong(tag.getBuffer(), tag.getTagOffset(), tag.getTagLength());
                    encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), ttl * Constants.TIME_UNITS_PER_MS);
                  } else {
                    encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), Long.MAX_VALUE);
                  }
                } else {
                  encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
                }
                nvalues--;
              } catch (IOException ioe) {
                LOG.error("", ioe);
                // FIXME(hbs): LOG?
              }
            }
          }
        }          
      } catch(IOException ioe) {
        LOG.error("",ioe);
        // FIXME(hbs): LOG?
      }
    }
    
    return encoder.getDecoder();
  }
  
  @Override
  public void remove() {
  }
  
  @Override
  public void close() throws Exception {
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_RESULTS, Sensision.EMPTY_LABELS, resultCount);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_CELLS, Sensision.EMPTY_LABELS, cellCount);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_ITERATORS, Sensision.EMPTY_LABELS, 1);

    if (null != this.scanner) { try { this.scanner.close(); } catch (Exception e) { LOG.error("scanner", e); } }
    if (null != this.htable) { try { this.htable.close(); } catch (Exception e) { LOG.error("htable", e); } }
  }
}
