package io.warp10.continuum.store;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.MetadataIdComparator;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.filter.SlicedRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.primitives.Longs;

public class OptimizedSlicedRowFilterGTSDecoderIterator extends GTSDecoderIterator implements AutoCloseable {
  
  private final List<List<Metadata>> groups;
  
  private int groupidx = 0;
  private SlicedRowFilterGTSDecoderIterator iterator = null;
  
  private final long now;
  private final long timespan;
  private final Connection conn;
  private final TableName tableName;
  private final byte[] colfam;
  private final boolean useBlockCache;
  private final KeyStore keystore;
  
  public OptimizedSlicedRowFilterGTSDecoderIterator(long now, long timespan, List<Metadata> metadatas, Connection conn, TableName tableName, byte[] colfam, KeyStore keystore, boolean useBlockCache) {
    
    this.now = now;
    this.timespan = timespan;
    this.conn = conn;
    this.tableName = tableName;
    this.colfam = colfam;
    this.useBlockCache = useBlockCache;
    this.keystore = keystore;
    
    //
    // Sort the Metadata
    //
    
    Collections.sort(metadatas, MetadataIdComparator.COMPARATOR);
    
    //
    // Create start/end keys for each metadata, we know they will be sorted
    //

    List<byte[]> keys = new ArrayList<byte[]>(metadatas.size() * 2);
    
    for (Metadata meta: metadatas) {
      byte[][] metakeys = SlicedRowFilterGTSDecoderIterator.getKeys(meta, now, timespan);
      keys.add(metakeys[0]);
      keys.add(metakeys[1]);
    }
    
    //
    // Now iterate over the list of Metadata, grouping them as long as they span a continuous set of Regions,
    // i.e. the end region containing the end key is the same as the region containing the start key of the
    // next metadata
    //
    
    this.groups = new ArrayList<List<Metadata>>();
    
    List<Metadata> group = new ArrayList<Metadata>();
    
    group.add(metadatas.get(0));
    groups.add(group);
       
    //
    // Retrieve regionKeys
    //
    
    List<byte[]> regionKeys = HBaseRegionKeys.getRegionKeys(conn, tableName);

    int previousStartKeyIndex = Collections.binarySearch(regionKeys, keys.get(0), Bytes.BYTES_COMPARATOR);
    int previousEndKeyIndex = Collections.binarySearch(regionKeys, keys.get(1), Bytes.BYTES_COMPARATOR);

    if (previousStartKeyIndex < 0) {
      previousStartKeyIndex = -1 - previousStartKeyIndex;
    }
    if (previousEndKeyIndex < 0) {
      previousEndKeyIndex = -1 - previousEndKeyIndex;
    }

    previousStartKeyIndex >>>= 1;
    previousEndKeyIndex >>>= 1;
    
    for (int i = 1; i < metadatas.size(); i++) {
      
      int currentStartKeyIndex = Collections.binarySearch(regionKeys, keys.get(i * 2), Bytes.BYTES_COMPARATOR);
      int currentEndKeyIndex = Collections.binarySearch(regionKeys, keys.get(i * 2 + 1), Bytes.BYTES_COMPARATOR);

      //
      // If both start and end key fall outside of any region, then ignore the current Metadata as it does not have
      // any data
      //
      
      boolean neg = false;
      
      if (currentStartKeyIndex < 0 && currentEndKeyIndex < 0) {
        neg = true;
      }
      
      if (currentStartKeyIndex < 0) {
        currentStartKeyIndex = -1 - currentStartKeyIndex;
        //currentStartKeyIndex = currentStartKeyIndex >>> 1;
      }
      
      if (currentEndKeyIndex < 0) {                    
        currentEndKeyIndex = -1 - currentEndKeyIndex;
        //currentEndKeyIndex = currentEndKeyIndex >>> 1;
      }

      // If both values are even and equal, it means that they are outside a region, so notify HBaseKeys that it should
      // reload the regions. We could also ignore the metadata, but that could lead to weird behavior as seen by the
      // requesting client.
      if (neg && 0 == currentStartKeyIndex % 2 && 0 == currentEndKeyIndex % 2 && currentStartKeyIndex == currentEndKeyIndex) {
        /*
        // Remove the current Metadata from the list
        metadatas.remove(i);
        // Decrement i so we leave i to the same index after it's incremented
        i--;
        continue;
        */
      }
              
      //
      // Change the indices so they represent region index and not bound index
      //
      
      currentStartKeyIndex = currentStartKeyIndex >>> 1;
      currentEndKeyIndex = currentEndKeyIndex >>> 1;
      
      //
      // If the region containing the end key of the previous GTS differs from the region
      // containing the start key of the current GTS by more than 1, then issue a new group as we likely
      // would span regions of no interest otherwise
      //
      
      if (currentStartKeyIndex - previousEndKeyIndex > 1) {
        group = new ArrayList<Metadata>();
        groups.add(group);
      }
      
      group.add(metadatas.get(i));
      
      previousStartKeyIndex = currentStartKeyIndex;
      previousEndKeyIndex = currentEndKeyIndex;
    }          
  }
  
  @Override
  public boolean hasNext() {
    
    boolean hasNext = false;
    
    if (null != iterator) {
      hasNext = iterator.hasNext();
    }
    
    while(!hasNext && groupidx < groups.size()) {
      if (null != iterator) {
        try { iterator.close(); } catch (Exception e) {}
      }
      iterator = new SlicedRowFilterGTSDecoderIterator(now, timespan, groups.get(groupidx), conn, tableName, colfam, keystore, useBlockCache);
      groupidx++;
      hasNext = iterator.hasNext();
    }
    
    return hasNext;
  }
  
  @Override
  public void close() throws Exception {
    if (null != iterator) {
      try { iterator.close(); } catch (Exception e) {}
      iterator = null;
    }
  }
  
  @Override
  public GTSDecoder next() {
    return iterator.next();
  }
}
