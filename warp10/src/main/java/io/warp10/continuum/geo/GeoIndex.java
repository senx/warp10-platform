//
//   Copyright 2016  Cityzen Data
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

package io.warp10.continuum.geo;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.geoxp.geo.GeoBloomFilter;

public class GeoIndex {
  /**
   * Depth of index chunks in ms
   */
  private final long depth;
  
  /**
   * Number of chunks in index
   */
  private final int chunks;
  
  /**
   * Lower timestamp of the current index
   */
  long currentLowerTimestamp = Long.MIN_VALUE;
  
  /**
   * Lower timestamp of the previous index
   */
  long previousLowerTimestamp = Long.MIN_VALUE;
  
  /**
   * Index resolution (1 -> 15)
   */
  int resolution;
  
  /**
   * Actual index data. Outside key is the time chunk, inside key is the GTS id, value is a GeoBloomFilter as provided by GeoXPLib
   */
  private final Map<Long,Map<String, GeoBloomFilter>> index;
  
  //Map<String, GeoBloomFilter> current = null;
  
  //Map<String, GeoBloomFilter> previous = null;
  
  private final Map<String, long[]> lkpIndex;
  
  public GeoIndex(int resolution, int chunks, long depth) {
    
    this.resolution = resolution;
    this.depth = depth;
    this.chunks = chunks;
    
    if (0 == this.depth) {
      this.lkpIndex = new HashMap<String, long[]>();
      this.index = null;
    } else {
      this.lkpIndex = null;
      this.index = new HashMap<Long, Map<String,GeoBloomFilter>>();
    }
  }
  
  public long indexLKP(GTSEncoder encoder) {
    GTSDecoder decoder = encoder.getDecoder();
    
    long timestamp = Long.MIN_VALUE;
    long location = GeoTimeSerie.NO_LOCATION;
    
    while(decoder.next()) {
      long loc = decoder.getLocation();
      long ts = decoder.getTimestamp();
      if (GeoTimeSerie.NO_LOCATION != loc && ts > timestamp) {        
        location = loc;
        timestamp = ts;
      }
    }
    
    if (GeoTimeSerie.NO_LOCATION != location) {
      long[] cells = GeoXPLib.indexable(location);
      Arrays.sort(cells);
      String gtsId = GTSHelper.gtsIdToString(encoder.getClassId(), encoder.getLabelsId(), false);      
      lkpIndex.put(gtsId, cells);
    }
    
    return 1;
  }
  
  /**
   * Index an encoder.
   * 
   * @param encoder
   * @return The number of datapoints actually indexed
   */
  public long index(GTSEncoder encoder) {
    
    //
    // Index LKP if depth is 0
    //
    
    if (0 == this.depth) {
      return indexLKP(encoder);
    }
    
    long now = System.currentTimeMillis();
    
    //
    // Determine first/last valid chunks
    //
    
    long lastchunk = now / this.depth;    
    long firstchunk = lastchunk - this.chunks + 1;
    
    synchronized(this) {
      //
      // If the total number of chunks in 'index' is above 'chunks',
      // remove expired chunks
      //
      
      if (this.index.size() > this.chunks) {
        Set<Long> indexChunks = new HashSet<Long>();
        indexChunks.addAll(this.index.keySet());
        for (long chunk: indexChunks) {
          if (chunk < firstchunk || chunk > lastchunk) {
            this.index.remove(chunk);
          }
        }
      }
    }
    
    GTSDecoder decoder = encoder.getDecoder(true);
    GeoBloomFilter filter = null;
    Map<String,GeoBloomFilter> lastindex = null;
    Map<String, GeoBloomFilter> chunkIndex = null;
    long previousChunk = Long.MIN_VALUE;
    
    String gtsId = GTSHelper.gtsIdToString(encoder.getClassId(), encoder.getLabelsId(), false);

    long indexed = 0L;
    
    while(decoder.next()) {
      long location = decoder.getLocation();
      
      // Skip datapoints with no location        
      if (GeoTimeSerie.NO_LOCATION == location) {
        continue;
      }

      long ts = decoder.getTimestamp() / Constants.TIME_UNITS_PER_MS;

      long chunk = ts / this.depth;
      
      //
      // Skip entries outside the valid range of timestamps
      //
      
      if (chunk < firstchunk || chunk > lastchunk) {
        continue;
      }
      
      
      synchronized(this) {
        if (chunk != previousChunk) {
          chunkIndex = this.index.get(chunk);
          
          if (null == chunkIndex) {
            this.index.put(chunk, new HashMap<String, GeoBloomFilter>());
          }
        
          chunkIndex = this.index.get(chunk);
          previousChunk = chunk;
        }
      }
      
      //
      // Do the actual indexing
      //
      
      synchronized(this) {
        if (chunkIndex != lastindex) {
          filter = chunkIndex.get(gtsId);
          if (null == filter) {
            filter = new GeoBloomFilter(this.resolution, null, null, 6, true);
            chunkIndex.put(gtsId, filter);
          }          
        }
        lastindex = chunkIndex;
      }
        
      filter.add(location);
      indexed++;
    }      
    
    return indexed;
  }
  
  private Set<String> findLKP(Collection<String> unfilteredGTS, GeoXPShape area, boolean inside) {
    Set<String> gts = new HashSet<String>();
    
    long[] areaCells = GeoXPLib.getCells(area);
        
    for (String id: unfilteredGTS) {
      long[] cells = lkpIndex.get(id);
      
      if (null == cells) {
        continue;
      }
    
      for (long cell: areaCells) {
        if (Arrays.binarySearch(cells, cell) >= 0) {
          gts.add(id);
          break;
        }
      }      
    }
    
    if (!inside) {
      Set<String> outgts = new HashSet<String>();
      outgts.addAll(unfilteredGTS);
      outgts.removeAll(gts);
      gts = outgts;
    }

    return gts;
  }
  
  /**
   * Find GTS which had values (or not) in a given area in a given time range
   * @param unfilteredGTS
   * @param area
   * @param inside
   * @param startTS start of timerange (inclusive), in ms
   * @param endTS end of timerange (inclusive), in ms
   * @return
   */
  public Set<String> find(Collection<String> unfilteredGTS, GeoXPShape area, boolean inside, long startTS, long endTS) {
    
    if (0 == this.depth) {
      return findLKP(unfilteredGTS, area, inside);
    }
    
    Set<String> gts = new HashSet<String>();

    //
    // Extract cells from shape
    //
    
    long[] cells = GeoXPLib.getCells(area);

    long now = System.currentTimeMillis();
    long lastchunk = now / this.depth;
    long firstchunk = lastchunk - this.chunks + 1;
    
    long startChunk = startTS / this.depth;
    long endChunk = endTS / this.depth;

    for (String id: unfilteredGTS) {
      boolean found = false;
      for (long chunk = firstchunk; chunk <= lastchunk; chunk++) {
        
        if (chunk < startChunk || chunk > endChunk) {
          continue;
        }

        Map<String, GeoBloomFilter> chunkIndex = this.index.get(chunk);

        if (null == chunkIndex) {
          continue;
        }

        GeoBloomFilter filter = chunkIndex.get(id);
        
        if (null == filter) {
          continue;
        }

        for (long cell: cells) {
                                   
          if (!filter.contains(cell)) {
            continue;
          }

          //
          // Since the Bloom filter has a non zero probability of false positive, we
          // mitigate this probability by checking all cells containing 'cell'. If they
          // are also contained by the Bloom filter, then we assume the containment is real
          //
          
          do {
            cell = GeoXPLib.parentCell(cell);
          } while (0L != cell && filter.contains(cell));
            
          if (0 == cell) {
            gts.add(id);
            found = true;
            break;
          }
        }

        //
        // If a cell was found, stop checking the others
        //
        
        if (found) {
          break;
        }
      }      
    }
   
    //
    // Invert the result if 'inside' is false
    //
    
    if (!inside) {
      Set<String> outgts = new HashSet<String>();
      outgts.addAll(unfilteredGTS);
      outgts.removeAll(gts);
      gts = outgts;
    }
    
    return gts;
  }  
  
  /**
   * Return an estimated memory footprint of this index
   * @return
   */
  public long size() {
    long total = 0L;
    
    if (0 == this.depth) {
      // Key strings (8 chars) + overhead (24 bytes)
      total += this.lkpIndex.size() * (16 + 24);
      // Actual locations (15 longs) + overhead (24 bytes)
      total += this.lkpIndex.size() * (8 * 15 + 24); 
    } else {
      //
      // We only account for the GeoBloomFilter actual bits, not for any
      // overhead
      //
      for (Map<String,GeoBloomFilter> chunkIndex: this.index.values()) {
        total += chunkIndex.size() * (16 + 24);
        for (GeoBloomFilter filter: chunkIndex.values()) {
          total += filter.size();
        }        
      }
    }
    
    return total;
  }
}    
