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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import io.warp10.CapacityExtractorOutputStream;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.sensision.Sensision;

public class InMemoryChunkSet {
  /**
   * Maximum number of wasted bytes per encoder returned by fetch. Any waste above this limit
   * will trigger a resize of the encoder.
   */
  private static final int ENCODER_MAX_WASTED = 1024;
  
  private final GTSEncoder[] chunks;
  
  /**
   * End timestamp of each chunk
   */
  private final long[] chunkends;
  
  /**
   * Flags indicating if timestamps are increasingly monotonic
   */
  private final BitSet chronological;
  
  /**
   * Last timestamp encountered in a chunk
   */
  private final long[] lasttimestamp;
  
  /**
   * Length of chunks in time units
   */
  private final long chunklen;
  
  /**
   * Number of chunks
   */
  private final int chunkcount;
  
  public InMemoryChunkSet(int chunkcount, long chunklen) {
    this.chunks = new GTSEncoder[chunkcount];
    this.chunkends = new long[chunkcount];
    this.chronological = new BitSet(chunkcount);
    this.lasttimestamp = new long[chunkcount];
    this.chunklen = chunklen;
    this.chunkcount = chunkcount;
  }
  
  /**
   * Store the content of a GTSEncoder in the various chunks we manage
   * 
   * @param encoder The GTSEncoder instance to store
   */
  public void store(GTSEncoder encoder) throws IOException {
    
    // Get the current time
    long now = TimeSource.getTime();
    long lastChunkEnd = chunkEnd(now);
    long firstChunkStart = lastChunkEnd - (chunkcount * chunklen) + 1;

    // Get a decoder without copying the encoder array
    GTSDecoder decoder = encoder.getUnsafeDecoder(false);
    
    int lastchunk = -1;

    GTSEncoder chunkEncoder = null;

    while(decoder.next()) {
      long timestamp = decoder.getTimestamp();
      
      // Ignore timestamp if it is not in the valid range
      if (timestamp < firstChunkStart || timestamp > lastChunkEnd) {
        continue;
      }
      
      // Compute the chunkid
      int chunkid = chunk(timestamp);
    
      if (chunkid != lastchunk) {
        chunkEncoder = null;
      
        synchronized(this.chunks) {
          // Is the chunk non existent or has expired?
          if (null == this.chunks[chunkid] || this.chunkends[chunkid] < firstChunkStart) {
            long end = chunkEnd(timestamp);
            this.chunks[chunkid] = new GTSEncoder(0L);
            this.lasttimestamp[chunkid] = end - this.chunklen;
            this.chronological.set(chunkid);
            this.chunkends[chunkid] = end;          
          }
          
          chunkEncoder = this.chunks[chunkid];          
        }
        
        lastchunk = chunkid;
      }

      synchronized(this.chunks[chunkid]) {
        if (timestamp < this.lasttimestamp[chunkid]) {
          this.chronological.set(chunkid, false);
        }
        this.lasttimestamp[chunkid] = timestamp;
      }
      
      chunkEncoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());      
    }
  }
  
  /**
   * Compute the chunk id given a timestamp.
   * @param timestamp
   * @return
   */
  private int chunk(long timestamp) {
    int chunkid;
    
    if (timestamp >= 0) {
      chunkid = (int) ((timestamp / chunklen) % chunkcount);
    } else {
      chunkid = chunkcount + (int) ((((timestamp + 1) / chunklen) % chunkcount) - 1);
      //chunkid = chunkcount - (int) ((- (timestamp + 1) / chunklen) % chunkcount);
    }
    
    return chunkid;
  }
  
  /**
   * Compute the end timestamp of the chunk this timestamp
   * belongs to.
   * 
   * @param timestamp
   * @return
   */
  private long chunkEnd(long timestamp) {    
    long end;
    
    if (timestamp > 0) {
      end = ((timestamp / chunklen) * chunklen) + chunklen - 1;
    } else {
      end = ((((timestamp + 1) / chunklen) - 1) * chunklen) + chunklen - 1;
    }
    
    return end;
  }
  
  /**
   * Fetches some data from this chunk set
   * 
   * @param now The end timestamp to consider (inclusive).
   * @param timespan The timespan or value count to consider.
   * @return
   */
  public GTSDecoder fetch(long now, long timespan, CapacityExtractorOutputStream extractor) throws IOException {
    GTSEncoder encoder = fetchEncoder(now, timespan);

    //
    // Resize the encoder so we don't waste too much memory
    //
    
    if (null != extractor) {      
      encoder.writeTo(extractor);
      int capacity = extractor.getCapacity();

      if (capacity - encoder.size() > ENCODER_MAX_WASTED) {
        encoder.resize(encoder.size());        
      }
    }

    return encoder.getUnsafeDecoder(false);
  }

  public GTSDecoder fetch(long now, long timespan) throws IOException {
    return fetch(now, timespan, null);
  }
  
  public List<GTSDecoder> getDecoders() {
    List<GTSDecoder> decoders = new ArrayList<GTSDecoder>();
    
    synchronized (this.chunks) {
      for (int i = 0; i < this.chunks.length; i++) {
        if (null == this.chunks[i]) {
          continue;
        }
        decoders.add(this.chunks[i].getUnsafeDecoder(false));
      }
    }
    
    return decoders;
  }
  
  public GTSEncoder fetchEncoder(long now, long timespan) throws IOException {

    if (timespan < 0) {
      return fetchCountEncoder(now, -timespan);
    }
    
    //
    // Determine the chunk id of 'now'
    // We offset it by chunkcount so we can safely decrement and
    // still have a positive remainder when doing a modulus
    //
    int nowchunk = chunk(now) + this.chunkcount;
    
    // Compute the first timestamp (included)
    long firstTimestamp = now - timespan + 1;
    
    GTSEncoder encoder = new GTSEncoder(0L);
    
    for (int i = 0; i < this.chunkcount; i++) {
      int chunk = (nowchunk - i) % this.chunkcount;
      
      GTSDecoder chunkDecoder = null;
      
      synchronized(this.chunks) {
        // Ignore a given chunk if it does not intersect our current range
        if (this.chunkends[chunk] < firstTimestamp || (this.chunkends[chunk] - this.chunklen) >= now) {
          continue;
        }
        
        // Extract a decoder to scan the chunk
        if (null != this.chunks[chunk]) {
          chunkDecoder = this.chunks[chunk].getUnsafeDecoder(false);
        }
      }
      
      if (null == chunkDecoder) {
        continue;
      }
      
      // Merge the data from chunkDecoder which is in the requested range in 'encoder'
      while(chunkDecoder.next()) {
        long ts = chunkDecoder.getTimestamp();
        
        if (ts > now || ts < firstTimestamp) {
          continue;
        }
        
        encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getValue());
      }
    }

    return encoder;
  }
  
  private GTSDecoder fetchCount(long now, long count) throws IOException {
    return fetchCountEncoder(now, count).getUnsafeDecoder(false);
  }

  private GTSEncoder fetchCountEncoder(long now, long count) throws IOException {
try {
    //
    // Determine the chunk id of 'now'
    // We offset it by chunkcount so we can safely decrement and
    // still have a positive remainder when doing a modulus
    //
    int nowchunk = chunk(now) + this.chunkcount;
    
    //
    // Create a target encoder with a hint based on the average size of datapoints
    //
    
    long oursize = this.getSize();
    long ourcount = this.getCount();
    int avgsize = (int) Math.ceil((double) oursize / (double) ourcount);
    int hint = (int) Math.min((int) (count * avgsize), this.getSize());
    GTSEncoder encoder = new GTSEncoder(0L, null, hint);
    
    // Initialize the number of datapoints to fetch
    long nvalues = count;
    
    // Loop over the chunks
    for (int i = 0; i < this.chunkcount; i++) {
      
      // Are we done fetching datapoints?
      if (nvalues <= 0) {
        break;
      }
            
      int chunk = (nowchunk - i) % this.chunkcount;
      
      GTSDecoder chunkDecoder = null;
      boolean inorder = true;
      long chunkEnd = -1;
      
      synchronized(this.chunks) {
        // Ignore a given chunk if it is after 'now'
        if (this.chunkends[chunk] - this.chunklen >= now) {
          continue;
        }
        
        // Extract a decoder to scan the chunk
        if (null != this.chunks[chunk]) {
          chunkDecoder = this.chunks[chunk].getUnsafeDecoder(false);
          inorder = this.chronological.get(chunk);
          chunkEnd = this.chunkends[chunk];
        }
      }
      
      if (null == chunkDecoder) {
        continue;
      }
      
      // We now have a chunk, we will treat it differently depending if
      // it is in chronological order or not
      
      if (inorder) {
        
        if (chunkEnd <= now && chunkDecoder.getCount() <= nvalues) {
          //
          // If the end timestamp of the chunk is before 'now' and the
          // chunk contains less than the remaining values we need to fetch
          // we can add everything.
          //
          while(chunkDecoder.next()) {
            encoder.addValue(chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getValue());
            nvalues--;
          }
        } else if (chunkDecoder.getCount() <= nvalues) {
          //
          // We have a chunk with chunkEnd > 'now' but which contains less than nvalues,
          // so we add all the values whose timestamp is <= 'now'
          //
          while(chunkDecoder.next()) {
            long ts = chunkDecoder.getTimestamp();
            if (ts > now) {
              // we can break because we know the encoder is in chronological order.
              break;
            }
            encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getValue());
            nvalues--;
          }          
        } else {
          //
          // The chunk has more values than what we need.
          // If the end of the chunk is <= now then we know we must skip count - nvalues and
          // add the rest to the result.
          // Otherwise it's a little trickier
          //
          
          if (chunkEnd <= now) {
            long skip = chunkDecoder.getCount() - nvalues;
            while(skip > 0 && chunkDecoder.next()) {
              skip--;
            }
            while(chunkDecoder.next()) {
              encoder.addValue(chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getValue());
              nvalues--;
            }          
          } else {            
            // Duplicate the decoder so we can scan it again later
            GTSDecoder dupdecoder = chunkDecoder.duplicate();
            // We will count the number of datapoints whose timestamp is <= now
            long valid = 0;
            while(chunkDecoder.next()) {
              long ts = chunkDecoder.getTimestamp();
              if (ts > now) {
                // we can break because we know the encoder is in chronological order.
                break;
              }
              valid++;
            }
            
            chunkDecoder = dupdecoder;
            long skip = valid - nvalues;
            while(skip > 0 && chunkDecoder.next()) {
              skip--;
              valid--;
            }
            while(valid > 0 && chunkDecoder.next()) {
              encoder.addValue(chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getValue());
              nvalues--;
              valid--;
            }                      
          }
        }
      } else {
        // The chunk decoder is not in chronological order...
        
        // Create a duplicate of the buffer in case we need it later
        GTSDecoder dupdecoder = chunkDecoder.duplicate();
                
        if (chunkEnd <= now && chunkDecoder.getCount() <= nvalues) {
          //
          // If the chunk decoder end is <= 'now' and the decoder contains less values than
          // what is still needed, add everything.
          //
          while(chunkDecoder.next()) {
            encoder.addValue(chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getValue());
            nvalues--;
          }          
        } else if(chunkDecoder.getCount() <= nvalues) {
          //
          // We have a chunk with chunkEnd > 'now' but which contains less than nvalues,
          // so we add all the values whose timestamp is <= 'now'
          //
          while(chunkDecoder.next()) {
            long ts = chunkDecoder.getTimestamp();
            if (ts > now) {
              // we skip the value as the encoder is not in chronological order
              continue;
            }
            encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getValue());
            nvalues--;
          }          
        } else {
          // We have a chunk which has more values than what we need and/or whose end
          // is after 'now'
          // We will transfer the datapoints whose timestamp is <= now in an array so we can sort them
          
          long[] ticks = new long[(int) chunkDecoder.getCount()];

          int idx = 0;
          
          while(chunkDecoder.next()) {
            long ts = chunkDecoder.getTimestamp();
            if (ts > now) {
              continue;
            }
            
            ticks[idx++] = ts;
          }
          
          if (idx > 1) {
            Arrays.sort(ticks, 0, idx);
          }
        
          chunkDecoder = dupdecoder;
          
          // We must skip values whose timestamp is <= ticks[idx - nvalues]
          
          if (idx > nvalues) {
            long lowest = ticks[idx - (int) nvalues];
            
            while(chunkDecoder.next() && nvalues > 0) {
              long ts = chunkDecoder.getTimestamp();
              if (ts < lowest) {
                continue;
              }
              encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getValue());
              nvalues--;
            }                                  
          } else {
            // The intermediary decoder has less than nvalues whose ts is <= now, transfer everything
            chunkDecoder = dupdecoder;
            
            int valid = idx;
            
            while(valid > 0 && chunkDecoder.next()) {
              long ts = chunkDecoder.getTimestamp();
              if (ts > now) {
                continue;
              }
              encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getValue());
              nvalues--;
              valid--;
            }                                              
          }
        }
      }      
    }
        
    return encoder;
} catch (Throwable t) {
  t.printStackTrace();
  throw t;
}
  }
  
  /**
   * Compute the total number of datapoints stored in this chunk set.
   * 
   * @return
   */
  public long getCount() {
    long count = 0L;
    
    for (GTSEncoder encoder: chunks) {
      if (null != encoder) {
        count += encoder.getCount();
      }
    }
    
    return count;
  }
  
  /**
   * Compute the total size occupied by the encoders in this chunk set
   * 
   * @return
   */
  public long getSize() {
    long size = 0L;
    
    for (GTSEncoder encoder: chunks) {
      if (null != encoder) {
        size += encoder.size();
      }
    }
    
    return size;
  }
  
  /**
   * Clean expired chunks according to 'now'
   * 
   * @param now
   */
  public long clean(long now) {
    long cutoff = chunkEnd(now) - this.chunkcount * this.chunklen;
    int dropped = 0;
    long droppedDatapoints = 0L;
    synchronized(this.chunks) {
      for (int i = 0; i < this.chunks.length; i++) {
        if (null == this.chunks[i]) {
          continue;
        }
        if (this.chunkends[i] <= cutoff) {
          droppedDatapoints += this.chunks[i].getCount();
          this.chunks[i] = null;
          dropped++;
        }
      }
    }
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_CHUNKS, Sensision.EMPTY_LABELS, dropped);
    
    return droppedDatapoints;
  }
  
  /**
   * Optimize all non current chunks by shrinking their buffers.
   * 
   * @param now
   */
  long optimize(CapacityExtractorOutputStream out, long now, AtomicLong allocation) {
    int currentChunk = chunk(now);
    
    long reclaimed = 0L;

    synchronized(this.chunks) {      
      for (int i = 0; i < this.chunks.length; i++) {
        if (null == this.chunks[i] || i == currentChunk) {
          continue;
        }
        int size = this.chunks[i].size();
        
        try {
          this.chunks[i].writeTo(out);
          int capacity = out.getCapacity();
          
          if (capacity > size) {
            this.chunks[i].resize(size);
            allocation.addAndGet(size);
            reclaimed += (capacity - size);
          }          
        } catch (IOException ioe) {          
        }
      }
    }
    
    return reclaimed;
  }
}
