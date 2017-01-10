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
import java.util.Arrays;

import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.sensision.Sensision;

public class InMemoryShardSet {
  // Keep track whether or not a GTSEncoder has all its timestamps in chronological order, speeds up fetching
  // 
  
  private final GTSEncoder[] shards;
  
  /**
   * End timestamp of each shard
   */
  private final long[] shardends;
  
  /**
   * Flags indicating if timestamps are increasingly monotonic
   */
  private final boolean[] chronological;
  
  /**
   * Last timestamp encountered in a shard
   */
  private final long[] lasttimestamp;
  
  /**
   * Length of shards in time units
   */
  private final long shardlen;
  
  /**
   * Number of shards
   */
  private final int shardcount;
  
  public InMemoryShardSet(int shardcount, long shardlen) {
    this.shards = new GTSEncoder[shardcount];
    this.shardends = new long[shardcount];
    this.chronological = new boolean[shardcount];
    this.lasttimestamp = new long[shardcount];
    this.shardlen = shardlen;
    this.shardcount = shardcount;
  }
  
  /**
   * Store the content of a GTSEncoder in the various shards we manage
   * 
   * @param encoder The GTSEncoder instance to store
   */
  public void store(GTSEncoder encoder) throws IOException {
    
    // Get the current time
    long now = TimeSource.getTime();
    long lastShardEnd = shardEnd(now);
    long firstShardStart = lastShardEnd - (shardcount * shardlen) + 1;

    // Get a decoder without copying the encoder array
    GTSDecoder decoder = encoder.getUnsafeDecoder(false);
    
    int lastshard = -1;

    GTSEncoder shardEncoder = null;

    while(decoder.next()) {
      long timestamp = decoder.getTimestamp();
      
      // Ignore timestamp if it is not in the valid range
      if (timestamp < firstShardStart || timestamp > lastShardEnd) {
        continue;
      }
      
      // Compute the shardid
      int shardid = shard(timestamp);
    
      if (shardid != lastshard) {
        shardEncoder = null;
      
        synchronized(this.shards) {
          // Is the shard non existent or has expired?
          if (null == this.shards[shardid] || this.shardends[shardid] < firstShardStart) {
            long end = shardEnd(timestamp);
            this.shards[shardid] = new GTSEncoder(0L);
            this.lasttimestamp[shardid] = end - this.shardlen;
            this.chronological[shardid] = true;
            this.shardends[shardid] = end;          
          }
          
          shardEncoder = this.shards[shardid];
          
          if (timestamp < this.lasttimestamp[shardid]) {
            this.chronological[shardid] = false;
          }
          this.lasttimestamp[shardid] = timestamp;
        }
        
        lastshard = shardid;
      }

      shardEncoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());      
    }
  }
  
  /**
   * Compute the shard id given a timestamp.
   * @param timestamp
   * @return
   */
  private int shard(long timestamp) {
    int shardid;
    
    if (timestamp >= 0) {
      shardid = (int) ((timestamp / shardlen) % shardcount);
    } else {
      shardid = shardcount + (int) ((((timestamp + 1) / shardlen) % shardcount) - 1);
      //shardid = shardcount - (int) ((- (timestamp + 1) / shardlen) % shardcount);
    }
    
    return shardid;
  }
  
  /**
   * Compute the end timestamp of the shard this timestamp
   * belongs to.
   * 
   * @param timestamp
   * @return
   */
  private long shardEnd(long timestamp) {    
    long end;
    
    if (timestamp > 0) {
      end = ((timestamp / shardlen) * shardlen) + shardlen - 1;
    } else {
      end = ((((timestamp + 1) / shardlen) - 1) * shardlen) + shardlen - 1;
    }
    
    return end;
  }
  
  /**
   * Fetches some data from this shard set
   * 
   * @param now The end timestamp to consider (inclusive).
   * @param timespan The timespan or value count to consider.
   * @return
   */
  public GTSDecoder fetch(long now, long timespan) throws IOException {
    return fetchEncoder(now, timespan).getUnsafeDecoder(false);
  }
  
  public GTSEncoder fetchEncoder(long now, long timespan) throws IOException {

    if (timespan < 0) {
      return fetchCountEncoder(now, -timespan);
    }
    
    //
    // Determine the shard id of 'now'
    // We offset it by shardcount so we can safely decrement and
    // still have a positive remainder when doing a modulus
    //
    int nowshard = shard(now) + this.shardcount;
    
    // Compute the first timestamp (included)
    long firstTimestamp = now - timespan + 1;
    
    GTSEncoder encoder = new GTSEncoder(0L);
    
    for (int i = 0; i < this.shardcount; i++) {
      int shard = (nowshard - i) % this.shardcount;
      
      GTSDecoder shardDecoder = null;
      
      synchronized(this.shards) {
        // Ignore a given shard if it does not intersect our current range
        if (this.shardends[shard] < firstTimestamp || (this.shardends[shard] - this.shardlen) >= now) {
          continue;
        }
        
        // Extract a decoder to scan the shard
        if (null != this.shards[shard]) {
          shardDecoder = this.shards[shard].getUnsafeDecoder(false);
        }
      }
      
      if (null == shardDecoder) {
        continue;
      }
      
      // Merge the data from shardDecoder which is in the requested range in 'encoder'
      while(shardDecoder.next()) {
        long ts = shardDecoder.getTimestamp();
        
        if (ts > now || ts < firstTimestamp) {
          continue;
        }
        
        encoder.addValue(ts, shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
      }
    }

    return encoder;
  }
  
  private GTSDecoder fetchCount(long now, long count) throws IOException {
    return fetchCountEncoder(now, count).getUnsafeDecoder(false);
  }
  
  private GTSEncoder fetchCountEncoder(long now, long count) throws IOException {

    //
    // Determine the shard id of 'now'
    // We offset it by shardcount so we can safely decrement and
    // still have a positive remainder when doing a modulus
    //
    int nowshard = shard(now) + this.shardcount;
    
    GTSEncoder encoder = new GTSEncoder();
    
    // Initialize the number of datapoints to fetch
    long nvalues = count;
    
    // Loop over the shards
    for (int i = 0; i < this.shardcount; i++) {
      int shard = (nowshard - i) % this.shardcount;
      
      GTSDecoder shardDecoder = null;
      boolean inorder = true;
      long shardEnd = -1;
      
      synchronized(this.shards) {
        // Ignore a given shard if it is after 'now'
        if (this.shardends[shard] - this.shardlen >= now) {
          continue;
        }
        
        // Extract a decoder to scan the shard
        if (null != this.shards[shard]) {
          shardDecoder = this.shards[shard].getUnsafeDecoder(false);
          inorder = this.chronological[shard];
          shardEnd = this.shardends[shard];
        }
      }
      
      if (null == shardDecoder) {
        continue;
      }
      
      // We now have a shard, we will treat it differently depending if
      // it is in chronological order or not
      
      if (inorder) {
        //
        // If the end timestamp of the shard is before 'now' and the
        // shard contains less than the remaining values we need to fetch
        // we can add everything.
        //
        
        if (shardEnd <= now && shardDecoder.getCount() <= nvalues) {
          while(shardDecoder.next()) {
            encoder.addValue(shardDecoder.getTimestamp(), shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
            nvalues--;
          }
        } else if (shardDecoder.getCount() <= nvalues) {
          // We have a shard with shardEnd > 'now' but which contains less than nvalues,
          // so we add all the values whose timestamp is <= 'now'
          while(shardDecoder.next()) {
            long ts = shardDecoder.getTimestamp();
            if (ts > now) {
              // we can break because we know the encoder is in chronological order.
              break;
            }
            encoder.addValue(ts, shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
            nvalues--;
          }          
        } else {
          //
          // The shard has more values than what we need.
          // If the end of the shard is <= now then we know we must skip count - nvalues and
          // add the rest to the result.
          // Otherwise it's a little trickier
          //
          
          if (shardEnd <= now) {
            long skip = shardDecoder.getCount() - nvalues;
            while(skip > 0 && shardDecoder.next()) {
              skip--;
            }
            while(shardDecoder.next()) {
              encoder.addValue(shardDecoder.getTimestamp(), shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
              nvalues--;
            }          
          } else {
            // We will transfer the datapoints whose timestamp is <= now in a an intermediate encoder
            GTSEncoder intenc = new GTSEncoder();
            while(shardDecoder.next()) {
              long ts = shardDecoder.getTimestamp();
              if (ts > now) {
                // we can break because we know the encoder is in chronological order.
                break;
              }
              intenc.addValue(ts, shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
              nvalues--;
            }
            // Then transfer the intermediate encoder to the result
            shardDecoder = intenc.getUnsafeDecoder(false);
            long skip = shardDecoder.getCount() - nvalues;
            while(skip > 0 && shardDecoder.next()) {
              skip--;
            }
            while(shardDecoder.next()) {
              encoder.addValue(shardDecoder.getTimestamp(), shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
              nvalues--;
            }                      
          }
        }
      } else {
        // The shard decoder is not in chronological order...
        
        // If the shard decoder end is <= 'now' and the decoder contains less values than
        // what is still needed, add everything.
        
        if (shardEnd <= now && shardDecoder.getCount() <= nvalues) {
          while(shardDecoder.next()) {
            encoder.addValue(shardDecoder.getTimestamp(), shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
            nvalues--;
          }          
        } else if(shardDecoder.getCount() <= nvalues) {
          // We have a shard with shardEnd > 'now' but which contains less than nvalues,
          // so we add all the values whose timestamp is <= 'now'
          while(shardDecoder.next()) {
            long ts = shardDecoder.getTimestamp();
            if (ts > now) {
              // we can break because we know the encoder is in chronological order.
              break;
            }
            encoder.addValue(ts, shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
            nvalues--;
          }          
        } else {
          // We have a shard which has more values than what we need and/or whose end
          // is after 'now'
          // We will transfer the datapoints whose timestamp is <= now in a an intermediate encoder
          GTSEncoder intenc = new GTSEncoder();
          while(shardDecoder.next()) {
            long ts = shardDecoder.getTimestamp();
            if (ts > now) {
              continue;
            }
            intenc.addValue(ts, shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
            nvalues--;
          }
          //
          // Now we need to extract the ticks of the intermediary encoder
          //
          long[] ticks = new long[(int) intenc.getCount()];
          int k = 0;
          shardDecoder = intenc.getUnsafeDecoder(false);
          while(shardDecoder.next()) {
            ticks[k++] = shardDecoder.getTimestamp();
          }
          // Now sort the ticks
          Arrays.sort(ticks);
          // We must skip values whose timestamp is <= ticks[ticks.length - nvalues]
          long skipbelow = ticks[ticks.length - (int) nvalues];
          
          // Then transfer the intermediate encoder to the result
          shardDecoder = intenc.getUnsafeDecoder(false);
          while(shardDecoder.next()) {
            long ts = shardDecoder.getTimestamp();
            if (ts < skipbelow) {
              continue;
            }
            encoder.addValue(ts, shardDecoder.getLocation(), shardDecoder.getElevation(), shardDecoder.getValue());
            nvalues--;
          }                      
        }
      }      
    }
    return encoder;
  }
  
  /**
   * Compute the total number of datapoints stored in this shard set.
   * 
   * @return
   */
  public long getCount() {
    long count = 0L;
    
    for (GTSEncoder encoder: shards) {
      if (null != encoder) {
        count += encoder.getCount();
      }
    }
    
    return count;
  }
  
  /**
   * Compute the total size occupied by the encoders in this shard set
   * 
   * @return
   */
  public long getSize() {
    long size = 0L;
    
    for (GTSEncoder encoder: shards) {
      if (null != encoder) {
        size += encoder.size();
      }
    }
    
    return size;
  }
  
  /**
   * Clean expired shards according to 'now'
   * 
   * @param now
   */
  public void clean(long now) {
    long cutoff = shardEnd(now) - this.shardcount * this.shardlen;
    int dropped = 0;
    synchronized(this.shards) {
      for (int i = 0; i < this.shards.length; i++) {
        if (null == this.shards[i]) {
          continue;
        }
        if (this.shardends[i] <= cutoff) {
          this.shards[i] = null;
          dropped++;
        }
      }
    }
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_SHARDS_DROPPED, Sensision.EMPTY_LABELS, dropped);
  }
}
