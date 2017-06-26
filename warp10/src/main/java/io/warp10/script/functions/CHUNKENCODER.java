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

package io.warp10.script.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.CapacityExtractorOutputStream;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Chunk a GTSEncoder instance into multiple encoders.
 * 
 */
public class CHUNKENCODER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String OVERLAP = "overlap";
  private static final String LASTCHUNK = "lastchunk";
  private static final String CHUNKWIDTH = "chunkwidth";
  private static final String CHUNKCOUNT = "chunkcount";
  private static final String CHUNKLABEL = "chunklabel";
  private static final String KEEPEMPTY = "keepempty";

  private final boolean withOverlap;
  
  public CHUNKENCODER(String name, boolean withOverlap) {
    super(name);
    this.withOverlap = withOverlap;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    Map<String,Object> params = new HashMap<String, Object>();
    
    if (!(top instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects on top of the stack a boolean indicating whether or not to keep empty chunks.");
    }

    boolean keepempty = (boolean) top;
    
    top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects the name of the 'chunk' label below the top of the stack.");
    }
    
    String chunklabel = (String) top;
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of chunks under the 'chunk' label.");
    }
    
    long chunkcount = (long) top;
    
    long overlap = 0L;
    
    if (this.withOverlap) {
      top = stack.pop();
      if (!(top instanceof Long)) {
        throw new WarpScriptException(getName() + " expects an overlap below the number of chunks.");
      }
      overlap = (long) top;
      
      if (overlap < 0) {
        overlap = 0;
      }      
    }
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a chunk width on top of the end timestamp of the most recent chunk.");
    }
    
    long chunkwidth = (long) top;

    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects the end timestamp of the most recent chunk under the chunk width.");
    }
    
    long lastchunk = (long) top;

    top = stack.pop();
    
    if (!(top instanceof GTSEncoder)) {
      throw new WarpScriptException(getName() + " operates on an encoder instance.");
    }
    
    stack.push(chunk((GTSEncoder) top, lastchunk, chunkwidth, chunkcount, chunklabel, keepempty, overlap));
    
    return stack;
  }
  
  private static List<GTSEncoder> chunk(GTSEncoder encoder, long lastchunk, long chunkwidth, long chunkcount, String chunklabel, boolean keepempty, long overlap) throws WarpScriptException {
    
    if (overlap < 0 || overlap > chunkwidth) {
      throw new WarpScriptException("Overlap cannot exceed chunk width.");
    }
    
    //
    // Check if 'chunklabel' exists in the GTS labels
    //
    
    Metadata metadata = encoder.getMetadata();
    
    if(metadata.getLabels().containsKey(chunklabel)) {
      throw new WarpScriptException("Cannot operate on encoders which already have a label named '" + chunklabel + "'");
    }

    // Order the chunks by descending chunkid
    TreeMap<Long, GTSEncoder> chunks = new TreeMap<Long,GTSEncoder>(new Comparator<Long>() {
      @Override
      public int compare(Long o1, Long o2) {
        return -o1.compareTo(o2);
      }
    });
    
    // Encoder has 0 values, if lastchunk was 0, return an empty list as we are unable to produce chunks
    if (0 == encoder.getCount() && 0 == encoder.size() && 0L == lastchunk) {
      return new ArrayList<GTSEncoder>();
    }
    
    //
    // Set chunkcount to Integer.MAX_VALUE if it's 0
    //
    
    boolean zeroChunkCount = false;
    
    if (0 == chunkcount) {
      chunkcount = Integer.MAX_VALUE;
      zeroChunkCount = true;
    }
    
    //
    // Loop on the chunks
    //
    
    GTSDecoder decoder = encoder.getUnsafeDecoder(false);
    
    try {
      while(decoder.next()) {
        long timestamp = decoder.getTimestamp();
        
        // Compute chunkid for the current timestamp (the end timestamp of the chunk timestamp is in)
        
        long chunkid = 0L;
        
        // Compute delta from 'lastchunk'
        
        long delta = timestamp - lastchunk;
        
        // Compute chunkid
        
        if (delta < 0) { // timestamp is before 'lastchunk'
          if (0 != -delta % chunkwidth) {
            delta += (-delta % chunkwidth);
          }
          chunkid = lastchunk + delta;
        } else if (delta > 0) { // timestamp if after 'lastchunk'
          if (0 != delta % chunkwidth) {
            delta = delta - (delta % chunkwidth) + chunkwidth;
          }
          chunkid = lastchunk + delta;
        } else {
          chunkid = lastchunk;
        }
        
        // Add datapoint in the chunk it belongs to
        
        GTSEncoder chunkencoder = chunks.get(chunkid);
        
        if (null == chunkencoder) {
          chunkencoder = new GTSEncoder(0L);
          chunkencoder.setMetadata(encoder.getMetadata());
          chunkencoder.getMetadata().putToLabels(chunklabel, Long.toString(chunkid));
          chunks.put(chunkid, chunkencoder);
        }
        
        chunkencoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());
        
        // Add datapoint to adjacent chunk if overlap is > 0
        
        if (overlap > 0) {
          // Check next chunk
          if (timestamp >= chunkid + 1 - overlap) {
            chunkencoder = chunks.get(chunkid + chunkwidth);
            if (null == chunkencoder) {
              chunkencoder = new GTSEncoder(0L);
              chunkencoder.setMetadata(encoder.getMetadata());
              chunkencoder.getMetadata().putToLabels(chunklabel, Long.toString(chunkid + chunkwidth));          
              chunks.put(chunkid + chunkwidth, chunkencoder);
            }
            chunkencoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());          
          }
          
          // Check previous chunk
          if (timestamp <= chunkid - chunkwidth + overlap) {
            chunkencoder = chunks.get(chunkid - chunkwidth);
            if (null == chunkencoder) {
              chunkencoder = new GTSEncoder(0L);
              chunkencoder.setMetadata(encoder.getMetadata());
              chunkencoder.getMetadata().putToLabels(chunklabel, Long.toString(chunkid - chunkwidth));          
              chunks.put(chunkid - chunkwidth, chunkencoder);
            }
            chunkencoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());                    
          }
        }
      }      
    } catch (IOException ioe) {
      throw new WarpScriptException("Encountered an error while creating chunks.", ioe);
    }
    
    ArrayList<GTSEncoder> encoders = new ArrayList<GTSEncoder>();
    
    // Now retain only the chunks we want according to chunkcount and lastchunk.
    
    CapacityExtractorOutputStream extractor = new CapacityExtractorOutputStream();

    for (long chunkid: chunks.keySet()) {

      // Do we have enough chunks?
      if (!zeroChunkCount && encoders.size() >= chunkcount) {
        break;
      }
      
      // Skip chunk if it is not in the requested range
      if (0 != lastchunk && chunkid > lastchunk) {
        continue;
      }
      
      GTSEncoder enc = chunks.get(chunkid);
      
      // Shrink encoder if it has more than 10% unused memory
      try {
        enc.writeTo(extractor);
        if (extractor.getCapacity() > 1.1 * enc.size()) {
          enc.resize(enc.size());
        }
      } catch (IOException ioe) {
        throw new WarpScriptException("Encountered an error while optimizing chunks.", ioe);
      }
      
      encoders.add(enc);
    }

    return encoders;
  }
}
