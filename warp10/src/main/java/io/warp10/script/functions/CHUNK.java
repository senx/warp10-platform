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

package io.warp10.script.functions;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Apply chunk on GTS instances
 * 
 * CHUNK expects the following parameters on the stack:
 * 
 * 5: lastchunk end Timestamp of the most recent chunk to create
 * 4: chunkwidth Width of each chunk in time units
 * 3: chunkcount Number of chunks GTS to produce
 * 2: chunklabel Name of the label containing the id of the chunk
 * 1: keepempty Should we keep empty chunks
 */
public class CHUNK extends GTSStackFunction {
  
  private static final String OVERLAP = "overlap";
  private static final String LASTCHUNK = "lastchunk";
  private static final String CHUNKWIDTH = "chunkwidth";
  private static final String CHUNKCOUNT = "chunkcount";
  private static final String CHUNKLABEL = "chunklabel";
  private static final String KEEPEMPTY = "keepempty";
  
  private final boolean withOverlap;
  
  public CHUNK(String name, boolean withOverlap) {
    super(name);
    this.withOverlap = withOverlap;
  }
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    Map<String,Object> params = new HashMap<String, Object>();
    
    if (!(top instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects on top of the stack a boolean indicating whether or not to keep empty chunks.");
    }

    params.put(KEEPEMPTY, (boolean) top);
    
    top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects the name of the 'chunk' label below the top of the stack.");
    }
    
    params.put(CHUNKLABEL, (String) top);
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of chunks under the 'chunk' label.");
    }
    
    params.put(CHUNKCOUNT, (long) top);
    
    if (this.withOverlap) {
      top = stack.pop();
      if (!(top instanceof Long)) {
        throw new WarpScriptException(getName() + " expects an overlap below the chunk label.");
      }
      long overlap = (long) top;
      
      if (overlap < 0) {
        overlap = 0;
      }
      
      params.put(OVERLAP, overlap);
    }
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a chunk width before the last chunk.");
    }
    
    params.put(CHUNKWIDTH, (long) top);

    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects the end timestamp of the most recent chunk under the chunk width.");
    }
    
    params.put(LASTCHUNK, (long) top);
    
    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    long lastchunk = (long) params.get(LASTCHUNK);
    long chunkwidth = (long) params.get(CHUNKWIDTH);
    long chunkcount = (long) params.get(CHUNKCOUNT);
    String chunklabel = (String) params.get(CHUNKLABEL);
    boolean keepempty = (boolean) params.get(KEEPEMPTY);
    
    long overlap = 0;
    
    if (params.containsKey(OVERLAP)) {
      overlap = (long) params.get(OVERLAP);
    }
    
    List<GeoTimeSerie> result = GTSHelper.chunk(gts, lastchunk, chunkwidth, chunkcount, chunklabel, keepempty, overlap);

    return result;
  }
}
