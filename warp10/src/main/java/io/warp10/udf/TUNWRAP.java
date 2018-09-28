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

package io.warp10.udf;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.warp.sdk.WarpScriptJavaFunctionException;
import io.warp10.warp.sdk.WarpScriptRawJavaFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Charsets;

/**
 * Unwraps a GTSWrapper in typed GTS
 * Since a wrapper can wrap a GTSEncoder which was created directly, the
 * said encoder can contain values of various types.
 * This function will unwrap the wrapper, creating a map keyed by the type and whose values
 * are GTS containing the various types.
 */
public class TUNWRAP extends NamedWarpScriptFunction implements WarpScriptStackFunction, WarpScriptRawJavaFunction {
  
  public TUNWRAP() {
    super("");
  }
  
  public TUNWRAP(String name) {
    super(name);
  }

  @Override
  public int argDepth() {
    return 0;
  }
  
  @Override
  public boolean isProtected() {
    return false;
  }

  @Override
  public List<Object> apply(List<Object> args) throws WarpScriptJavaFunctionException {
    List<Object> result = new ArrayList<Object>();
    try {
      this.apply((WarpScriptStack) args.get(0));
    } catch (WarpScriptException wse) {
      throw new WarpScriptJavaFunctionException(wse);
    }
    return result;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    Set<Object> ticks = new HashSet<Object>();
    
    if (top instanceof Collection) {
      boolean noticks = false;
      for (Object elt: (Collection) top) {
        if (!(elt instanceof Long)) {
          noticks = true;
        }
      }
      if (!noticks) {
        ticks.addAll((Collection<Object>) top);
        top = stack.pop();
      }
    }
    
    if (!(top instanceof String) && !(top instanceof byte[]) && !(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a string or byte array or a list thereof.");
    }
    
    List<Object> inputs = new ArrayList<Object>();
    
    if (top instanceof String || top instanceof byte[]) {
      inputs.add(top);
    } else {
      for (Object o: (List) top) {
        if (!(o instanceof String) && !(o instanceof byte[])) {
          throw new WarpScriptException(getName() + " operates on a string or byte array or a list thereof.");
        }
        inputs.add(o);
      }
    }
    
    List<Object> outputs = new ArrayList<Object>();
    
    for (Object s: inputs) {
      byte[] bytes = s instanceof String ? OrderPreservingBase64.decode(s.toString().getBytes(Charsets.US_ASCII)) : (byte[]) s;
      
      TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
      
      try {
        GTSWrapper wrapper = new GTSWrapper();
        
        deser.deserialize(wrapper, bytes);
     
        GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
        
        GeoTimeSerie[] series = new GeoTimeSerie[4];
        
        for (int i = 0; i < series.length; i++) {
          // Use a heuristic and consider a hint of 25% of the wrapper count
          series[i] = new GeoTimeSerie(wrapper.getLastbucket(), (int) wrapper.getBucketcount(), wrapper.getBucketspan(), (int) wrapper.getCount());
          series[i].setMetadata(decoder.getMetadata());
        }

        GeoTimeSerie gts = null;
          
        while(decoder.next()) {
          long timestamp = decoder.getTimestamp();
          
          if (!ticks.isEmpty() && !ticks.contains(timestamp)) {
            continue;
          }
          
          long location = decoder.getLocation();
          long elevation = decoder.getElevation();
          Object value = decoder.getValue();
          
          if (value instanceof Long) {
            gts = series[0];
          } else if (value instanceof Boolean) {
            gts = series[1];
          } else if (value instanceof String) {
            gts = series[2];
          } else {
            gts = series[3];
          }
          
          GTSHelper.setValue(gts, timestamp, location, elevation, value, false);
        }
        
        Map<String,GeoTimeSerie> typedSeries = new HashMap<String, GeoTimeSerie>();
        
        //
        // Shrink the series
        //
        
        for (int i = 0; i < series.length; i++) {
          GTSHelper.shrink(series[i]);
        }
        
        typedSeries.put("LONG", series[0]);
        typedSeries.put("BOOLEAN", series[1]);
        typedSeries.put("STRING", series[2]);
        typedSeries.put("DOUBLE", series[3]);
        
        outputs.add(typedSeries);
      } catch (TException te) {
        throw new WarpScriptException(getName() + " failed to unwrap GTS.", te);
      }      
    }
    
    if (!(top instanceof List)) {
      stack.push(outputs.get(0));      
    } else {
      stack.push(outputs);
    }
    
    return stack;
  }  
}
