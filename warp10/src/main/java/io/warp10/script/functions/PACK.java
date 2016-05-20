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

import io.warp10.continuum.gts.UnsafeString;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Pack a list of numeric or boolean values according to a specified format
 */
public class PACK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PACK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a format string on top of the stack.");
    }

    String fmt = o.toString();
    
    o = stack.pop();

    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list of numeric or boolean values.");
    }

    List<Object> values = (List<Object>) o;
    
    for (Object value: values) {
      if (!(value instanceof Number) && !(value instanceof Boolean)) {
        throw new WarpScriptException(getName() + " operates on a list of numeric or boolean values.");
      }
    }
        
    //
    // Parse the format
    //
    
    int idx = 0;

    List<String> types = new ArrayList<String>();
    List<Integer> lengths = new ArrayList<Integer>();
    
    int totalbits = 0;
    
    while(idx < fmt.length()) {
      
      String type = new String(UnsafeString.substring(fmt, idx, idx + 2));
      
      char prefix = fmt.charAt(idx++);
      
      if (idx > fmt.length()) {
        throw new WarpScriptException(getName() + " encountered an invalid format specification.");
      }
      
      int len = 0;
      
      if ('<' == prefix || '>' == prefix) {
        char t = fmt.charAt(idx++);
                
        boolean nolen = false;
        
        if ('L' == t) {
          len = 64;
        } else if ('D' == t) {
          len = 64;
          nolen = true;
        } else {
          throw new WarpScriptException(getName() + " encountered an invalid format specification '" + prefix + t + "'.");
        }
        
        // Check if we have a length
        if (!nolen && idx < fmt.length()) {
          if (fmt.charAt(idx) <= '9' && fmt.charAt(idx) >= '0') {
            len = 0;
            while (idx < fmt.length() && fmt.charAt(idx) <= '9' && fmt.charAt(idx) >= '0') {
              len *= 10;
              len += (int) (fmt.charAt(idx++) - '0');
            }
          }
        }
        
        if (len > 64) {
          throw new WarpScriptException(getName() + " encountered an invalid length for 'L', max length is 64.");
        }
      } else if ('S' == prefix || 's' == prefix) {
        type = "" + prefix;
        if (idx >= fmt.length()) {
          throw new WarpScriptException(getName() + " encountered an invalid Skip specification.");
        }
        if (fmt.charAt(idx) <= '9' && fmt.charAt(idx) >= '0') {
          len = 0;
          while (idx < fmt.length() && fmt.charAt(idx) <= '9' && fmt.charAt(idx) >= '0') {
            len *= 10;
            len += (int) (fmt.charAt(idx++) - '0');
          }
        }
      } else if ('B' == prefix) {
        type = "" + prefix;
        len = 1;
      } else {
        throw new WarpScriptException(getName() + " encountered an invalid format specification '" + prefix + "'.");
      }
      
      types.add(type);
      lengths.add(len);
      
      totalbits += len;
    }

    //
    // Now encode the various values
    //
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream(((totalbits + 7) / 8));
    
    int nbits = 0;
    int vidx = 0;
    
    long curbyte = 0;
    
    for (int i = 0; i < types.size(); i++) {
      
      int len = lengths.get(i);
      long value = 0L;
      
      boolean bigendian = true;

      if ("s".equals(types.get(i))) {
        value = 0L;
        bigendian = false;
      } else if ("S".equals(types.get(i))) {
        value = 0xFFFFFFFFFFFFFFFFL;
        bigendian = false;
      } else {
        Object v = values.get(vidx++);
        
        if (v instanceof Boolean) {
          if (Boolean.TRUE.equals(v)) {
            v = 1L;
          } else {
            v = 0L;
          }
        }

        if ("<D".equals(types.get(i))) {
          bigendian = false;
          value = Double.doubleToRawLongBits(((Number) v).doubleValue());
        } else if (">D".equals(types.get(i))) {
          bigendian = true;
          value = Double.doubleToRawLongBits(((Number) v).doubleValue());        
        } else if ("<L".equals(types.get(i))) {
          bigendian = false;
          value = ((Number) v).longValue();
        } else if (">L".equals(types.get(i))) {
          bigendian = true;          
          value = ((Number) v).longValue();
        } else if ("B".equals(types.get(i))) {
          bigendian = false;
          value = 0 != ((Number) v).longValue() ? 1L : 0L;
        }
      }
      
      if (bigendian) {
        
        value = Long.reverse(value);
        
        if (len < 64) {
          value >>>= (64 - len);
        }
      }
      
      for (int k = 0; k < len; k++) {
        curbyte <<= 1;
        curbyte |= (value & 0x1L);
        value >>= 1;
        nbits++;
        
        if (0 == nbits % 8) {
          baos.write((int) (curbyte & 0xFFL));
          curbyte = 0L;
        }
      }
    }

    if (0 != nbits % 8) {
      curbyte <<= 8 - (nbits % 8);
      baos.write((int) (curbyte & 0xFFL));
    }
    
    stack.push(baos.toByteArray());
    
    return stack;
  }
}
