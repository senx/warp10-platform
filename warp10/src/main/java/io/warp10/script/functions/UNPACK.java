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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Pack a list of numeric or boolean values according to a specified format
 */
public class UNPACK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public UNPACK(String name) {
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
    
    if (!(o instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on an array of bytes.");
    }
    
    byte[] data = (byte[]) o;
    
    //
    // Parse the format
    //
    
    int idx = 0;

    List<String> types = new ArrayList<String>();
    List<Integer> lengths = new ArrayList<Integer>();
    
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
        
        if ('L' == t || 'U' == t) {
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
    }

    //
    // Now decode the various values
    //
    
    int bitno = 0;
    
//    BitSet bits = new BitSet(data.length * 8);
//    
//    for (int i = 0; i < data.length * 8; i++) {
//      if ((data[i/8] & (1 << (7 - (i % 8)))) != 0) {
//        bits.set(i);
//      }
//    }

    //
    // Invert the bits of the input data
    //
    
    byte[] atad = new byte[data.length];
    
    for (int i = 0; i < data.length; i++) {
      atad[i] = (byte) ((((((long) data[i]) & 0xFFL) * 0x0202020202L & 0x010884422010L) % 1023L) & 0xFFL);
    }
    
    BitSet bits = BitSet.valueOf(atad);
    
    List<Object> values = new ArrayList<Object>();
    
    for (int i = 0; i < types.size(); i++) {
      
      int len = lengths.get(i);
      
      if ("s".equals(types.get(i)) || "S".equals(types.get(i))) {
        bitno += len;
        continue;
      }
      
      if ("B".equals(types.get(i))) {
        values.add(bits.get(bitno++));
        continue;
      }
      
      boolean bigendian = true;
      
      if (types.get(i).startsWith("<")) {
        bigendian = false;
      } else {
        bigendian = true;
      }
      
      //
      // Extract bits
      //
      
      long value = 0L;
      
      for (int k = 0; k < len; k++) {
        value <<= 1;
        
        if (!bigendian) {
          value |= (bits.get(bitno + len - 1 - k) ? 1 : 0);
        } else {
          value |= (bits.get(bitno + k) ? 1 : 0);
        }        
      }

      bitno += len;

      switch (types.get(i).charAt(1)) {
        case 'D':
          values.add(Double.longBitsToDouble(value));
          break;
        case 'L':
          values.add((value << (64 - len)) >> (64 - len));
          break;
        case 'U':
          values.add(value);
          break;
      }
    }

    stack.push(values);
    
    return stack;
  }
}
