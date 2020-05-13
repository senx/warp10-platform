//
//   Copyright 2020  SenX S.A.S.
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

package io.warp10;

import java.util.Arrays;

public class WarpHexDecoder {
  
  private static final byte[] decodingTable = new byte['f' + 1];
  
  static {
    Arrays.fill(decodingTable, (byte) -1);
    for (int i = 0; i < 10; i++) {
      decodingTable[48 + i] = (byte) i;
    }
    decodingTable['A'] = (byte) 0xa;
    decodingTable['B'] = (byte) 0xb;
    decodingTable['C'] = (byte) 0xc;
    decodingTable['D'] = (byte) 0xd;
    decodingTable['E'] = (byte) 0xe;
    decodingTable['F'] = (byte) 0xf;
    
    decodingTable['a'] = (byte) 0xa;
    decodingTable['b'] = (byte) 0xb;
    decodingTable['c'] = (byte) 0xc;
    decodingTable['d'] = (byte) 0xd;
    decodingTable['e'] = (byte) 0xe;
    decodingTable['f'] = (byte) 0xf;
  }
  
  public static byte[] decode(String str) throws IllegalArgumentException {
    
    int len = str.length();
    
    if (0 != len % 2) {
      throw new IllegalArgumentException("Odd number of hexadecimal digits.");
    }
    
    byte[] decoded = new byte[len >>> 1];
    
    int idx = 0;
    int bidx = 0;
    
    byte b1;
    byte b2;
    
    while(idx < len) {
      char c = str.charAt(idx++);
      char d = str.charAt(idx++);
      if (c > 'f' || d > 'f') {
        throw new IllegalArgumentException("Invalid hex character.");
      }
      b1 = decodingTable[c];
      b2 = decodingTable[d];
      if (b1 < 0 || b2 < 0) {
        throw new IllegalArgumentException("Invalid hex character.");        
      }
      decoded[bidx++] = (byte) ((b1 << 4) | b2);
    }
    
    return decoded;
  }
}
