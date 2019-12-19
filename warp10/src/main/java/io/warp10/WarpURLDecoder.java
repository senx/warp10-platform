//
//   Copyright 2019  SenX S.A.S.
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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;

public class WarpURLDecoder {
  public static String decode(String input, Charset charset) throws UnsupportedEncodingException {
    
    // We only decode the % encoding.
    
    if (!input.contains("%")) {
      return input;
    }
    
    // The input contains a '%', if it contains a '+' we need to replace the '+' with '%2B'
    // so decoding is correct.
    
    if (input.contains("+")) {
      StringBuilder sb = new StringBuilder();
      
      int start = 0;
      int idx = 0;
      
      while(idx < input.length()) {
        if ('+' == input.charAt(idx)) {
          sb.append(input, start, idx);
          sb.append("%2B");
          start = idx + 1;
        }
        idx++;
      }
      
      if (idx != start) {
        sb.append(input, start, idx);
      }
      
      input = sb.toString();
    }

    return URLDecoder.decode(input, charset.name());
  }
}
