package io.warp10;

import io.warp10.continuum.gts.UnsafeString;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class WarpURLEncoder {
  public static final String encode(String input, String encoding) throws UnsupportedEncodingException {
    String encoded = URLEncoder.encode(input, encoding);
    
    char[] chars = UnsafeString.getChars(encoded);

    StringBuilder sb = null;

    int lastidx = 0;
    int idx = 0;
    
    //
    // Replace '+' by %20
    //

    while(idx < chars.length) {
      if ('+' == chars[idx]) {
        if (null == sb) {
          sb = new StringBuilder(encoded.length());
        }
        sb.append(chars, lastidx, idx - lastidx);
        sb.append("%20");
        lastidx = ++idx;
      } else {
        idx++;
      }
    }
    
    if (null == sb) {
      return encoded;
    }
    
    if (idx > lastidx) {
      sb.append(chars, lastidx, idx - lastidx);
    }
    
    return sb.toString();
  }  
}
