package io.warp10;

import java.io.InputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.google.common.base.Charsets;

public class Revision {
  
  private static final String REVFILE = "REVISION";
  
  public static final String REVISION;
  
  static {
    String rev = "UNAVAILABLE";
    try {
      InputStream is = WarpConfig.class.getClassLoader().getResourceAsStream(REVFILE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[128];
      while(true) {
        int len = is.read(buf);
        if (len <= 0) {
          break;
        }
        baos.write(buf, 0, len);
      }
      is.close();
      rev = new String(baos.toByteArray(), Charsets.UTF_8);      
    } catch (Exception e) {
    }
    
    REVISION = rev;
  }
}
