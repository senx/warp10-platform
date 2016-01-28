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

package io.warp10.standalone;

import io.warp10.WarpDist;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.thrift.data.WebCallMethod;
import io.warp10.script.thrift.data.WebCallRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StandaloneWebCallService extends Thread {
  
  /**
   * User agent to use when making calls
   */
  private static final String ua;
  
  private static boolean launched = false;
  
  private static final List<Pattern> patterns = new ArrayList<Pattern>();
  private static final BitSet exclusion = new BitSet();
  
  static {
    //
    // Read properties to set up proxy etc
    //
    
    Properties props = WarpDist.getProperties();
    
    ua = props.getProperty(Configuration.WEBCALL_USER_AGENT);
    
    //
    // Extract list of forbidden/allowed patterns
    //
    
    String patternConf = props.getProperty(Configuration.WEBCALL_HOST_PATTERNS);
    
    if (null != patternConf) {
      //
      // Split patterns on ','
      //
      
      String[] subpatterns = patternConf.split(",");
  
      int idx = 0;
      
      for (String pattern: subpatterns) {
        if (pattern.contains("%")) {
          try {
            pattern = URLDecoder.decode(pattern, "UTF-8");
          } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(uee);
          }
        }
        
        boolean exclude = false;
        
        if (pattern.startsWith("!")) {
          exclude = true;
          pattern = pattern.substring(1);
        }
        
        //
        // Compile pattern
        //
        
        Pattern p = Pattern.compile(pattern);
        
        patterns.add(p);
        exclusion.set(idx, exclude);
        idx++;
      }
      
      //
      // If no inclusions were specified, add a pass all .* as first pattern
      //
      
      if (exclusion.cardinality() == idx) {
        int n = exclusion.length();
        
        for (int i = n; i >= 1; i--) {
          exclusion.set(i, exclusion.get(i - 1));
        }
        
        exclusion.set(0, false);
        patterns.add(0, Pattern.compile(".*"));
      }
      
    } else {
      //
      // Permit all hosts by default
      //
      patterns.add(Pattern.compile(".*"));
      exclusion.set(0, false);
    }
  }

  private static final ArrayBlockingQueue<WebCallRequest> requests = new ArrayBlockingQueue<WebCallRequest>(1024);
  
  public static synchronized boolean offer(WebCallRequest request) {
    //
    // Launch service if not done yet
    //
    
    if (!launched) {
      launch();
    }
    
    try {
      return requests.offer(request, 10, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      return false;
    }
  }
  
  private static void launch() {
    Thread t = new StandaloneWebCallService();
    t.setDaemon(true);
    t.start();
    launched = true;
  }
  
  @Override
  public void run() {
    
    // FIXME(hbs): use an executor to spawn multiple requests in parallel?
    // maybe we only need to do that in the production setup with a dedicated daemon which
    // reads WebCallRequests off of Kafka
    
    //
    // Loop endlessly, emptying the queue and pushing requests
    //
    
    while(true) {
      WebCallRequest request = requests.poll();
      
      //
      // Sleep some if queue was empty
      //
      
      if (null == request) {
        try { Thread.sleep(100L); } catch (InterruptedException ie) {}
        continue;
      }

      doCall(request);
    }
  }
  
  public static void doCall(WebCallRequest request) {
    //
    // Build the URLConnection 
    //
    
    HttpURLConnection conn = null;
    
    try {
      
      URL url = new URL(request.getUrl());

      if (!checkURL(url)) {
        return;
      }
            
      conn = (HttpURLConnection) url.openConnection();
      
      //
      // Add headers
      //
      
      if (request.getHeadersSize() > 0) {
        for (Entry<String,String> entry: request.getHeaders().entrySet()) {
          conn.addRequestProperty(entry.getKey(), entry.getValue());
        }          
      }
      
      if (null != ua) {
        conn.addRequestProperty("User-Agent", ua);
      }
      
      conn.addRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_WEBCALL_UUIDX), request.getWebCallUUID());
            
      //
      // If issueing a POST request, set doOutput
      //
      
      if (WebCallMethod.POST == request.getMethod()) {
        conn.setDoOutput(true);
        // Make sure we use chunking
        conn.setChunkedStreamingMode(2048);
      } else {
        conn.setDoOutput(false);
      }

      //
      // Connect
      //
      
      conn.connect();
      
      if (WebCallMethod.POST == request.getMethod()) {
        OutputStream os = conn.getOutputStream();
        
        os.write(request.getBody().getBytes("UTF-8"));
        os.close();
      }
      
      //
      // Retrieve response
      //
      
      int code = conn.getResponseCode();                
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } finally {
      if (null != conn) {
        conn.disconnect();
      }
    }    
  }
  
  public static boolean checkURL(URL url) {
    
    String protocol = url.getProtocol();

    //
    // Only honor http/https
    //
    
    if (!("http".equals(protocol)) && !("https".equals(protocol))) {
      return false;
    }

    //
    // Check host patterns in order, consider the final value of 'accept'
    //
    
    String host = url.getHost();
    
    boolean accept = false;
    
    for (int i = 0; i < patterns.size(); i++) {      
      Matcher m = patterns.get(i).matcher(host);
      
      if (m.matches()) {
        accept = !exclusion.get(i);
      }
    }
    
    return accept;
  }
}
