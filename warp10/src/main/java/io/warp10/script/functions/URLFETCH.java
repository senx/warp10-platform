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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.StandaloneWebCallService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.binary.Base64;

/**
 * Fetch content from a URL
 */
public class URLFETCH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public URLFETCH(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    if (!stack.isAuthenticated()) {
      throw new WarpScriptException(getName() + " requires the stack to be authenticated.");
    }

    Object o = stack.pop();
    
    if (!(o instanceof String) && !(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a URL or list thereof on top of the stack.");
    }

    List<URL> urls = new ArrayList<URL>();

    try {
      if (o instanceof String) {
        urls.add(new URL(o.toString()));
      } else {
        for (Object oo: (List) o) {
          urls.add(new URL(oo.toString()));
        }      
      }      
    } catch (MalformedURLException mue) {
      throw new WarpScriptException(getName() + " encountered an invalid URL.", mue);
    }
    
    //
    // Check URLs
    //
    
    for (URL url: urls) {     
      if (!StandaloneWebCallService.checkURL(url)) {
        throw new WarpScriptException(getName() + " encountered an invalid URL '" + url + "'");
      }
    }

    //
    // Check that we do not exceed the maxurlfetch limit
    //
    
    AtomicLong urlfetchCount = (AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_COUNT);
    AtomicLong urlfetchSize = (AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_SIZE);
    
    if (urlfetchCount.get() + urls.size() > (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_LIMIT)) {
      throw new WarpScriptException(getName() + " is limited to " + stack.getAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_LIMIT) + " calls.");
    }

    List<Object> results = new ArrayList<Object>();
    
    for (URL url: urls) {
      urlfetchCount.addAndGet(1);

      HttpURLConnection conn = null;

      try {
        conn = (HttpURLConnection) url.openConnection();
        conn.setDoInput(true);
        conn.setDoOutput(false);
        conn.setRequestMethod("GET");

        byte[] buf = new byte[8192];
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        InputStream in = conn.getInputStream();
        
        while(true) {
          int len = in.read(buf);
          
          if (len < 0) {
            break;
          }
          
          if (urlfetchSize.get() + baos.size() + len > (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_MAXSIZE)) {
            throw new WarpScriptException(getName() + " would exceed maximum size of content which can be retrieved via URLFETCH (" + stack.getAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_MAXSIZE) + " bytes)");
          }
          
          baos.write(buf, 0, len);
        }

        urlfetchSize.addAndGet(baos.size());

        List<Object> res = new ArrayList<Object>();
        
        res.add(conn.getResponseCode());
        Map<String,List<String>> hdrs = conn.getHeaderFields();
        
        if (hdrs.containsKey(null)) {
          List<String> statusMsg = hdrs.get(null);
          if (statusMsg.size() > 0) {
            res.add(statusMsg.get(0));
          } else {
            res.add("");
          }
        } else {
          res.add("");
        }

        //
        // Make the headers map modifiable
        //
        
        hdrs = new HashMap<String, List<String>>(hdrs);
        hdrs.remove(null);
        
        res.add(hdrs);
        res.add(Base64.encodeBase64String(baos.toByteArray()));

        results.add(res);
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " encountered an error while fetching '" + url + "'", ioe);
      } finally {
        if (null != conn) {
          conn.disconnect();
        }
      }
    }
    
    stack.push(results);
    
    return stack;
  }
}
