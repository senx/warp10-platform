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

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Set the metadata (attributes) for the GTS on the stack
 * 
 */
public class META extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private URL url = null;
  
  public META(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    //
    // Extract token
    //
    
    Object otoken = stack.pop();
    
    if (!(otoken instanceof String)) {
      throw new WarpScriptException(getName() + " expects a token on top of the stack.");
    }
    
    String token = (String) otoken;

    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();
    
    Object o = stack.pop();
    
    if (o instanceof GeoTimeSerie) {
      series.add((GeoTimeSerie) o);
    } else if (o instanceof List) {
      for (Object oo: (List<Object>) o) {
        if (oo instanceof GeoTimeSerie) {
          series.add((GeoTimeSerie) oo);
        } else {
          throw new WarpScriptException(getName() + " can only operate on Geo Time Series or a list thereof.");
        }
      }
    } else {
      throw new WarpScriptException(getName() + " can only operate on Geo Time Series or a list thereof");
    }
    
    //
    // Return immediately if 'series' is empty
    //
    
    if (0 == series.size()) {
      return stack;
    }
    
    //
    // Check that all GTS have a name and attributes
    //
    
    for (GeoTimeSerie gts: series) {
      if (null == gts.getName() || "".equals(gts.getName())) {
        throw new WarpScriptException(getName() + " can only set attributes of Geo Time Series which have a non empty name.");
      }
      if (null == gts.getMetadata().getAttributes()) {
        throw new WarpScriptException(getName() + " can only operate on Geo Time Series which have attributes.");
      }
    }
    
    //
    // Create the OutputStream
    //
    
    HttpURLConnection conn = null;

    try {

      if (null == url) {
        if (WarpConfig.getProperties().containsKey(Configuration.CONFIG_WARPSCRIPT_META_ENDPOINT)) {
          url = new URL(WarpConfig.getProperties().getProperty(Configuration.CONFIG_WARPSCRIPT_META_ENDPOINT));
        } else {
          throw new WarpScriptException(getName() + " configuration parameter '" + Configuration.CONFIG_WARPSCRIPT_META_ENDPOINT + "' not set.");
        }
      }

      /*
      if (null == this.proxy) {
        conn = (HttpURLConnection) this.url.openConnection();
      } else {
        conn = (HttpURLConnection) this.url.openConnection(this.proxy);
      }
      */
      conn = (HttpURLConnection) url.openConnection();
      
      conn.setDoOutput(true);
      conn.setDoInput(true);
      conn.setRequestMethod("POST");
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_META_TOKENX), token);
      conn.setRequestProperty("Content-Type", "application/gzip");
      conn.setChunkedStreamingMode(16384);
      conn.connect();
      
      OutputStream os = conn.getOutputStream();
      GZIPOutputStream out = new GZIPOutputStream(os);
      PrintWriter pw = new PrintWriter(out);
      
      StringBuilder sb = new StringBuilder();
      
      for (GeoTimeSerie gts: series) {
        sb.setLength(0);
        GTSHelper.metadataToString(sb, gts.getName(), gts.getLabels());
        Map<String,String> attributes = null != gts.getMetadata().getAttributes() ? gts.getMetadata().getAttributes() : new HashMap<String,String>();
        GTSHelper.labelsToString(sb, attributes);
        pw.println(sb.toString());
      }
      
      pw.close();
      
      //
      // Update was successful, delete all batchfiles
      //
      
      if (200 != conn.getResponseCode()) {
        throw new WarpScriptException(getName() + " failed to complete successfully (" + conn.getResponseMessage() + ")");
      }
      
      //is.close();
      conn.disconnect();
      conn = null;
    } catch (IOException ioe) { 
      throw new WarpScriptException(getName() + " failed.", ioe);
    } finally {
      if (null != conn) {
        conn.disconnect();
      }
    }

    return stack;
  }
}
