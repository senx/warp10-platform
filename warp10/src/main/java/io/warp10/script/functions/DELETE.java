//
//   Copyright 2018-2023  SenX S.A.S.
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
import io.warp10.WarpURLEncoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.AcceleratorConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Delete a set of GTS.
 * 
 * For safety reasons DELETE will first perform a dryrun call to the /delete endpoint to retrieve
 * the number of GTS which would be deleted by the call. If this number is above the expected number provided
 * by the user the actual delete will not be performed and instead an error will be raised. 
 */
public class DELETE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private URL url = null;
  
  public DELETE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    //
    // Extract expected number
    //
    boolean onlyDryRun = false;
    long expected = Long.MAX_VALUE;

    Object o = stack.pop();

    if (o instanceof Long) {
      expected = ((Long) o).longValue();
    } else if (null == o) {
      onlyDryRun = true;
    } else {
      throw new WarpScriptException(getName() + " expects a " + TYPEOF.TYPE_LONG + " count on top of the stack or " + TYPEOF.TYPE_NULL + " to indicate a dry run.");
    }
    
    //
    // Extract start / end
    //
    
    o = stack.pop();
    
    if (!(o instanceof Long) && !(o instanceof String) && (null != o)) {
      throw new WarpScriptException(getName() + " expects the end timestamp to be a Long, a String or NULL.");
    }
    
    Object end = o;
    
    o = stack.pop();
    
    if (!(o instanceof Long) && !(o instanceof String) && (null != o)) {
      throw new WarpScriptException(getName() + " expects the start timestamp to be a Long, a String or NULL.");
    }
    
    Object start = o;

    if ((null == start && null != end) || (null != start && null == end)) {
      throw new WarpScriptException(getName() + " expects both start and end timestamps MUST be NULL if one of them is.");
    }
    
    //
    // Extract selector
    //
    
    o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a Geo Time Series selector below the time parameters.");
    }
    
    String selector = o.toString();
    
    //
    // Extract token
    //
    
    o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a token below the selector.");
    }
    
    String token = (String) o;

    //
    // Issue a dryrun call to DELETE
    //
    
    HttpURLConnection conn = null;

    try {
      if (null == url) {
        String url_property = WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_DELETE_ENDPOINT);
        if (null != url_property) {
          try {
            url = new URL(url_property);
          } catch (MalformedURLException mue) {
            throw new WarpScriptException(getName() + " configuration parameter '" + Configuration.CONFIG_WARPSCRIPT_DELETE_ENDPOINT + "' does not define a valid URL.");
          }
        } else {
          throw new WarpScriptException(getName() + " configuration parameter '" + Configuration.CONFIG_WARPSCRIPT_DELETE_ENDPOINT + "' not set.");
        }
      }

      StringBuilder qsurl = new StringBuilder(url.toString());
      
      if (null == url.getQuery()) {
        qsurl.append("?");
      } else {
        qsurl.append("&");
      }

      if (null != start && null != end) {
        qsurl.append(Constants.HTTP_PARAM_END);
        qsurl.append("=");
        qsurl.append(end);
        qsurl.append("&");
        qsurl.append(Constants.HTTP_PARAM_START);
        qsurl.append("=");
        qsurl.append(start);
      } else {
        qsurl.append(Constants.HTTP_PARAM_DELETEALL);
        qsurl.append("=");
        qsurl.append("true");
      }
      
      qsurl.append("&");
      qsurl.append(Constants.HTTP_PARAM_SELECTOR);
      qsurl.append("=");
      qsurl.append(WarpURLEncoder.encode(selector, StandardCharsets.UTF_8));

      if (null != stack.getAttribute(AcceleratorConfig.ATTR_NOCACHE)) {
        boolean nocache = Boolean.TRUE.equals(stack.getAttribute(AcceleratorConfig.ATTR_NOCACHE));
        qsurl.append("&");
        if (nocache) {
          qsurl.append(AcceleratorConfig.NOCACHE);
        } else {
          qsurl.append(AcceleratorConfig.CACHE);
        }
      }

      if (null != stack.getAttribute(AcceleratorConfig.ATTR_NOPERSIST)) {
        boolean nopersist = Boolean.TRUE.equals(stack.getAttribute(AcceleratorConfig.ATTR_NOPERSIST));
        qsurl.append("&");
        if (nopersist) {
          qsurl.append(AcceleratorConfig.NOPERSIST);
        } else {
          qsurl.append(AcceleratorConfig.PERSIST);
        }
      }

      //
      // Issue the dryrun request
      //
      
      URL requrl = new URL(qsurl.toString() + "&" + Constants.HTTP_PARAM_DRYRUN + "=true");

      conn = (HttpURLConnection) requrl.openConnection();
      
      conn.setDoOutput(false);
      conn.setDoInput(true);
      conn.setRequestMethod("GET");
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DELETE_TOKENX), token);
      conn.connect();
            
      if (200 != conn.getResponseCode()) {
        throw new WarpScriptException(getName() + " failed to complete dryrun request successfully (" + conn.getResponseMessage() + ")");
      }
      
      long actualCount = 0;

      try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        if (onlyDryRun) {
          long gtsLimit = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_GTS_LIMIT);

          AtomicLong gtscount = (AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_GTS_COUNT);

          List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

          String line;
          while (null != (line = br.readLine())) {
            if (gtscount.incrementAndGet() > gtsLimit) {
              throw new WarpScriptException(getName() + " exceeded limit of " + gtsLimit + " Geo Time Series, current count is " + gtscount.get() + ". Consider raising the limit or using capabilities.");
            }

            Metadata meta = MetadataUtils.parseMetadata(line);
            if (null == meta) {
              throw new WarpScriptException(getName() + " got invalid Metadata from the delete endpoint: " + line);
            }
            GeoTimeSerie gts = new GeoTimeSerie();
            // Use safeSetMetadata because the metadata have been created by parseMetadata.
            gts.safeSetMetadata(meta);
            series.add(gts);
          }

          stack.push(series);

          // Stop here for the dry run.
          return stack;
        } else {
          while (null != br.readLine()) {
            actualCount++;

            // Do an early check for the expected count
            if (expected < actualCount) {
              throw new WarpScriptException(getName() + " expected at most " + expected + " Geo Time Series to be deleted but " + actualCount + " would have been deleted instead.");
            }
          }
        }
      }
      conn.disconnect();
      conn = null;
      
      //
      // Do nothing if no GTS are to be removed
      //
      
      if (0 == actualCount) {
        stack.push(0);
        return stack;
      }
      
      //
      // Now issue the actual call, hoping the deleted count is identical to the one we expected...
      //
      
      requrl = new URL(qsurl.toString());
      
      conn = (HttpURLConnection) requrl.openConnection();
      
      conn.setDoOutput(false);
      conn.setDoInput(true);
      conn.setRequestMethod("GET");
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DELETE_TOKENX), token);
      conn.connect();
            
      if (200 != conn.getResponseCode()) {
        throw new WarpScriptException(getName() + " failed to complete actual request successfully (" + conn.getResponseMessage() + ")");
      }

      try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        actualCount = 0;

        while (null != br.readLine()) {
          actualCount++;
        }
      }

      conn.disconnect();
      conn = null;

      stack.push(actualCount);
      
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
