//
//   Copyright 2021-2023  SenX S.A.S.
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

package io.warp10.script.ext.http;

import io.warp10.WarpConfig;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WebAccessController;
import io.warp10.warp.sdk.Capabilities;

import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Send an HTTP request to a url
 *
 * Capabilities:
 * - http.requests (maximum number of calls)
 * - http.size (maximum download size in number of bytes)
 * - http.chunksize (maximum chunk size in number of bytes)
 * - if the configuration parameter warpscript.http.capability exists,
 *   then its value is a capability that is checked to enable usage of this function
 *
 * Params:
 * url The URL to send the request to. Must begin with http:// or https://
 * method The optional http method. Default to GET
 * headers An optional header map
 * body An optional body. UTF-8 STRING or BYTES
 * chunk.size Chunk size
 * chunk.macro A macro that is executed whenever a chunk has been downloaded. It expects a MAP that contains chunk number (a LONG), status code (a LONG), status message (a STRING), headers (a MAP), and chunk content (a BYTES objects)
 * username Optional field. If both username and password field are present, basic authentication will be performed
 * password Optional field. If both username and password field are present, basic authentication will be performed
 *
 * Output:
 * RESPONSE A map that contains status code (a LONG), status message (a STRING), headers (a MAP) and full content of the response (a BYTES objects). The content is empty if chunk option is used
 *
 */
public class HTTP extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  //
  // Arguments
  //

  public static final String METHOD = "method";
  public static final String URL = "url";
  public static final String HEADERS = "headers";
  public static final String BODY = "body";
  public static final String CHUNK_SIZE = "chunk.size";
  public static final String CHUNK_MACRO = "chunk.macro";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String CONNECT_TIMEOUT = "timeout";

  //
  // Output
  //

  public static final String STATUS_CODE = "status.code";
  public static final String STATUS_MESSAGE = "status.message";
  public static final String RESPONSE_HEADERS = "headers";
  public static final String CONTENT = "content";
  public static final String CHUNK_NUMBER = "chunk.number";

  //
  // Control
  //

  private static final WebAccessController defaultWebAccessController;

  //
  // Limits
  //

  private static final long baseMaxRequests;
  private static final long baseMaxSize;
  private static final long baseMaxChunkSize;
  private static final int baseMaxTimeout;

  private static final String DEFAULT_HTTP_HOST_PATTERN = "!.*";

  //
  // Parameter extraction
  //

  static {
    String patternConf = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_HOST_PATTERNS, DEFAULT_HTTP_HOST_PATTERN);

    defaultWebAccessController = new WebAccessController(patternConf);

    // retrieve limits
    String confMaxRequests = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_REQUESTS);
    if (null == confMaxRequests) {
      baseMaxRequests = HttpWarpScriptExtension.DEFAULT_HTTP_REQUESTS;
    } else {
      baseMaxRequests = Long.parseLong(confMaxRequests);
    }

    String confMaxSize = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_SIZE);
    if (null == confMaxSize) {
      baseMaxSize = HttpWarpScriptExtension.DEFAULT_HTTP_MAXSIZE;
    } else {
      baseMaxSize = Long.parseLong(confMaxSize);
    }

    String confMaxChunkSize = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_CHUNK_SIZE);
    if (null == confMaxChunkSize) {
      baseMaxChunkSize = HttpWarpScriptExtension.DEFAULT_HTTP_CHUNK_SIZE;
    } else {
      baseMaxChunkSize = Long.parseLong(confMaxChunkSize);
    }

    String confMaxTimeout = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_TIMEOUT);
    if (null == confMaxTimeout) {
      baseMaxTimeout = HttpWarpScriptExtension.DEFAULT_HTTP_TIMEOUT;
    } else {
      long l = Long.parseLong(confMaxTimeout);
      if (l < 0) {
        throw new RuntimeException("Configuration key " + HttpWarpScriptExtension.WARPSCRIPT_HTTP_TIMEOUT + " must be positive.");
      }
      if (l == 0 || l > Integer.MAX_VALUE) {
        l = Integer.MAX_VALUE;
      }
      baseMaxTimeout = (int) l;
    }

  }

  public HTTP(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object o = stack.pop();
    if (!(o instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a MAP as input.");
    }

    Map params = (Map) o;

    //
    // Check authorization
    //
    String httpCap = Capabilities.get(stack, WarpScriptStack.CAPABILITY_HTTP);
    if (null == httpCap) {
      throw new WarpScriptException(getName() + " requires capability '" + WarpScriptStack.CAPABILITY_HTTP + "'.");
    }

    //
    // The http capability may contain the url filter. If not, use the default one.
    //
    WebAccessController webAccessController = defaultWebAccessController;
    if (!httpCap.isEmpty()) {
      webAccessController = new WebAccessController(httpCap);
    }

    //
    // Retrieve call number limit and download size limit
    //

    long maxrequests;
    if (null != Capabilities.get(stack, HttpWarpScriptExtension.ATTRIBUTE_HTTP_REQUESTS)) {
      maxrequests = Long.valueOf(Capabilities.get(stack, HttpWarpScriptExtension.ATTRIBUTE_HTTP_REQUESTS));
    } else {
      maxrequests = baseMaxRequests;
    }

    long maxsize;
    if (null != Capabilities.get(stack, HttpWarpScriptExtension.ATTRIBUTE_HTTP_SIZE)) {
      maxsize = Long.valueOf(Capabilities.get(stack, HttpWarpScriptExtension.ATTRIBUTE_HTTP_SIZE));
    } else {
      maxsize = baseMaxSize;
    }

    //
    // Retrieve arguments
    //

    Object body = params.get(BODY);
    Object oo = params.get(METHOD);
    String method;
    if (null == oo) {

      if (null == body) {
        method = "GET";

      } else {
        method = "POST";
      }

    } else if (oo instanceof String) {
      method = (String) oo;

    } else {
      throw new WarpScriptException(getName() + " expects " + METHOD + " to be a STRING.");
    }

    Map<Object, Object> headers;
    oo = params.get(HEADERS);
    if (null == oo) {
      headers = new HashMap<Object, Object>();
    } else if (oo instanceof Map) {
      headers = (Map) oo;
    } else {
     throw new WarpScriptException(getName() + " expects " + HEADERS + " to be a MAP.");
    }

    Long chunkSize = null;
    oo = params.get(CHUNK_SIZE);

    if (null != oo) {

      if (oo instanceof Long) {
        chunkSize = (Long) oo;

      } else {
        throw new WarpScriptException(getName() + " expects " + CHUNK_SIZE + " to be a LONG.");
      }

      if (0 >= chunkSize) {
        throw new WarpScriptException(getName() + " expects " + CHUNK_SIZE + " value to be greater than 0.");
      }

      long maxChunkSize;
      if (null != Capabilities.get(stack, HttpWarpScriptExtension.ATTRIBUTE_CHUNK_SIZE)) {
        maxChunkSize = Long.valueOf(Capabilities.get(stack, HttpWarpScriptExtension.ATTRIBUTE_CHUNK_SIZE));
      } else {
        maxChunkSize = baseMaxChunkSize;
      }
      if (chunkSize > maxChunkSize) {
        throw new WarpScriptException(getName() + " expects a chunk size in number of bytes that do not exceed " + maxChunkSize + ".");
      }
    }

    WarpScriptStack.Macro chunkMacro = null;
    o = params.get(CHUNK_MACRO);
    if (null != o) {
      if (!(o instanceof WarpScriptStack.Macro)) {
        throw new WarpScriptException(getName() + " expects a macro in the input parameters map as value of " + CHUNK_MACRO);
      }
      chunkMacro = (WarpScriptStack.Macro) o;
    }

    int timeout = baseMaxTimeout;  // Default timeout is the one hardcoded in Warp 10 configuration
    Object t = params.get(CONNECT_TIMEOUT);
    if (t != null) {
      // When user try to change it, it must be less than max(default timeout, capability timeout)
      if (!(t instanceof Long)) {
        throw new WarpScriptException(getName() + " expect a positive LONG for " + CONNECT_TIMEOUT + " parameter.");
      }
      Long tl = ((Long) t).longValue() / Constants.TIME_UNITS_PER_MS;
      int ti;
      if (tl > Integer.MAX_VALUE) {
        ti = Integer.MAX_VALUE;
      } else {
        ti = tl.intValue();
      }
      if (ti <= 0) {  // Timeout 0 or a negative value is allowed for "no timeout"
        ti = Integer.MAX_VALUE;
      }
      Long capTimeout = Capabilities.getLong(stack, HttpWarpScriptExtension.CAPABILITY_HTTP_TIMEOUT);
      if (capTimeout != null) {
        if (capTimeout < 0) {
          throw new WarpScriptException("Capability " + HttpWarpScriptExtension.CAPABILITY_HTTP_TIMEOUT + " must be a positive LONG");
        }
        if (capTimeout == 0 || capTimeout > Integer.MAX_VALUE) {
          timeout = Integer.MAX_VALUE;
        } else {
          timeout = Math.max(capTimeout.intValue(), timeout);
        }
      }
      if (ti > timeout) {
        throw new WarpScriptException(CONNECT_TIMEOUT + " is greater than maximum allowed timeout (" + timeout + "ms)");
      }

      // as timeout is an integer, Integer.MAX_VALUE is 24.8 days
      // when user specify 'timeout' MAXLONG, and is allowed to do so by configuration or capability, there should be no timeout at all.
      timeout = (ti == Integer.MAX_VALUE) ? 0 : ti;

    }

    //
    // Check URL
    //

    Object urlParam = params.get(URL);
    if (null == urlParam) {
      throw new WarpScriptException(getName() + " expects a url.");

    } else if (!(urlParam instanceof String)) {
      throw new WarpScriptException(getName() + " expects " + URL + " to be a STRING.");
    }

    URL url = null;
    try {
      url = new URL((String) urlParam);
    } catch (MalformedURLException mue) {
      throw new WarpScriptException(getName() + " encountered an invalid URL.", mue);
    }

    if (!"http".equals(url.getProtocol()) && !"https".equals(url.getProtocol())) {
      throw new WarpScriptException(getName() + " only supports http and https protocols.");
    }

    if (!webAccessController.checkURL(url)) {
      throw new WarpScriptException(getName() + " URL forbidden by configuration or capability.");
    }

    //
    // Check that we do not exceed the limits
    //

    // Get the current counters in the stack and initialize them if not present.
    AtomicLong urlCount;
    AtomicLong downloadSize;

    Object ufCount = stack.getAttribute(HttpWarpScriptExtension.ATTRIBUTE_HTTP_REQUESTS);
    Object ufSize = stack.getAttribute(HttpWarpScriptExtension.ATTRIBUTE_HTTP_SIZE);

    if (null == ufCount || null == ufSize) {
      urlCount = new AtomicLong();
      downloadSize = new AtomicLong();
      stack.setAttribute(HttpWarpScriptExtension.ATTRIBUTE_HTTP_REQUESTS, urlCount);
      stack.setAttribute(HttpWarpScriptExtension.ATTRIBUTE_HTTP_SIZE, downloadSize);
    } else {
      urlCount = (AtomicLong) ufCount;
      downloadSize = (AtomicLong) ufSize;
    }

    if (urlCount.addAndGet(1) > maxrequests) {
      throw new WarpScriptException(getName() + " is limited to " + maxrequests + " calls per script execution.");
    }

    Map<String, Object> res = new LinkedHashMap<String, Object>();
    HttpURLConnection conn = null;

    try {
      conn = (HttpURLConnection) url.openConnection();
      conn.setConnectTimeout(timeout);
      conn.setReadTimeout(timeout);
      //
      // Set headers
      //

      Object username = params.get(USERNAME);
      Object password = params.get(PASSWORD);

      if (null != username && null != password) {

        //
        // Compute basic auth
        //

        if (!(username instanceof String)) {
          throw new WarpScriptException(getName() + " expects a STRING username when doing basic authentication.");
        }

        if (!(password instanceof String)) {
          throw new WarpScriptException(getName() + " expects a STRING password when doing basic authentication.");
        }

        String userInfo = ((String) username) + ":" + ((String) password);
        String basicAuth = "Basic " + Base64.encodeBase64String(userInfo.getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", basicAuth);
      }

      for (Map.Entry<Object, Object> prop: headers.entrySet()) {
        conn.setRequestProperty(String.valueOf(prop.getKey()), String.valueOf(prop.getValue()));
      }

      conn.setDoInput(true);
      conn.setRequestMethod(method.toUpperCase());

      //
      // Set body
      //

      if (!"GET".equals(method) && !"TRACE".equals(method)) {

        byte[] bodyB = null;
        if (body instanceof String) {
          bodyB = ((String) body).getBytes(StandardCharsets.UTF_8);

        } else if (body instanceof byte[]) {
          bodyB = (byte[]) body;

        } else if (null != body) {
          throw new WarpScriptException(getName() + " expects the body of the request to be a STRING or BYTES object.");
        }

        if (null != bodyB) {
          conn.setDoOutput(bodyB.length > 0);
          if (bodyB.length > 0) {
            try (OutputStream os = conn.getOutputStream()) {
              os.write(bodyB);
            }
          }
        }

      } else if (null != body) {
        throw new WarpScriptException(getName() + " " + method + " cannot be used with a body.");
      }

      //
      // Form response
      //

      res.put(STATUS_CODE, conn.getResponseCode());
      Map<String, List<String>> hdrs = conn.getHeaderFields();

      // headers map is immutable
      res.put(RESPONSE_HEADERS, hdrs);

      // also put status_message in a separate field
      if (hdrs.containsKey(null)) {
        List<String> statusMsg = hdrs.get(null);
        if (statusMsg.size() > 0) {
          res.put(STATUS_MESSAGE, statusMsg.get(0));
        } else {
          res.put(STATUS_MESSAGE, "");
        }
      } else {
        res.put(STATUS_MESSAGE, "");
      }

      //
      // Read response
      //

      InputStream in = null;

      // When there is an error (response code is 404 for instance), body is in the error stream.
      try {
        in = conn.getInputStream();
      } catch (IOException ioe) {
        in = conn.getErrorStream();
      }

      if (null == chunkSize) {

        if (null != in) {
          byte[] buf = new byte[8192];
          ByteArrayOutputStream baos = new ByteArrayOutputStream();

          while (true) {
            int len = in.read(buf);
            if (len < 0) {
              break;
            }

            if (downloadSize.get() + baos.size() + len > maxsize) {
              throw new WarpScriptException(getName() + " would exceed maximum size of content which can be retrieved via this function (" + maxsize + " bytes) per script execution.");
            }

            baos.write(buf, 0, len);
          }

          downloadSize.addAndGet(baos.size());
          res.put(CONTENT, baos.toByteArray());

        } else {
          res.put(CONTENT, new byte[0]);
        }

      } else {

        //
        // Chunk processing
        //

        if (null != in) {

          int chunkNumber = 0;
          boolean eof = false;

          while (!eof) {
            chunkNumber++;

            byte[] buf = new byte[chunkSize.intValue()];
            int len = 0;
            while (len < chunkSize.intValue()) {
              int read = in.read(buf, len,chunkSize.intValue() - len);
              if (read <= 0) {
                eof = true;
                break;
              }
              len += read;
            }
            if (len <= 0) {
              break;
            }

            if (downloadSize.addAndGet(len) > maxsize) {
              throw new WarpScriptException(getName() + " would exceed maximum size of content which can be retrieved via this function (" + maxsize + " bytes)");
            }

            if (len == chunkSize) {
              res.put(CONTENT, buf);
            } else {
              byte[] buf2 = new byte[len];
              System.arraycopy(buf, 0, buf2, 0, buf2.length);
              res.put(CONTENT, buf2);
            }
            res.put(CHUNK_NUMBER, Long.valueOf(chunkNumber));

            Map<String, Object> chunkRes = Collections.unmodifiableMap(res);
            stack.push(chunkRes);
            if (null != chunkMacro) {
              stack.exec(chunkMacro);
            }
          }
        }

        //
        // Finalize chunk processing with a last execution of the chunk.macro
        // buffer is empty and chunk.number is set to -1 for this one
        //

        res.put(CHUNK_NUMBER, -1L);
        res.put(CONTENT, new byte[0]);
        Map<String, Object> chunkRes = Collections.unmodifiableMap(res);
        stack.push(chunkRes);
        if (null != chunkMacro) {
          stack.exec(chunkMacro);
        }

        //
        // End chunk processing
        //

        res.remove(CHUNK_NUMBER);
        res.remove(CONTENT);
      }

    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " encountered an error while making an HTTP " + method + " request to '" + url + "'", ioe);
    } finally {
      if (null != conn) {
        conn.disconnect();
      }
    }

    stack.push(res);

    return stack;
  }
}
