//
//   Copyright 2021  SenX S.A.S.
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
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WebAccessController;
import io.warp10.standalone.StandaloneWebCallService;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Send an HTTP request to a url
 *
 * To raise maximum number of calls and download size limit, use these capabilities:
 * http.requests
 * http.size
 *
 * Params:
 * url The URL to send the request to. Must begin with http:// or https://
 * method The optional http method. Default to GET
 * headers An optional header map
 * body An optional body. UTF-8 STRING or BYTES
 * headers.macro A optional macro that expects this input parameters map on the stack, and push back the headers. Convenient for custom authorization schemes
 * chunk.size Chunk size
 * chunk.macro A macro that is executed whenever a chunk has been downloaded. It expects a MAP that contains chunk number (a LONG), status code (a LONG), status message (a STRING), headers (a MAP), and chunk content (a BYTES objects)
 * username Optional field. If both username and password field are present and headers.macro is absent, basic authentication will be performed
 * password Optional field. If both username and password field are present and headers.macro is absent, basic authentication will be performed
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
  public static final String HEADERS_MACRO = "headers.macro";
  public static final String CHUNK_SIZE = "chunk.size";
  public static final String CHUNK_MACRO = "chunk.macro";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";

  //
  // Output
  //

  public static final String RESPONSE = "response";
  public static final String STATUS_CODE = "status.code";
  public static final String STATUS_MESSAGE = "status.message";
  public static final String RESPONSE_HEADERS = "headers";
  public static final String CONTENT = "content";
  public static final String CHUNK_NUMBER = "chunk.number";

  //
  // Control
  //

  private static final WebAccessController webAccessController;

  //
  // Authorization
  //

  private static final boolean auth;
  private static final String capName;

  //
  // Limits
  //

  private static final long baseMaxRequests;
  private static final long baseMaxSize;
  private static final long baseMaxChunkSize;

  //
  // Parameter extraction
  //

  static {
    String patternConf = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_HOST_PATTERNS);

    // If not defined, use already existing StandaloneWebCallService webAccessController which uses Configuration.WEBCALL_HOST_PATTERNS
    if (null == patternConf) {
      webAccessController = StandaloneWebCallService.getWebAccessController();
    } else {
      webAccessController = new WebAccessController(patternConf);
    }

    // retrieve authentication required
    auth = "true".equals(WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_AUTHENTICATION_REQUIRED));

    // retrieve capName
    capName = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_CAPABILITY);

    // retrieve limits
    Object confMaxRequests = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_REQUESTS);
    if (null == confMaxRequests) {
      baseMaxRequests = HttpWarpScriptExtension.DEFAULT_HTTP_REQUESTS;
    } else {
      baseMaxRequests = Long.parseLong((String) confMaxRequests);
    }

    Object confMaxSize = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_SIZE);
    if (null == confMaxSize) {
      baseMaxSize = HttpWarpScriptExtension.DEFAULT_HTTP_MAXSIZE;
    } else {
      baseMaxSize = Long.parseLong((String) confMaxSize);
    }

    Object confMaxChunkSize = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_CHUNK_SIZE);
    if (null == confMaxChunkSize) {
      baseMaxChunkSize = HttpWarpScriptExtension.DEFAULT_HTTP_CHUNK_SIZE;
    } else {
      baseMaxChunkSize = Long.parseLong((String) confMaxChunkSize);
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

    if (auth && !stack.isAuthenticated()) {
      throw new WarpScriptException(getName() + " requires the stack to be authenticated.");
    }

    if (null != capName && null == Capabilities.get(stack, capName)) {
      throw new WarpScriptException("Capability " + capName + " is required by function " + getName());
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

    String method = (String) params.get(METHOD);
    if (null == method) {
      method = "GET";
    }

    Map<Object, Object> headers = (Map) params.getOrDefault(HEADERS, new HashMap<>());
    Object body = params.get(BODY);

    o = params.get(HEADERS_MACRO);
    if (!(o instanceof WarpScriptStack.Macro)) {
      throw new WarpScriptException(getName() + " expects a macro in the input parameters map as value of " + HEADERS_MACRO);
    }
    WarpScriptStack.Macro headersMacro = (WarpScriptStack.Macro) o;

    Long chunkSize = (Long) params.getOrDefault(CHUNK_SIZE, -1L);
    long maxchunksize;
    if (null != Capabilities.get(stack, HttpWarpScriptExtension.ATTRIBUTE_CHUNK_SIZE)) {
      maxchunksize = Long.valueOf(Capabilities.get(stack, HttpWarpScriptExtension.ATTRIBUTE_CHUNK_SIZE));
    } else {
      maxchunksize = baseMaxChunkSize;
    }
    if (chunkSize > maxchunksize) {
      throw new WarpScriptException(getName() + " expects a chunk size in number of bytes that do not exceed " + chunkSize + ".");
    }

    WarpScriptStack.Macro chunkMacro = (WarpScriptStack.Macro) params.get(CHUNK_MACRO);

    //
    // Check URL
    //

    Object urlParam = params.get(URL);
    if (null == urlParam) {
      throw new WarpScriptException(getName() + " expects a url.");
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
      throw new WarpScriptException(getName() + " invalid host or scheme in URL.");
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

    Map<String, Object> res = new HashMap<>();
    HttpURLConnection conn = null;

    try {
      conn = (HttpURLConnection) url.openConnection();

      //
      // Set headers
      //

      if (null == headersMacro) {

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

      } else {

        stack.push(params);
        stack.exec(headersMacro);
        o = stack.pop();

        if (!(o instanceof Map)) {
          throw new WarpScriptException(getName() + " expects the " + HEADERS_MACRO + " to push a headers map onto the stack.");
        }

        headers = (Map) o;
      }

      for (Map.Entry<Object, Object> prop: headers.entrySet()) {
        conn.setRequestProperty(String.valueOf(prop.getKey()), String.valueOf(prop.getValue()));
      }

      conn.setDoInput(true);
      conn.setRequestMethod(method.toUpperCase());

      //
      // Set body
      //

      if ("GET" != method && "DELETE" != method && "TRACE" != method && "OPTIONS" != method && "HEAD" != method) {

        if (body instanceof String) {
          String bodyS = (String) body;
          conn.setDoOutput(bodyS.length() > 0);
          if (bodyS.length() > 0) {
            try (OutputStream os = conn.getOutputStream()) {
              os.write(bodyS.getBytes(StandardCharsets.UTF_8));
            }
          }

        } else if (body instanceof byte[]) {
          byte[] bodyB = (byte[]) body;
          conn.setDoOutput(bodyB.length > 0);
          if (bodyB.length > 0) {
            try (OutputStream os = conn.getOutputStream()) {
              os.write(bodyB);
            }
          }

        } else if (null != body) {
          throw new WarpScriptException(getName() + " expects the body of the request to be a STRING or BYTES object.");
        }
      }

      //
      // Form response
      //

      res.put(STATUS_CODE, conn.getResponseCode());
      Map<String, List<String>> hdrs = conn.getHeaderFields();

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
      // Make the headers map modifiable
      //

      hdrs = new LinkedHashMap<String, List<String>>(hdrs);
      hdrs.remove(null);

      res.put(RESPONSE_HEADERS, hdrs);

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

      if (chunkSize <= 0) {

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

        if (null != in) {

          int chunkNumber = 0;
          while (true) {
            chunkNumber++;

            Map<String, Object> chunkRes = new LinkedHashMap<>(res);
            byte[] buf = new byte[chunkSize.intValue()];
            int len = in.read(buf);
            if (len < 0) {
              break;
            }

            if (downloadSize.addAndGet(len) > maxsize) {
              throw new WarpScriptException(getName() + " would exceed maximum size of content which can be retrieved via this function (" + maxsize + " bytes)");
            }

            if (len == chunkSize) {
              chunkRes.put(CONTENT, buf);
            } else {
              byte[] buf2 = new byte[len];
              System.arraycopy(buf, 0, buf2, 0, buf2.length);
              chunkRes.put(CONTENT, buf2);
            }
            chunkRes.put(CHUNK_NUMBER, new Long(chunkNumber));
            stack.push(chunkRes);
            if (null != chunkMacro) {
              stack.exec(chunkMacro);
            }
          }

        } else {
          Map<String, Object> chunkRes = new LinkedHashMap<>(res);
          chunkRes.put(CHUNK_NUMBER, 1L);
          chunkRes.put(CONTENT, new byte[0]);
          stack.push(chunkRes);
          if (null != chunkMacro) {
            stack.exec(chunkMacro);
          }
        }

        res.put(CONTENT, new byte[0]);
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
