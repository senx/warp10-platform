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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WebAccessController;
import io.warp10.script.formatted.FormattedWarpScriptFunction;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Send an HTTP request to an url
 *
 * To raise maximum number of calls and download size limit, use these capabilities:
 * .cap:http.requests
 * .cap:http.size
 */
public class HTTP extends FormattedWarpScriptFunction {

  //
  // Arguments
  //

  public static final String METHOD = "method";
  public static final String URL = "url";
  public static final String HEADERS = "headers";
  public static final String BODY = "body";
  public static final String AUTH_INFO = "auth info";
  public static final String AUTH_MACRO = "auth macro";
  public static final String CHUNK_SIZE = "chunk size";
  public static final String CHUNK_MACRO = "chunk macro";

  private final Arguments args;
  protected Arguments getArguments() {
    return args;
  }

  //
  // Output
  //

  public static final String RESPONSE = "response";
  public static final String STATUS_CODE = "status code";
  public static final String STATUS_MESSAGE = "status message";
  public static final String RESPONSE_HEADERS = "headers";
  public static final String CONTENT = "content";
  public static final String CHUNK_NUMBER = "chunk number";

  private final Arguments output;
  protected Arguments getOutput() {
    return output;
  }

  //
  // Control
  //

  private final WebAccessController webAccessController;

  //
  // Authorization
  //

  private final boolean auth;
  private final String capName;

  public HTTP(String name) {
    super(name);

    String patternConf = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_HOST_PATTERNS);

    // If not defined, use already existing StandaloneWebCallService webAccessController which uses Configuration.WEBCALL_HOST_PATTERNS
    if (null == patternConf) {
      webAccessController = StandaloneWebCallService.getWebAccessController();
    } else {
      webAccessController = new WebAccessController(patternConf);
    }

    getDocstring().append("Apply an HTTP method over an url and fetch response.");

    args = new ArgumentsBuilder()
      .addArgument(String.class, METHOD, "The http method.")
      .addArgument(String.class, URL, "The URL to send the request to. Must begin with http:// or https://.")
      .addOptionalArgument(Map.class, HEADERS, "An optional header.", new HashMap<>())
      .addOptionalArgument(Object.class, BODY, "An optional body. STRING or BYTES.", "")
      .addOptionalArgument(List.class, AUTH_INFO, "Authentication arguments. For example for basic authentication, provide [username, password].", null)
      .addOptionalArgument(WarpScriptStack.Macro.class, AUTH_MACRO, "A macro that expects " + AUTH_INFO + " on the stack, and returns a map to be appended with the headers. Default to basic authentication.", null)
      .addOptionalArgument(Long.class, CHUNK_SIZE, "Chunk size", -1L)
      .addOptionalArgument(WarpScriptStack.Macro.class, CHUNK_MACRO, "A macro that is executed whenever a chunk has been downloaded. It expects a MAP that contains chunk number (a LONG), status code (a LONG), status message (a STRING), headers (a MAP), and chunk content (a BYTES objects).", new WarpScriptStack.Macro())
      .build();

    output = new ArgumentsBuilder()
      .addArgument(Map.class, RESPONSE, "A map that contains status code (a LONG), status message (a STRING), headers (a MAP) and full content of the response (a BYTES objects). The content is empty if chunk option is used.")
      .build();

    // retrieve authentication required
    auth = "true".equals(WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_AUTHENTICATION_REQUIRED));

    // retrieve capName
    String capNameSuffix = WarpConfig.getProperty(HttpWarpScriptExtension.WARPSCRIPT_HTTP_CAPABILITY);
    if (null != capNameSuffix) {
      capName = WarpScriptStack.CAPABILITIES_PREFIX + capNameSuffix;
    } else {
      capName = null;
    }
  }

  @Override
  public WarpScriptStack apply(Map<String, Object> formattedArgs, WarpScriptStack stack) throws WarpScriptException {

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
    if (null != Capabilities.get(stack, WarpScriptStack.CAPABILITIES_PREFIX + HttpWarpScriptExtension.ATTRIBUTE_HTTP_COUNT)) {
      maxrequests = Long.valueOf(Capabilities.get(stack, WarpScriptStack.CAPABILITIES_PREFIX + HttpWarpScriptExtension.ATTRIBUTE_HTTP_COUNT));
    } else {
      maxrequests = HttpWarpScriptExtension.DEFAULT_HTTP_LIMIT;
    }

    long maxsize;
    if (null != Capabilities.get(stack, WarpScriptStack.CAPABILITIES_PREFIX + HttpWarpScriptExtension.ATTRIBUTE_HTTP_SIZE)) {
      maxsize = Long.valueOf(Capabilities.get(stack, WarpScriptStack.CAPABILITIES_PREFIX + HttpWarpScriptExtension.ATTRIBUTE_HTTP_SIZE));
    } else {
      maxsize = HttpWarpScriptExtension.DEFAULT_HTTP_MAXSIZE;
    }

    //
    // Retrieve arguments
    //

    String method = (String) formattedArgs.get(METHOD);
    Map<Object, Object> headers = (Map) formattedArgs.get(HEADERS);
    Object body = formattedArgs.get(BODY);
    List authInfo = (List) formattedArgs.get(AUTH_INFO);
    WarpScriptStack.Macro authMacro = (WarpScriptStack.Macro) formattedArgs.get(AUTH_MACRO);
    Long chunkSize = (Long) formattedArgs.get(CHUNK_SIZE);
    WarpScriptStack.Macro chunkMacro = (WarpScriptStack.Macro) formattedArgs.get(CHUNK_MACRO);

    //
    // Check URL
    //

    URL url = null;
    try {
      url = new URL((String) formattedArgs.get(URL));
    } catch (MalformedURLException mue) {
      throw new WarpScriptException(getName() + " encountered an invalid URL.", mue);
    }

    if (!webAccessController.checkURL(url)) {
      throw new WarpScriptException(getName() + " invalid host or scheme in URL.");
    }

    if (!"http".equals(url.getProtocol()) && !"https".equals(url.getProtocol())) {
      throw new WarpScriptException(getName() + " only supports http and https protocols.");
    }

    //
    // Check that we do not exceed the limits
    //

    // Get the current counters in the stack and initialize them if not present.
    AtomicLong urlCount;
    AtomicLong downloadSize;

    Object ufCount = stack.getAttribute(HttpWarpScriptExtension.ATTRIBUTE_HTTP_COUNT);
    Object ufSize = stack.getAttribute(HttpWarpScriptExtension.ATTRIBUTE_HTTP_SIZE);

    if (null == ufCount || null == ufSize) {
      urlCount = new AtomicLong();
      downloadSize = new AtomicLong();
      stack.setAttribute(HttpWarpScriptExtension.ATTRIBUTE_HTTP_COUNT, urlCount);
      stack.setAttribute(HttpWarpScriptExtension.ATTRIBUTE_HTTP_SIZE, downloadSize);
    } else {
      urlCount = (AtomicLong) ufCount;
      downloadSize = (AtomicLong) ufSize;
    }

    if (urlCount.addAndGet(1) > maxrequests) {
      throw new WarpScriptException(getName() + " is limited to " + maxrequests + " calls.");
    }

    Map<String, Object> res = new HashMap<>();
    HttpURLConnection conn = null;

    try {
      conn = (HttpURLConnection) url.openConnection();

      //
      // Encode userinfo and set headers
      //

      if (null != authInfo) {

        Map additionalHeaders;
        if (null != authMacro) {
          stack.push(authInfo);
          stack.exec(authMacro);
          additionalHeaders = (Map) stack.pop();

        } else {
          // doing basic auth
          if (authInfo.size() != 2) {
            throw new WarpScriptException(getName() + " expects a list with two items, username and password, in argument " + authInfo + ".");
          }

          if (!(authInfo.get(0) instanceof String)) {
            throw new WarpScriptException(getName() + " expects a STRING username when using basic authentication.");
          }

          if (!(authInfo.get(1) instanceof String)) {
            throw new WarpScriptException(getName() + " expects a STRING password when using basic authentication.");
          }

          String userInfo = authInfo.get(0) + ":" + authInfo.get(1);
          String basicAuth = "Basic " + Base64.encodeBase64String(userInfo.getBytes(StandardCharsets.UTF_8));
          additionalHeaders =  new HashMap<Object, Object>();
          additionalHeaders.put("Authorization", basicAuth);
        }

        headers.putAll(additionalHeaders);
      }

      for (Map.Entry<Object, Object> prop: headers.entrySet()) {
        conn.setRequestProperty(String.valueOf(prop.getKey()), String.valueOf(prop.getValue()));
      }

      conn.setDoInput(true);
      conn.setRequestMethod(method.toUpperCase());

      //
      // Set body
      //

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

      } else {
        throw new WarpScriptException(getName() + " expects the body of the request to be a STRING or BYTES object.");
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

      hdrs = new HashMap<String, List<String>>(hdrs);
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
        byte[] buf = new byte[8192];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        while (true) {
          int len = in.read(buf);
          if (len < 0) {
            break;
          }

          if (downloadSize.get() + baos.size() + len > maxsize) {
            throw new WarpScriptException(getName() + " would exceed maximum size of content which can be retrieved via this function (" + maxsize + " bytes)");
          }

          baos.write(buf, 0, len);
        }

        downloadSize.addAndGet(baos.size());
        res.put(CONTENT, baos.toByteArray());

      } else {
        byte[] buf = new byte[chunkSize.intValue()];
        Map<String, Object> chunkRes = new HashMap<>(res);

        int chunkNumber = 0;
        while (true) {
          chunkNumber++;

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
            for (int i = 0; i < buf2.length; i++) {
              buf2[i] = buf[i];
            }
            chunkRes.put(CONTENT, buf2);
          }
          chunkRes.put(CHUNK_NUMBER, new Long(chunkNumber));
          stack.push(chunkRes);
          stack.exec(chunkMacro);
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
