//
//   Copyright 2018-2020  SenX S.A.S.
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
package io.warp10.plugins.http;

import io.warp10.WarpConfig;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.warp.sdk.AbstractWarp10Plugin;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.DispatcherType;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

public class WarpScriptHandler extends AbstractHandler {

  private final HTTPWarp10Plugin plugin;
  private final Properties properties;
  private final StoreClient storeClient;
  private final DirectoryClient directoryClient;

  public WarpScriptHandler(HTTPWarp10Plugin plugin) {
    this.plugin = plugin;
    this.properties = WarpConfig.getProperties();
    this.storeClient = AbstractWarp10Plugin.getExposedStoreClient();
    this.directoryClient = AbstractWarp10Plugin.getExposedDirectoryClient();
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
    // Only handle REQUEST
    if (DispatcherType.REQUEST != baseRequest.getDispatcherType()) {
      baseRequest.setHandled(true);
      return;
    }

    String prefix = this.plugin.getPrefix(target);
    Macro macro = this.plugin.getMacro(prefix);

    if (null == macro) {
      return;
    }

    baseRequest.setHandled(true);

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(this.storeClient, this.directoryClient, this.properties);

    try {
      WarpConfig.setThreadProperty(WarpConfig.THREAD_PROPERTY_SESSION, UUID.randomUUID().toString());
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[HTTPWarp10Plugin " + request.getRequestURL() + "]");

      //
      // Push the details onto the stack
      //

      Map<String, Object> params = new HashMap<String, Object>();

      params.put("method", request.getMethod());
      params.put("target", target);
      params.put("pathinfo", target.substring(prefix.length()));

      Enumeration<String> hdrs = request.getHeaderNames();
      Map<String, List<String>> headers = new HashMap<String, List<String>>();
      while (hdrs.hasMoreElements()) {
        String hdr = hdrs.nextElement();
        Enumeration<String> hvalues = request.getHeaders(hdr);
        List<String> hval = new ArrayList<String>();
        while (hvalues.hasMoreElements()) {
          hval.add(hvalues.nextElement());
        }
        if (plugin.isLcHeaders()) {
          headers.put(hdr.toLowerCase(), hval);
        } else {
          headers.put(hdr, hval);
        }
      }
      params.put("headers", headers);

      // We have to get the input stream before getting the parameters, else we won't be able to read it!
      InputStream inputStream = null;

      // The input stream is handled very differently if the WarpScript is expecting a splitted stream or not.
      if (null == plugin.streamDelimiter(prefix)) {
        // In the case of x-www-form-urlencoded request, the parameters are sent in the payload.
        // We must not get the input stream if we want the request.getParameterMap() below to
        // automatically parse the payload and add the result to the parameter map.
        // Thus we get the input stream if the request is not x-www-form-urlencoded or if the configuration
        // explicitly states that the payload must not be parsed.
        if ((!MimeTypes.Type.FORM_ENCODED.is(request.getContentType()) || !plugin.isParsePayload(prefix))) {
          // Ready all the stream and store the payload.
          byte[] payload = IOUtils.toByteArray(request.getInputStream());
          if (0 < payload.length) {
            params.put("payload", payload);
          }
        }
      } else {
        // The input stream will be splitted and given to the macro later.
        if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {
          inputStream = new GZIPInputStream(request.getInputStream());
        } else {
          inputStream = request.getInputStream();
        }
      }

      // Get the parameters in the URL and payload if the parsing of x-www-form-urlencoded payload was allowed, see comment above.
      // request.getParameterMap() will not parse the payload if the input stream of the request has been retrieved.
      Map<String, List<String>> httpparams = new HashMap<String, List<String>>();
      Map<String, String[]> pmap = request.getParameterMap();
      for (Entry<String, String[]> param: pmap.entrySet()) {
        httpparams.put(param.getKey(), Arrays.asList(param.getValue()));
      }
      params.put("params", httpparams);

      try {
        if (null == plugin.streamDelimiter(prefix)) {
          // Not streaming, only push params and exec the macro.
          stack.push(params);
          stack.exec(macro);
        } else {
          Byte splitByte = plugin.streamDelimiter(prefix);
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

          int readCount;
          byte[] buffer = new byte[1024];

          // Read the stream by chunks until end of stream.
          while ((readCount = inputStream.read(buffer)) != -1) {
            int offset = 0;

            // Split the chunk.
            while (true) {
              int indexOfSplit = indexOf(buffer, splitByte, offset, readCount);
              if (-1 == indexOfSplit) {
                // No delimiter found, add to output stream and continue reading the input stream.
                outputStream.write(buffer, offset, readCount - offset);
                break;
              } else {
                // Delimiter found, add split to output stream, which may not be empty because of code above.
                outputStream.write(buffer, offset, indexOfSplit - offset);

                // Push params, payload and execute macro.
                stack.push(params);
                stack.push(outputStream.toByteArray());
                stack.exec(macro);

                // Empty output stream.
                outputStream.reset();
              }

              // Update offet, add 1 to also ignore delimiter.
              offset = indexOfSplit + 1;
            }
          }

          // End of stream, push null payload.
          stack.push(params);
          stack.push(null);
          stack.exec(macro);
        }

        Object top = stack.pop();

        if (top instanceof Map) {
          Map<String, Object> result = (Map<String, Object>) top;
          if (result.containsKey("status")) {
            response.setStatus(((Number) result.get("status")).intValue());
          }

          if (result.containsKey("headers")) {
            Map<String, Object> respheaders = (Map<String, Object>) result.get("headers");
            for (Entry<String, Object> hdr: respheaders.entrySet()) {
              if (hdr.getValue() instanceof List) {
                for (Object o: (List) hdr.getValue()) {
                  response.addHeader(hdr.getKey(), o.toString());
                }
              } else {
                response.setHeader(hdr.getKey(), hdr.getValue().toString());
              }
            }
          }

          if (result.containsKey("body")) {
            Object body = result.get("body");
            if (body instanceof byte[]) {
              OutputStream out = response.getOutputStream();
              out.write((byte[]) body);
            } else {
              PrintWriter writer = response.getWriter();
              writer.print(body.toString());
            }
          }
        } else {
          if (top instanceof byte[]) {
            response.setContentType("application/octet-stream");
            OutputStream out = response.getOutputStream();
            out.write((byte[]) top);
          } else {
            response.setContentType("text/plain");
            PrintWriter writer = response.getWriter();
            writer.println(String.valueOf(top));
          }
        }
      } catch (WarpScriptException wse) {
        throw new IOException(wse);
      }
    } finally {
      WarpConfig.clearThreadProperties();
      WarpScriptStackRegistry.unregister(stack);
    }
  }

  private static int indexOf(byte[] array, final byte target, int start, int end) {
    for (int i = start; i < end; i++) {
      if (target == array[i]) {
        return i;
      }
    }
    return -1;
  }
}
