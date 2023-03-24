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

package io.warp10.continuum.egress;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.ThrowableUtils;
import io.warp10.WarpConfig;
import io.warp10.continuum.BootstrapManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.LogUtil;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.KeyStore;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Signal;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.script.WarpScriptStopException;
import io.warp10.script.ext.stackps.StackPSWarpScriptExtension;
import io.warp10.script.functions.DURATION;
import io.warp10.script.functions.RUNNERNONCE;
import io.warp10.script.functions.TIMEBOX;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.Capabilities;

public class EgressExecHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EgressExecHandler.class);
  private static final Logger EVENTLOG = LoggerFactory.getLogger("warpscript.events");

  private static StoreClient exposedStoreClient = null;
  private static DirectoryClient exposedDirectoryClient = null;

  private final KeyStore keyStore;
  private final StoreClient storeClient;
  private final DirectoryClient directoryClient;

  private final BootstrapManager bootstrapManager;

  // Our version of TIMEBOX will signal the stack with a KILL signal to ensure its
  // execution is definitely aborted. It will also not enforce limits since limits are
  // handled here.
  private static final TIMEBOX TIMEBOX = new TIMEBOX(WarpScriptLib.TIMEBOX, Signal.KILL, false, true);

  private static final RUNNERNONCE RUNNERNONCE = new RUNNERNONCE(WarpScriptLib.RUNNERNONCE);

  private static final long RUNNER_NONCE_VALIDITY = Long.parseLong(WarpConfig.getProperty(Configuration.EGRESS_RUNNER_NONCE_VALIDITY, Long.toString(1000L)));
  private static final long MAXTIME;

  static {
    MAXTIME = Long.parseLong(WarpConfig.getProperty(Configuration.EGRESS_MAXTIME, "0")) * Constants.TIME_UNITS_PER_MS;
    if (MAXTIME < 0) {
      throw new RuntimeException("Invalid negative value for " + Configuration.EGRESS_MAXTIME + ".");
    }
  }

  public EgressExecHandler(KeyStore keyStore, Properties properties, DirectoryClient directoryClient, StoreClient storeClient) {

    this.keyStore = keyStore;
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;

    //
    // Check if we have a 'bootstrap' property
    //

    if (properties.containsKey(Configuration.CONFIG_WARPSCRIPT_BOOTSTRAP_PATH)) {

      final String path = properties.getProperty(Configuration.CONFIG_WARPSCRIPT_BOOTSTRAP_PATH);

      long period = properties.containsKey(Configuration.CONFIG_WARPSCRIPT_BOOTSTRAP_PERIOD) ?  Long.parseLong(properties.getProperty(Configuration.CONFIG_WARPSCRIPT_BOOTSTRAP_PERIOD)) : 0L;
      this.bootstrapManager = new BootstrapManager(path, period);
    } else {
      this.bootstrapManager = new BootstrapManager();
    }

    if ("true".equals(properties.getProperty(Configuration.EGRESS_CLIENTS_EXPOSE))) {
      exposedStoreClient = storeClient;
      exposedDirectoryClient = directoryClient;
    }
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {

    if (target.startsWith(Constants.API_ENDPOINT_EXEC)) {
      baseRequest.setHandled(true);
    } else {
      return;
    }

    int errorCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

    //
    // CORS header
    //

    resp.setHeader("Access-Control-Allow-Origin", "*");

    resp.setHeader(Constants.HTTP_HEADER_TIMEUNIT, Long.toString(Constants.TIME_UNITS_PER_S));

    //
    // Making the Elapsed header available in cross-domain context
    //

    resp.addHeader("Access-Control-Expose-Headers", Constants.getHeader(Configuration.HTTP_HEADER_ELAPSEDX)
        + "," + Constants.getHeader(Configuration.HTTP_HEADER_OPSX)
        + "," + Constants.getHeader(Configuration.HTTP_HEADER_FETCHEDX)
        + "," + Constants.HTTP_HEADER_TIMEUNIT);

    //
    // Generate UUID for this script execution
    //

    UUID uuid = UUID.randomUUID();

    //
    // FIXME(hbs): Make sure we have at least one valid token
    //

    //
    // Create the stack to use
    //

    WarpScriptStack stack = new MemoryWarpScriptStack(this.storeClient, this.directoryClient);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[EgressExecHandler " + Thread.currentThread().getName() + "]");

    if (null != req.getHeader(StackPSWarpScriptExtension.HEADER_SESSION)) {
      stack.setAttribute(StackPSWarpScriptExtension.ATTRIBUTE_SESSION, req.getHeader(StackPSWarpScriptExtension.HEADER_SESSION));
    }

    Throwable t = null;

    StringBuilder scriptSB = new StringBuilder();
    List<Long> times = new ArrayList<Long>();

    int lineno = 0;

    long now = System.nanoTime();

    try {
      WarpConfig.setThreadProperty(WarpConfig.THREAD_PROPERTY_SESSION, UUID.randomUUID().toString());

      //
      // Replace the context with the bootstrap one
      //

      StackContext context = this.bootstrapManager.getBootstrapContext();

      if (null != context) {
        stack.push(context);
        stack.restore();
      }

      //
      // Expose the headers if instructed to do so
      //

      String expose = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_EXPOSE_HEADERS));

      if (null != expose) {
        Map<String,Object> headers = new HashMap<String,Object>();
        Enumeration<String> names = req.getHeaderNames();
        while(names.hasMoreElements()) {
          String name = names.nextElement();
          Enumeration<String> values = req.getHeaders(name);
          List<String> elts = new ArrayList<String>();
          while(values.hasMoreElements()) {
            String value = values.nextElement();
            elts.add(value);
          }
          headers.put(name, elts);
        }
        stack.store(expose, headers);
      }

      //
      // Execute the bootstrap code. This is done before adding capabilities.
      //

      stack.exec(WarpScriptLib.BOOTSTRAP);

      //
      // Add capabilities specified in the capabilities header
      //

      String capabilities = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_CAPABILITIES));

      if (null != capabilities) {
        String[] tokens = capabilities.split(",");
        for (String token: tokens) {
          Capabilities.add(stack, token.trim());
        }
      }

      //
      // Extract parameters from the path info and set their value as symbols
      //
      // TODO(hbs): we should us an alternate stack for those executions and limit the number of ops and timebox the exec

      String pathInfo = req.getPathInfo();

      if (pathInfo != null && pathInfo.length() > Constants.API_ENDPOINT_EXEC.length() + 1) {
        pathInfo = pathInfo.substring(Constants.API_ENDPOINT_EXEC.length() + 1);
        String[] tokens = pathInfo.split("/");

        // Store the depth of the stack in case the Bootstrap leave something on the stack.
        int initialStackDepth = stack.depth();

        for (String token: tokens) {
          String[] subtokens = token.split("=", 2);

          if (2 != subtokens.length) {
            errorCode = HttpServletResponse.SC_BAD_REQUEST;
            throw new MalformedURLException("Each symbol definition must have at least one equal sign.");
          }

          // Legit uses of URLDecoder.decode, do not replace by WarpURLDecoder
          // as the encoding is performed by the browser
          subtokens[0] = URLDecoder.decode(subtokens[0], StandardCharsets.UTF_8.name());
          subtokens[1] = URLDecoder.decode(subtokens[1], StandardCharsets.UTF_8.name());

          //
          // Execute values[0] so we can interpret it prior to storing it in the symbol table
          //

          scriptSB.append("// @param ").append(subtokens[0]).append("=").append(subtokens[1]).append("\n");

          stack.exec(subtokens[1]);

          if (1 != (stack.depth() - initialStackDepth)) {
            throw new WarpScriptException("Each symbol definition must output one element.");
          }

          stack.store(subtokens[0], stack.pop());
        }
      }

      //
      // Now read lines of the body, interpreting them
      //

      //
      // Determine if content if gzipped
      //

      boolean gzipped = false;

      if ("application/gzip".equals(req.getHeader("Content-Type"))) {
        gzipped = true;
      }

      BufferedReader br = null;

      if (gzipped) {
        GZIPInputStream is = new GZIPInputStream(req.getInputStream());
        br = new BufferedReader(new InputStreamReader(is));
      } else {
        br = req.getReader();
      }

      List<Long> elapsed = (List<Long>) stack.getAttribute(WarpScriptStack.ATTRIBUTE_ELAPSED);

      elapsed.add(TimeSource.getNanoTime());

      boolean terminate = false;

      //
      // Timeboxing of the /exec requests is done in the following manner:
      //
      // If the configuration 'egress.maxtime' is set, the execution is
      // bounded by the provided value (in ms).
      //
      // If a token with the capability 'timebox.maxtime' is
      // passed in the X-Warp10-Capabilities header and its value is above that specified in
      // 'egress.maxtime', the value of the capability will be used as the new limit of the custom execution times.
      //
      // The header X-Warp10-Timebox is checked, if present, the value is interpreted as a
      // limit in ms or as an ISO8601 period. This value is the requested maximum execution time
      // of the /exec request. It will be capped to either egress.maxtime or the value provided in the
      // token (see above).
      //
      // If the header X-Warp10-Runner-Nonce is set with a value which is within egress.runner.nonce.validity
      // then the time boxing is waived.
      //

      String timebox = req.getHeader(Constants.HTTP_HEADER_TIMEBOX);

      // Value set in 'egress.maxtime'
      long maxtime = MAXTIME;

      //
      // Check the capability
      //

      long maxtimeCapability = MAXTIME;

      if (maxtime > 0 && null != Capabilities.get(stack, WarpScriptStack.CAPABILITY_TIMEBOX_MAXTIME)) {
        String val = Capabilities.get(stack, WarpScriptStack.CAPABILITY_TIMEBOX_MAXTIME).trim();

        if (val.startsWith("P")) {
          maxtimeCapability = DURATION.parseDuration(new Instant(), val, false, false);
        } else {
          try {
            maxtimeCapability = Long.valueOf(Capabilities.get(stack, WarpScriptStack.CAPABILITY_TIMEBOX_MAXTIME)) * Constants.TIME_UNITS_PER_MS;
          } catch (NumberFormatException nfe) {
          }
        }
        // make sure value is positive
        maxtimeCapability = Math.max(0, maxtimeCapability);

        if (maxtimeCapability > 0) {
          maxtime = Math.max(maxtime, maxtimeCapability);
        } else {
          maxtime = 0;
        }
      }

      long maxtimeHeader = 0;

      if (null != timebox) {
        if (timebox.startsWith("P")) {
          maxtimeHeader = DURATION.parseDuration(new Instant(), timebox, false, false);
        } else {
          try {
            maxtimeHeader = Long.parseLong(timebox) * Constants.TIME_UNITS_PER_MS;
          } catch (NumberFormatException nfe) {
          }
        }

        maxtimeHeader = Math.max(0, maxtimeHeader);
      }

      if (maxtimeHeader > 0) {
        if (maxtime > 0) {
          maxtime = Math.min(maxtime, maxtimeHeader);
        } else {
          // No bound was set, use the limit provided in the header
          maxtime = maxtimeHeader;
        }
      }

      if (null != req.getHeader(Constants.HTTP_HEADER_RUNNER_NONCE)) {
        try {
          Long nonce = RUNNERNONCE.getNonce(req.getHeader(Constants.HTTP_HEADER_RUNNER_NONCE));

          if (null != nonce) {
            long delta = TimeSource.getTime() - nonce;

            // Waive the time boxing if the nonce is still valid
            if ((delta / Constants.TIME_UNITS_PER_MS) <= RUNNER_NONCE_VALIDITY) {
              maxtime = 0;
            } else {
              LOG.warn("Runner nonce has expired.");
            }
          }
        } catch (Exception e) {
        }
      }

      boolean forcedMacro = maxtime > 0;

      if (forcedMacro) {
        stack.macroOpen();
      }

      if (null != req.getHeader(Constants.HTTP_HEADER_LINES)) {
        stack.setAttribute(WarpScriptStack.ATTRIBUTE_LINENO, true);
      }

      while(!terminate) {
        String line = br.readLine();

        if (null == line) {
          break;
        }

        lineno++;

        // Store line for logging purposes, BEFORE execution is attempted, so we know what line may have caused an exception
        scriptSB.append(line).append("\n");

        long nano = System.nanoTime();

        try {
          if (Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_LINENO)) && !((MemoryWarpScriptStack) stack).isInMultiline()) {
            // We call 'exec' so statements are correctly put in macros if we are currently building one
            stack.exec("'[Line #" + Long.toString(lineno) + "]'");
            stack.exec(WarpScriptLib.SECTION);
          }
          stack.exec(line, lineno);
        } catch (WarpScriptStopException ese) {
          // Do nothing, this is simply an early termination which should not generate errors
          terminate = true;
        }

        long end = System.nanoTime();

        // Record elapsed time
        if (Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_TIMINGS))) {
          elapsed.add(end - now);
        }

        times.add(end - nano);
      }

      if (forcedMacro) {
        stack.macroClose();
      }

      //
      // Make sure stack is balanced
      //

      stack.checkBalanced();

      if (maxtime > 0) {
        stack.push(maxtime);
        TIMEBOX.apply(stack);
      }

      // Handle possible signals to determine if termination is normal or not
      stack.handleSignal();

      //
      // Check the user defined headers and set them.
      //

      if (null != stack.getAttribute(WarpScriptStack.ATTRIBUTE_HEADERS)) {
        Map<String,String> headers = (Map<String,String>) stack.getAttribute(WarpScriptStack.ATTRIBUTE_HEADERS);
        if (!Constants.hasReservedHeader(headers)) {
          StringBuilder sb = new StringBuilder();
          for (Entry<String,String> header: headers.entrySet()) {
            if (sb.length() > 0) {
              sb.append(",");
            }
            sb.append(header.getKey());
            resp.setHeader(header.getKey(), header.getValue());
          }
          resp.addHeader("Access-Control-Expose-Headers", sb.toString());
        }
      }

      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_ELAPSEDX), Long.toString(System.nanoTime() - now));
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_OPSX), stack.getAttribute(WarpScriptStack.ATTRIBUTE_OPS).toString());
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_FETCHEDX), stack.getAttribute(WarpScriptStack.ATTRIBUTE_FETCH_COUNT).toString());

      //resp.setContentType("application/json");
      //resp.setCharacterEncoding(StandardCharsets.UTF_8.name());

      //
      // Output the exported symbols in a map
      //

      Object exported = stack.getAttribute(WarpScriptStack.ATTRIBUTE_EXPORTED_SYMBOLS);

      if (exported instanceof Set && !((Set) exported).isEmpty()) {
        Map<String,Object> exports = new HashMap<String,Object>();
        Map<String,Object> symtable = stack.getSymbolTable();
        for (Object symbol: (Set) exported) {
          if (null == symbol) {
            exports.putAll(symtable);
            break;
          }
          exports.put(symbol.toString(), symtable.get(symbol.toString()));
        }
        stack.push(exports);
      }

      StackUtils.toJSON(resp.getWriter(), stack);
    } catch (Throwable e) {
      t = e;

      int debugDepth = (int) stack.getAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH);

      //
      // Check the user defined headers and set them.
      //

      if (null != stack.getAttribute(WarpScriptStack.ATTRIBUTE_HEADERS)) {
        Map<String,String> headers = (Map<String,String>) stack.getAttribute(WarpScriptStack.ATTRIBUTE_HEADERS);
        if (!Constants.hasReservedHeader(headers)) {
          StringBuilder sb = new StringBuilder();
          for (Entry<String,String> header: headers.entrySet()) {
            if (sb.length() > 0) {
              sb.append(",");
            }
            sb.append(header.getKey());
            resp.setHeader(header.getKey(), header.getValue());
          }
          resp.addHeader("Access-Control-Expose-Headers", sb.toString());
        }
      }

      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_ELAPSEDX), Long.toString(System.nanoTime() - now));
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_OPSX), stack.getAttribute(WarpScriptStack.ATTRIBUTE_OPS).toString());
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_FETCHEDX), stack.getAttribute(WarpScriptStack.ATTRIBUTE_FETCH_COUNT).toString());

      resp.addHeader("Access-Control-Expose-Headers", Constants.getHeader(Configuration.HTTP_HEADER_ERROR_LINEX) + "," + Constants.getHeader(Configuration.HTTP_HEADER_ERROR_MESSAGEX));
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_ERROR_LINEX), Long.toString(lineno));
      String headerErrorMsg = ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_HEADER_LENGTH);
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_ERROR_MESSAGEX), headerErrorMsg);

      //
      // Output the exported symbols in a map
      //

      Object exported = stack.getAttribute(WarpScriptStack.ATTRIBUTE_EXPORTED_SYMBOLS);

      if (exported instanceof Set && !((Set) exported).isEmpty()) {
        Map<String,Object> exports = new HashMap<String,Object>();
        Map<String,Object> symtable = stack.getSymbolTable();
        for (Object symbol: (Set) exported) {
          if (null == symbol) {
            exports.putAll(symtable);
            break;
          }
          exports.put(symbol.toString(), symtable.get(symbol.toString()));
        }
        try { stack.push(exports); if (debugDepth < Integer.MAX_VALUE) { debugDepth++; } } catch (WarpScriptException wse) {}
      }

      if(debugDepth > 0) {
        resp.setStatus(errorCode);
        PrintWriter pw = resp.getWriter();

        try {
          // Set max stack depth to max int value - 1 so we can push our error message
          stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.MAX_VALUE - 1);
          stack.push("ERROR line #" + lineno + ": " + ThrowableUtils.getErrorMessage(t));
          if (debugDepth < Integer.MAX_VALUE) {
            debugDepth++;
          }
        } catch (WarpScriptException ee) {
        }

        try {
          // As the resulting JSON is streamed, there is no need to limit its size.
          StackUtils.toJSON(pw, stack, debugDepth, Long.MAX_VALUE);
        } catch (WarpScriptException ee) {
        }

      } else {
        // Check if the response is already committed. This may happen if the writer has already been written to and an
        // error happened during the write, for instance a stack overflow caused by infinite recursion.
        if(!resp.isCommitted()) {
          String prefix = "";
          // If error happened before any WarpScript execution, do not add line.
          if (lineno > 0) {
            prefix = "ERROR line #" + lineno + ": ";
          }
          String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
          resp.sendError(errorCode, msg);
        }
        return;
      }
    } finally {
      stack.signal(Signal.KILL);
      WarpConfig.clearThreadProperties();
      WarpScriptStackRegistry.unregister(stack);

      // Clear this metric in case there was an exception
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_REQUESTS, Sensision.EMPTY_LABELS, 1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_TIME_US, Sensision.EMPTY_LABELS, (long) ((System.nanoTime() - now) / 1000));
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_OPS, Sensision.EMPTY_LABELS, (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_OPS));

      //
      // Record the JVM free memory
      //

      Sensision.set(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_JVM_FREEMEMORY, Sensision.EMPTY_LABELS, Runtime.getRuntime().freeMemory());

      LoggingEvent event = LogUtil.setLoggingEventAttribute(null, LogUtil.WARPSCRIPT_SCRIPT, scriptSB.toString());

      event = LogUtil.setLoggingEventAttribute(event, LogUtil.WARPSCRIPT_TIMES, times);

      if (null != t) {
        event = LogUtil.setLoggingEventStackTrace(event, LogUtil.STACK_TRACE, t);
        Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_ERRORS, Sensision.EMPTY_LABELS, 1);
      }

      LogUtil.addHttpHeaders(event, req);

      String msg = LogUtil.serializeLoggingEvent(this.keyStore, event);

      if (null != t) {
        EVENTLOG.error(msg);
      } else {
        EVENTLOG.info(msg);
      }
    }
  }

  public static final StoreClient getExposedStoreClient() {
    return exposedStoreClient;
  }

  public static final DirectoryClient getExposedDirectoryClient() {
    return exposedDirectoryClient;
  }
}
