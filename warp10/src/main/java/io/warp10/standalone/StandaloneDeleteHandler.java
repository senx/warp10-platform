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

package io.warp10.standalone;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.io.EofException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.ThriftUtils;
import io.warp10.ThrowableUtils;
import io.warp10.WarpConfig;
import io.warp10.WarpManager;
import io.warp10.WarpURLDecoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.LogUtil;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.MetadataIdComparator;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.KeyStore;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.IngressPlugin;

public class StandaloneDeleteHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneDeleteHandler.class);

  private static final int MAX_LOGGED_DELETED_GTS = 1000;

  private final KeyStore keyStore;
  private final StoreClient storeClient;
  private final StandaloneDirectoryClient directoryClient;

  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  private final DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSS").withZoneUTC();

  private final boolean disabled;

  private IngressPlugin plugin = null;

  public StandaloneDeleteHandler(KeyStore keystore, StandaloneDirectoryClient directoryClient, StoreClient storeClient) {
    this.keyStore = keystore;
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    this.disabled = "true".equals(WarpConfig.getProperty(Configuration.STANDALONE_DELETE_DISABLE));
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (target.equals(Constants.API_ENDPOINT_DELETE)) {
      baseRequest.setHandled(true);
    } else {
      return;
    }

    if (disabled) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Delete endpoint is disabled by configuration.");
      return;
    }

    if (null != WarpManager.getAttribute(WarpManager.DELETE_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.DELETE_DISABLED)));
      return;
    }

    //
    // CORS header
    //

    response.setHeader("Access-Control-Allow-Origin", "*");

    long nano = System.nanoTime();

    //
    // TODO(hbs): Extract producer/owner from token
    //

    String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));

    if (null == token) {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Missing token.");
      return;
    }

    WriteToken writeToken;

    try {
      writeToken = Tokens.extractWriteToken(token);
      if (writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_NODELETE)) {
        throw new WarpScriptException("Token cannot be used for deletions.");
      }
    } catch (WarpScriptException ee) {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ThrowableUtils.getErrorMessage(ee, Constants.MAX_HTTP_REASON_LENGTH));
      return;
    }

    String application = writeToken.getAppName();
    String producer = Tokens.getUUID(writeToken.getProducerId());
    String owner = Tokens.getUUID(writeToken.getOwnerId());

    boolean expose = writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_EXPOSE);

    //
    // For delete operations, producer and owner MUST be equal
    //

    if (!producer.equals(owner)) {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Invalid write token for deletion.");
      return;
    }

    Map<String,String> sensisionLabels = new HashMap<String,String>();
    sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

    long count = 0;
    long gts = 0;

    Throwable t = null;
    StringBuilder metas = new StringBuilder();
    // Boolean indicating whether or not we should continue adding results to 'metas'
    boolean metasSaturated = false;

    //
    // Extract start/end
    //

    String startstr = request.getParameter(Constants.HTTP_PARAM_START);
    String endstr = request.getParameter(Constants.HTTP_PARAM_END);

    //
    // Extract nocache/nopersist
    //

    boolean nocache = AcceleratorConfig.getDefaultDeleteNocache();
    boolean forcedNocache = false;
    boolean nopersist = AcceleratorConfig.getDefaultDeleteNopersist();
    boolean forcedNopersist = false;

    if (null != request.getParameter(AcceleratorConfig.NOCACHE)) {
      forcedNocache = true;
      nocache = true;
    }
    if (null != request.getParameter(AcceleratorConfig.CACHE)) {
      if (forcedNocache) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot specify both '" + AcceleratorConfig.NOCACHE + "' and '" + AcceleratorConfig.CACHE + "'.");;
        return;
      }
      forcedNocache = true;
      nocache = false;
    }
    if (null != request.getParameter(AcceleratorConfig.NOPERSIST)) {
      forcedNopersist = true;
      nopersist = true;
    }
    if (null != request.getParameter(AcceleratorConfig.PERSIST)) {
      if (forcedNopersist) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot specify both '" + AcceleratorConfig.NOPERSIST + "' and '" + AcceleratorConfig.PERSIST + "'.");;
        return;
      }
      forcedNopersist = true;
      nopersist = false;
    }

    if (nocache && nopersist) {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot specify both '" + AcceleratorConfig.NOCACHE + "' and '" + AcceleratorConfig.NOPERSIST + "'.");;
      return;
    }

    //
    // Extract selector
    //

    String selector = request.getParameter(Constants.HTTP_PARAM_SELECTOR);

    String minage = request.getParameter(Constants.HTTP_PARAM_MINAGE);

    if (null != minage) {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Standalone version does not support the '" + Constants.HTTP_PARAM_MINAGE + "' parameter in delete requests.");
      return;
    }

    boolean dryrun = null != request.getParameter(Constants.HTTP_PARAM_DRYRUN);

    boolean validated = false;

    try {
      if (null == producer || null == owner) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }

      //
      // Build extra labels
      //

      Map<String,String> extraLabels = new HashMap<String,String>();

      // Add extra labels, remove producer,owner,app
      if (writeToken.getLabelsSize() > 0) {
        extraLabels.putAll(writeToken.getLabels());
        extraLabels.remove(Constants.PRODUCER_LABEL);
        extraLabels.remove(Constants.OWNER_LABEL);
        extraLabels.remove(Constants.APPLICATION_LABEL);
      }

      //
      // Only set owner and potentially app, producer may vary
      //
      extraLabels.put(Constants.OWNER_LABEL, owner);
      // FIXME(hbs): remove me
      if (null != application) {
        extraLabels.put(Constants.APPLICATION_LABEL, application);
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }

      boolean hasRange = false;

      long start = Long.MIN_VALUE;
      long end = Long.MAX_VALUE;

      // Both or neither start and end must be specified
      if (null == startstr ^ null == endstr) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Both " + Constants.HTTP_PARAM_START + " and " + Constants.HTTP_PARAM_END + " should be defined.");
        return;
      }

      // Parse start and end parameters
      if (null != startstr) { // also implies endstr is not null.
        if (startstr.contains("T")) {
          start = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(startstr);
        } else {
          start = Long.parseLong(startstr);
        }

        if (endstr.contains("T")) {
          end = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(endstr);
        } else {
          end = Long.parseLong(endstr);
        }

        hasRange = true;
      }

      boolean metaonly = null != request.getParameter(Constants.HTTP_PARAM_METAONLY);

      if (!hasRange && (null == request.getParameter(Constants.HTTP_PARAM_DELETEALL) && !metaonly)) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Parameter " + Constants.HTTP_PARAM_DELETEALL + " or " + Constants.HTTP_PARAM_METAONLY + " should be set when no time range is specified.");
        return;
      }

      if (metaonly && !Constants.DELETE_METAONLY_SUPPORT) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Parameter " + Constants.HTTP_PARAM_METAONLY + " cannot be used as metaonly support is not enabled.");
        return;
      }

      if (metaonly && hasRange) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Parameter " + Constants.HTTP_PARAM_METAONLY + " can only be set if no range is specified.");
        return;
      }

      if (!hasRange && (nocache || nopersist)) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Time range is mandatory when specifying '" + AcceleratorConfig.NOCACHE + "' or '" + AcceleratorConfig.NOPERSIST + "'.");
        return;
      }

      if (start > end) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Invalid time range specification.");
        return;
      }

      //
      // Extract the class and labels selectors
      // The class selector and label selectors are supposed to have
      // values which use percent encoding, i.e. explicit percent encoding which
      // might have been re-encoded using percent encoding when passed as parameter
      //
      //

      Matcher m = EgressFetchHandler.SELECTOR_RE.matcher(selector);

      if (!m.matches()) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }

      String classSelector = WarpURLDecoder.decode(m.group(1), StandardCharsets.UTF_8);
      String labelsSelection = m.group(2);

      Map<String,String> labelsSelectors;

      try {
        labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
      } catch (ParseException pe) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ThrowableUtils.getErrorMessage(pe, Constants.MAX_HTTP_REASON_LENGTH));
        return;
      }

      validated = true;

      //
      // Force 'producer'/'owner'/'app' from token
      //

      labelsSelectors.putAll(extraLabels);

      List<Metadata> metadatas = null;

      List<String> clsSels = new ArrayList<String>();
      List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
      clsSels.add(classSelector);
      lblsSels.add(labelsSelectors);

      DirectoryRequest drequest = new DirectoryRequest();

      Long activeAfter = null == request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER) ? null : Long.parseLong(request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER));
      Long quietAfter = null == request.getParameter(Constants.HTTP_PARAM_QUIETAFTER) ? null : Long.parseLong(request.getParameter(Constants.HTTP_PARAM_QUIETAFTER));

      if (!Constants.DELETE_ACTIVITY_SUPPORT) {
        if (null != activeAfter || null != quietAfter) {
          response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Activity based selection is disabled by configuration.");
          return;
        }
      }

      if (null != activeAfter) {
        drequest.setActiveAfter(activeAfter);
      }

      if (null != quietAfter) {
        drequest.setQuietAfter(quietAfter);
      }

      drequest.setClassSelectors(clsSels);
      drequest.setLabelsSelectors(lblsSels);

      metadatas = directoryClient.find(drequest);

      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("text/plain");

      PrintWriter pw = response.getWriter();
      StringBuilder sb = new StringBuilder();

      //
      // Sort Metadata by classid/labels id so deletion is more efficient
      //

      metadatas.sort(MetadataIdComparator.COMPARATOR);

      if (nocache) {
        AcceleratorConfig.nocache();
      } else {
        AcceleratorConfig.cache();
      }

      if (nopersist) {
        AcceleratorConfig.nopersist();
      } else {
        AcceleratorConfig.persist();
      }

      for (Metadata metadata: metadatas) {
        //
        // Remove data
        //

        long localCount = 0;

        if (!dryrun) {
          if (null != this.plugin) {
            if (!this.plugin.delete(this, writeToken, metadata)) {
              continue;
            }
          }
          if (!metaonly) {
            localCount = this.storeClient.delete(writeToken, metadata, start, end);
          }
        }

        //
        // Remove metadata from DB and Directory
        //

        if (!hasRange) {
          if (!dryrun) {
            this.directoryClient.unregister(metadata);
          }
        }

        count += localCount;

        sb.setLength(0);
        GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels(), expose);

        if (metadata.getAttributesSize() > 0) {
          // Always expose attributes
          GTSHelper.labelsToString(sb, metadata.getAttributes(), true);
        } else {
          sb.append("{}");
        }

        pw.write(sb.toString());
        pw.write("\r\n");
        if (!metasSaturated) {
          if (gts < MAX_LOGGED_DELETED_GTS) {
            metas.append(sb);
            metas.append("\n");
          } else {
            metasSaturated = true;
            metas.append("...");
            metas.append("\n");
          }
        }

        gts++;

        // Log detailed metrics for this GTS owner and app
        Map<String, String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_OWNER, metadata.getLabels().get(Constants.OWNER_LABEL));
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, metadata.getLabels().get(Constants.APPLICATION_LABEL));
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_DATAPOINTS_PEROWNERAPP, labels, localCount);
      }

      // Call delete with a null Metadata so the FDB backend gets a chance to commit any pending mutations
      this.storeClient.delete(writeToken, null, start, end);
    } catch (Throwable thr) {
      t = thr;
      // If we have not yet written anything on the output stream, call sendError
      if (0 == gts && !response.isCommitted()) {
        String prefix = "Error when deleting data: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(thr, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
      } else if (thr.getCause() instanceof EofException) {
        LOG.info((dryrun ? "Dry-run delete" : "Delete") + " request was aborted.");
      } else {
        throw new IOException(thr);
      }
    } finally {
      if (!dryrun) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_REQUESTS, sensisionLabels, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_GTS, sensisionLabels, gts);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_DATAPOINTS, sensisionLabels, count);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_TIME_US, sensisionLabels, (System.nanoTime() - nano) / 1000);
      }

      LoggingEvent event = LogUtil.setLoggingEventAttribute(null, LogUtil.DELETION_TOKEN, token);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_SELECTOR, selector);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_START, startstr);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_END, endstr);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_METADATA, metas.toString());
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_COUNT, Long.toString(count));
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_GTS, Long.toString(gts));

      LogUtil.addHttpHeaders(event, request);

      if (null != t) {
        LogUtil.setLoggingEventStackTrace(null, LogUtil.STACK_TRACE, t);
      }

      LOG.info(LogUtil.serializeLoggingEvent(this.keyStore, event));

      if (null != this.plugin) {
        this.plugin.flush(this);
      }
    }

    response.setStatus(HttpServletResponse.SC_OK);
  }

  public void setPlugin(IngressPlugin plugin) {
    this.plugin = plugin;
  }
}
