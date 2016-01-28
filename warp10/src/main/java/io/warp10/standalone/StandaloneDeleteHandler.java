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

import io.warp10.continuum.Configuration;
import io.warp10.continuum.LogUtil;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.egress.EgressExecHandler;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneDeleteHandler extends AbstractHandler {
  
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneDeleteHandler.class);

  private final KeyStore keyStore;
  private final StoreClient storeClient;
  private final StandaloneDirectoryClient directoryClient;
  
  private final byte[] classKey;
  private final byte[] labelsKey;  
  
  private final long[] classKeyLongs;
  private final long[] labelsKeyLongs;
  
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  public StandaloneDeleteHandler(KeyStore keystore, StandaloneDirectoryClient directoryClient, StoreClient storeClient) {
    this.keyStore = keystore;
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    
    this.classKey = this.keyStore.getKey(KeyStore.SIPHASH_CLASS);
    this.classKeyLongs = SipHashInline.getKey(this.classKey);
    
    this.labelsKey = this.keyStore.getKey(KeyStore.SIPHASH_LABELS);
    this.labelsKeyLongs = SipHashInline.getKey(this.labelsKey);
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    
    if (target.equals(Constants.API_ENDPOINT_DELETE)) {
      baseRequest.setHandled(true);
    } else {
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
            
    WriteToken writeToken;
    
    try {
      writeToken = Tokens.extractWriteToken(token);
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }
    
    String application = writeToken.getAppName();
    String producer = Tokens.getUUID(writeToken.getProducerId());
    String owner = Tokens.getUUID(writeToken.getOwnerId());
      
    //
    // For delete operations, producer and owner MUST be equal
    //
    
    if (!producer.equals(owner)) {
      throw new IOException("Invalid write token for deletion.");
    }
    
    Map<String,String> sensisionLabels = new HashMap<String,String>();
    sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

    long count = 0;
    long gts = 0;
    
    Throwable t = null;
    StringBuilder metas = new StringBuilder();

    //
    // Extract start/end
    //
    
    String startstr = request.getParameter(Constants.HTTP_PARAM_START);
    String endstr = request.getParameter(Constants.HTTP_PARAM_END);
          
    //
    // Extract selector
    //
    
    String selector = request.getParameter(Constants.HTTP_PARAM_SELECTOR);
    
    String minage = request.getParameter(Constants.HTTP_PARAM_MINAGE);
    
    if (null != minage) {
      throw new IOException("Standalone version does not support the '" + Constants.HTTP_PARAM_MINAGE + "' parameter in delete requests.");
    }
    
    try {      
      if (null == producer || null == owner) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }
      
      //
      // Build extra labels
      //
      
      Map<String,String> extraLabels = new HashMap<String,String>();
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
      
      if (null != startstr) {
        if (null == endstr) {
          throw new IOException("Both " + Constants.HTTP_PARAM_START + " and " + Constants.HTTP_PARAM_END + " should be defined.");
        }
        if (startstr.contains("T")) {
          start = fmt.parseDateTime(startstr).getMillis() * Constants.TIME_UNITS_PER_MS;
        } else {
          start = Long.valueOf(startstr);
        }
      }
      
      if (null != endstr) {
        if (null == startstr) {
          throw new IOException("Both " + Constants.HTTP_PARAM_START + " and " + Constants.HTTP_PARAM_END + " should be defined.");
        }
        if (endstr.contains("T")) {
          end = fmt.parseDateTime(endstr).getMillis() * Constants.TIME_UNITS_PER_MS;          
        } else {
          end = Long.valueOf(endstr);
        }
      }
      
      if (Long.MIN_VALUE == start && Long.MAX_VALUE == end && null == request.getParameter(Constants.HTTP_PARAM_DELETEALL)) {
        throw new IOException("Parameter " + Constants.HTTP_PARAM_DELETEALL + " should be set when deleting a full range.");
      }
      
      if (Long.MIN_VALUE != start || Long.MAX_VALUE != end) {
        hasRange = true;
      }
      
      if (start > end) {
        throw new IOException("Invalid time range specification.");
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
      
      String classSelector = URLDecoder.decode(m.group(1), "UTF-8");
      String labelsSelection = m.group(2);
      
      Map<String,String> labelsSelectors;

      try {
        labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
      } catch (ParseException pe) {
        throw new IOException(pe);
      }
      
      //
      // Force 'producer'/'owner'/'app' from token
      //
      
      labelsSelectors.putAll(extraLabels);

      List<Metadata> metadatas = null;
      
      List<String> clsSels = new ArrayList<String>();
      List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
      clsSels.add(classSelector);
      lblsSels.add(labelsSelectors);
      
      metadatas = directoryClient.find(clsSels, lblsSels);

      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("text/plain");
      
      PrintWriter pw = response.getWriter();
      StringBuilder sb = new StringBuilder();
      
      for (Metadata metadata: metadatas) {        
        //
        // Remove from DB
        //
       
        if (!hasRange) {
          this.directoryClient.unregister(metadata);
        }
        
        //
        // Remove data
        //
        
        count += this.storeClient.delete(writeToken, metadata, start, end);
        sb.setLength(0);
        GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels());
        pw.write(sb.toString());
        pw.write("\r\n");
        metas.append(sb);
        metas.append("\n");
        gts++;
      }
    } catch (Exception e) {
      t = e;
      throw e;
    } finally {      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_REQUESTS, sensisionLabels, 1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_GTS, sensisionLabels, gts);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_DATAPOINTS, sensisionLabels, count);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_TIME_US, sensisionLabels, (System.nanoTime() - nano) / 1000);
      
      LoggingEvent event = LogUtil.setLoggingEventAttribute(null, LogUtil.DELETION_TOKEN, token);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_SELECTOR, selector);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_START, startstr);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_END, endstr);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_METADATA, metas.toString());
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_COUNT, Long.toString(count));
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DELETION_GTS, Long.toString(gts));
      
      if (null != t) {
        LogUtil.setLoggingEventStackTrace(null, LogUtil.STACK_TRACE, t);
      }
      
      LOG.info(LogUtil.serializeLoggingEvent(this.keyStore, event));

    }

    response.setStatus(HttpServletResponse.SC_OK);
  }  
}
