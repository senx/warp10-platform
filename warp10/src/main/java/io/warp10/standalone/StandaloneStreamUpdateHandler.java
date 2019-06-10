//
//   Copyright 2018  SenX S.A.S.
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

import io.warp10.WarpConfig;
import io.warp10.WarpManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.ingress.DatalogForwarder;
import io.warp10.continuum.ingress.Ingress;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DatalogRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.UpgradeResponse;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * WebSocket handler which handles streaming updates
 * 
 * WARNING: since we push GTSEncoders only after we reached a threshold of we've changed GTS, plasma consumers
 *          will only see updates once the GTSEncoder has been transmitted to the StoreClient
 */
public class StandaloneStreamUpdateHandler extends WebSocketHandler.Simple {
    
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneStreamUpdateHandler.class);
  
  private final KeyStore keyStore;
  private final Properties properties;
  private final StoreClient storeClient;
  private final StandaloneDirectoryClient directoryClient;
  private final long maxValueSize;
  
  private final String datalogId;
  private final byte[] datalogPSK;
  private final boolean datalogSync;
  private final File loggingDir;
  
  private final boolean updateActivity;
  private final boolean parseAttributes;
  
  private final DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSS").withZoneUTC();

  @WebSocket(maxTextMessageSize=1024 * 1024, maxBinaryMessageSize=1024 * 1024)
  public static class StandaloneStreamUpdateWebSocket {
    
    private static final int METADATA_CACHE_SIZE = 1000;
    
    private StandaloneStreamUpdateHandler handler;
    
    private boolean errormsg = false;
    
    private long seqno = 0L;
    
    private WriteToken wtoken;

    private String encodedToken;
    
    /**
     * Cache used to determine if we should push metadata into Kafka or if it was previously seen.
     * Key is a BigInteger constructed from a byte array of classId+labelsId (we cannot use byte[] as map key)
     */
    private final Map<BigInteger, Object> metadataCache = new LinkedHashMap<BigInteger, Object>(100, 0.75F, true) {
      @Override
      protected boolean removeEldestEntry(java.util.Map.Entry<BigInteger, Object> eldest) {
        return this.size() > METADATA_CACHE_SIZE;
      }
    };
    
    private Map<String,String> sensisionLabels = new HashMap<String,String>();
    
    private Map<String,String> extraLabels = null;
    
    @OnWebSocketConnect
    public void onWebSocketConnect(Session session) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_REQUESTS, sensisionLabels, 1);
    }
    
    @OnWebSocketMessage
    public void onWebSocketMessage(Session session, String message) throws Exception {
      
      try {
            
        if (null != WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)) {
          throw new IOException(String.valueOf(WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)));
        }

        //
        // Split message on whitespace boundary if the message starts by a known verb
        //
        
        String[] tokens = null;
      
        if (message.startsWith("TOKEN") || message.startsWith("CLEARTOKEN") || message.startsWith("NOOP") || message.startsWith("ONERROR")) {
          tokens = message.split("\\s+");
          tokens[0] = tokens[0].trim();
        }
              
        
        if (null != tokens && "TOKEN".equals(tokens[0])) {
          
          setToken(tokens[1]);
          
          session.getRemote().sendString("OK " + (seqno++) + " TOKEN");
        } else if (null != tokens && "CLEARTOKEN".equals(tokens[0])) {
          // Clear the current token
          this.wtoken = null;
          session.getRemote().sendString("OK " + (seqno++) + " CLEARTOKEN");
        } else if (null != tokens && "NOOP".equals(tokens[0])) {
          // Do nothing...
          session.getRemote().sendString("OK " + (seqno++) + " NOOP");
        } else if (null != tokens && "ONERROR".equals(tokens[0])) {
          if ("message".equalsIgnoreCase(tokens[1])) {
            this.errormsg = true;
          } else if ("close".equalsIgnoreCase(tokens[1])) {
            this.errormsg = false;
          }
          session.getRemote().sendString("OK " + (seqno++) + " ONERROR");
        } else {
          //
          // Anything else is considered a measurement
          //
          
          long nano = System.nanoTime();
          
          //
          // Loop on all lines
          //
          
          int count = 0;
          
          long now = TimeSource.getTime();
          
          File loggingFile = null;   
          PrintWriter loggingWriter = null;
          FileDescriptor loggingFD = null;
          DatalogRequest dr = null;
          
          try {
            GTSEncoder lastencoder = null;
            GTSEncoder encoder = null;

            BufferedReader br = new BufferedReader(new StringReader(message));

            // Atomic boolean to track if attributes were parsed
            AtomicBoolean hadAttributes = this.handler.parseAttributes ? new AtomicBoolean(false) : null;

            boolean lastHadAttributes = false;
            
            do {
              
              if (this.handler.parseAttributes) {
                lastHadAttributes = lastHadAttributes || hadAttributes.get();
                hadAttributes.set(false);
              }
              
              String line = br.readLine();
              
              if (null == line) {
                break;
              }

              //
              // Check if we encountered an 'UPDATE xxx' line
              //
              
              if (line.startsWith("UPDATE ")) {
                String[] subtokens = line.split("\\s+");
                setToken(subtokens[1]);
                
                //
                // Close the current datalog file if it exists
                //
                
                if (null != loggingWriter) {
                  Map<String,String> labels = new HashMap<String,String>();
                  labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(dr.getId().getBytes(Charsets.US_ASCII)), Charsets.UTF_8));
                  labels.put(SensisionConstants.SENSISION_LABEL_TYPE, dr.getType());
                  Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_REQUESTS_LOGGED, labels, 1);

                  if (handler.datalogSync) {
                    loggingWriter.flush();
                    loggingFD.sync();
                  }
                  loggingWriter.close();
                  loggingFile.renameTo(new File(loggingFile.getAbsolutePath() + DatalogForwarder.DATALOG_SUFFIX));
                  loggingFile = null;
                  loggingWriter = null;
                }
                
                continue;
              }

              if (null == this.wtoken) {
                throw new IOException("Missing token.");
              }

              //
              // Open the logging file if it is not open yet and if datalogging is enabled
              //
              
              if (null != handler.loggingDir && null == loggingFile) {
                long nanos = TimeSource.getNanoTime();
                StringBuilder sb = new StringBuilder();
                sb.append(Long.toHexString(nanos));
                sb.insert(0, "0000000000000000", 0, 16 - sb.length());
                sb.append("-");
                sb.append(handler.datalogId);
                
                sb.append("-");
                sb.append(handler.dtf.print(nanos / 1000000L));
                sb.append(Long.toString(1000000L + (nanos % 1000000L)).substring(1));
                sb.append("Z");
                
                dr = new DatalogRequest();
                dr.setTimestamp(nanos);
                dr.setType(Constants.DATALOG_UPDATE);
                dr.setId(handler.datalogId);
                dr.setToken(encodedToken); 
                
                //
                // Force 'now'
                //
                
                dr.setNow(Long.toString(now));
                
                //
                // Serialize the request
                //
                
                TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
                
                byte[] encoded;
                
                try {
                  encoded = ser.serialize(dr);
                } catch (TException te) {
                  throw new IOException(te);
                }
                
                if (null != handler.datalogPSK) {
                  encoded = CryptoUtils.wrap(handler.datalogPSK, encoded);
                }
                
                encoded = OrderPreservingBase64.encode(encoded);
                        
                loggingFile = new File(handler.loggingDir, sb.toString());
                
                FileOutputStream fos = new FileOutputStream(loggingFile);
                loggingFD = fos.getFD();
                OutputStreamWriter osw = new OutputStreamWriter(fos, Charsets.UTF_8);
                loggingWriter = new PrintWriter(osw);
                
                //
                // Write request
                //
                
                loggingWriter.println(new String(encoded, Charsets.US_ASCII));
              }

              try {
                encoder = GTSHelper.parse(lastencoder, line, extraLabels, now, this.handler.maxValueSize, hadAttributes);
              } catch (ParseException pe) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_PARSEERRORS, sensisionLabels, 1);
                throw new IOException("Parse error at '" + line + "'", pe);
              }

              //
              // Force PRODUCER/OWNER
              //
              
              //encoder.setLabel(Constants.PRODUCER_LABEL, producer);
              //encoder.setLabel(Constants.OWNER_LABEL, owner);
              
              if (encoder != lastencoder || lastencoder.size() > StandaloneIngressHandler.ENCODER_SIZE_THRESHOLD) {
                
                //
                // Check throttling
                //
                
                if (null != lastencoder) {
                  String producer = extraLabels.get(Constants.PRODUCER_LABEL);
                  String owner = extraLabels.get(Constants.OWNER_LABEL);
                  String application = extraLabels.get(Constants.APPLICATION_LABEL);
                  ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId());
                  ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount());
                }
                
                //
                // Build metadata object to push
                //
                
                if (encoder != lastencoder) {
                  Metadata metadata = new Metadata(encoder.getMetadata());
                  
                  if (this.handler.updateActivity) {
                    metadata.setLastActivity(System.currentTimeMillis());
                  }
                  
                  metadata.setSource(Configuration.INGRESS_METADATA_SOURCE);
                  this.handler.directoryClient.register(metadata);
                }

                if (null != lastencoder) {
                  // 128BITS
                  lastencoder.setClassId(GTSHelper.classId(this.handler.keyStore.getKey(KeyStore.SIPHASH_CLASS), lastencoder.getName()));
                  lastencoder.setLabelsId(GTSHelper.labelsId(this.handler.keyStore.getKey(KeyStore.SIPHASH_LABELS), lastencoder.getLabels()));
                  this.handler.storeClient.store(lastencoder);
                  count += lastencoder.getCount();
                  
                  
                  if (this.handler.parseAttributes && lastHadAttributes) {
                    // We need to push lastencoder's metadata update as they were updated since the last
                    // metadata update message sent
                    Metadata meta = new Metadata(lastencoder.getMetadata());
                    meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
                    this.handler.directoryClient.register(meta);
                    lastHadAttributes = false;
                  }
                }
                
                if (encoder != lastencoder) {
                  lastencoder = encoder;
                  
                  // This is the case when we just parsed either the first input line or one for a different
                  // GTS than the previous one.
                } else {
                  //lastencoder = null
                  //
                  // Allocate a new GTSEncoder and reuse Metadata so we can
                  // correctly handle a continuation line if this is what occurs next
                  //
                  Metadata metadata = lastencoder.getMetadata();
                  lastencoder = new GTSEncoder(0L);
                  lastencoder.setMetadata(metadata);
                  
                  // This is the case when lastencoder and encoder are identical, but lastencoder was too big and needed
                  // to be flushed
                }
              }
              
              if (null != loggingWriter) {
                loggingWriter.println(line);
              }
            } while (true); 
            
            br.close();
            
            if (null != lastencoder && lastencoder.size() > 0) {
              
              //
              // Check throttling
              //
              
              String producer = extraLabels.get(Constants.PRODUCER_LABEL);
              String owner = extraLabels.get(Constants.OWNER_LABEL);
              String application = extraLabels.get(Constants.APPLICATION_LABEL);
              ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId());
              ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount());

              lastencoder.setClassId(GTSHelper.classId(this.handler.keyStore.getKey(KeyStore.SIPHASH_CLASS), lastencoder.getName()));
              lastencoder.setLabelsId(GTSHelper.labelsId(this.handler.keyStore.getKey(KeyStore.SIPHASH_LABELS), lastencoder.getLabels()));
              this.handler.storeClient.store(lastencoder);
              count += lastencoder.getCount();
              
              if (this.handler.parseAttributes && lastHadAttributes) {
                // Push a metadata UPDATE message so attributes are stored
                // Build metadata object to push
                Metadata meta = new Metadata(lastencoder.getMetadata());
                // Set source to indicate we
                meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
                this.handler.directoryClient.register(meta);
              }
            }              
          } finally {
            if (null != loggingWriter) {              
              Map<String,String> labels = new HashMap<String,String>();
              labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(dr.getId().getBytes(Charsets.US_ASCII)), Charsets.UTF_8));
              labels.put(SensisionConstants.SENSISION_LABEL_TYPE, dr.getType());
              Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_REQUESTS_LOGGED, labels, 1);

              loggingWriter.close();
              loggingFile.renameTo(new File(loggingFile.getAbsolutePath() + DatalogForwarder.DATALOG_SUFFIX));
              loggingFile = null;
              loggingWriter = null;
            }

            this.handler.storeClient.store(null);
            this.handler.directoryClient.register(null);

            nano = System.nanoTime() - nano;
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);          
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_MESSAGES, sensisionLabels, 1);
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_TIME_US, sensisionLabels, nano / 1000);      
          }
          session.getRemote().sendString("OK " + (seqno++) + " UPDATE " + count + " " + nano);
        }        
      } catch (Throwable t) {
        if (this.errormsg) {
          session.getRemote().sendString("ERROR " + t.getMessage());
        } else {
          throw t;
        }
      }
    }
    
    @OnWebSocketClose    
    public void onWebSocketClose(Session session, int statusCode, String reason) {
    }
    
    public void setHandler(StandaloneStreamUpdateHandler handler) {
      this.handler = handler;
    }
    
    private void setToken(String token) throws IOException {
      //
      // TOKEN <TOKEN>
      //
      
      //
      // Extract token
      //
      
      WriteToken wtoken = null;
      
      try {
        wtoken = Tokens.extractWriteToken(token);          
      } catch (Exception e) {
        wtoken = null;
      }
      
      if (null == wtoken) {
        throw new IOException("Invalid token.");
      }

      if (wtoken.getAttributesSize() > 0 && wtoken.getAttributes().containsKey(Constants.TOKEN_ATTR_NOUPDATE)) {
        throw new IOException("Token cannot be used for updating data.");
      }

      String application = wtoken.getAppName();
      String producer = Tokens.getUUID(wtoken.getProducerId());
      String owner = Tokens.getUUID(wtoken.getOwnerId());
        
      this.sensisionLabels.clear();
      this.sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

      long count = 0;
      
      if (null == producer || null == owner) {
        throw new IOException("Invalid token.");
      }
        
      //
      // Build extra labels
      //
        
      this.extraLabels = new HashMap<String,String>();
      
      // Add labels from the WriteToken if they exist
      if (wtoken.getLabelsSize() > 0) {
        extraLabels.putAll(wtoken.getLabels());
      }
      
      // Force internal labels
      this.extraLabels.put(Constants.PRODUCER_LABEL, producer);
      this.extraLabels.put(Constants.OWNER_LABEL, owner);
      // FIXME(hbs): remove me
      if (null != application) {
        this.extraLabels.put(Constants.APPLICATION_LABEL, application);
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }

      this.wtoken = wtoken;
      this.encodedToken = token;
    }
  }
  
  public StandaloneStreamUpdateHandler(KeyStore keystore, Properties properties, StandaloneDirectoryClient directoryClient, StoreClient storeClient) {
    super(StandaloneStreamUpdateWebSocket.class);

    this.keyStore = keystore;
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    this.properties = properties;
    this.updateActivity = "true".equals(properties.getProperty(Configuration.INGRESS_ACTIVITY_UPDATE));

    this.maxValueSize = Long.parseLong(properties.getProperty(Configuration.STANDALONE_VALUE_MAXSIZE, StandaloneIngressHandler.DEFAULT_VALUE_MAXSIZE));
    
    this.parseAttributes = "true".equals(properties.getProperty(Configuration.INGRESS_PARSE_ATTRIBUTES));
    
    if (properties.containsKey(Configuration.DATALOG_DIR)) {
      File dir = new File(properties.getProperty(Configuration.DATALOG_DIR));
      
      if (!dir.exists()) {
        throw new RuntimeException("Data logging target '" + dir + "' does not exist.");
      } else if (!dir.isDirectory()) {
        throw new RuntimeException("Data logging target '" + dir + "' is not a directory.");
      } else {
        loggingDir = dir;
        LOG.info("Data logging enabled in directory '" + dir + "'.");
      }
      
      String id = properties.getProperty(Configuration.DATALOG_ID);
      
      if (null == id) {
        throw new RuntimeException("Property '" + Configuration.DATALOG_ID + "' MUST be set to a unique value for this instance.");
      } else {
        datalogId = new String(OrderPreservingBase64.encode(id.getBytes(Charsets.UTF_8)), Charsets.US_ASCII);
      }
      
    } else {
      loggingDir = null;
      datalogId = null;
    }

    this.datalogSync = "true".equals(WarpConfig.getProperty(Configuration.DATALOG_SYNC));
    if (properties.containsKey(Configuration.DATALOG_PSK)) {
      this.datalogPSK = this.keyStore.decodeKey(properties.getProperty(Configuration.DATALOG_PSK));
    } else {
      this.datalogPSK = null;
    }    
  }
     
  public DirectoryClient getDirectoryClient() {
    return this.directoryClient;
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (Constants.API_ENDPOINT_PLASMA_UPDATE.equals(target)) {
      baseRequest.setHandled(true);
      super.handle(target, baseRequest, request, response);
    }
  }
  
  @Override
  public void configure(final WebSocketServletFactory factory) {
    
    final StandaloneStreamUpdateHandler self = this;

    final WebSocketCreator oldcreator = factory.getCreator();
    
    WebSocketCreator creator = new WebSocketCreator() {
      @Override
      public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
        StandaloneStreamUpdateWebSocket ws = (StandaloneStreamUpdateWebSocket) oldcreator.createWebSocket(req, resp);
        ws.setHandler(self);
        return ws;
      }
    };

    factory.setCreator(creator);
    
    //
    // Update the maxMessageSize if need be
    //
    if (this.properties.containsKey(Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE)) {
      factory.getPolicy().setMaxTextMessageSize((int) Long.parseLong(this.properties.getProperty(Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE)));
      factory.getPolicy().setMaxBinaryMessageSize((int) Long.parseLong(this.properties.getProperty(Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE)));
    }

    super.configure(factory);
  }    
}
