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

package io.warp10.continuum.ingress;

import io.warp10.WarpManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandaloneIngressHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

/**
 * WebSocket handler which handles streaming updates
 * 
 * WARNING: since we push GTSEncoders only after we reached a threshold of we've changed GTS, plasma consumers
 *          will only see updates once the GTSEncoder has been transmitted to the StoreClient
 */
public class IngressStreamUpdateHandler extends WebSocketHandler.Simple {
    
  private final Ingress ingress;
  
  @WebSocket(maxTextMessageSize=1024 * 1024, maxBinaryMessageSize=1024 * 1024)
  public static class StandaloneStreamUpdateWebSocket {
    
    private IngressStreamUpdateHandler handler;
    
    private boolean errormsg = false;
    
    private long seqno = 0L;
    
    private WriteToken wtoken;

    private Map<String,String> sensisionLabels = new HashMap<String,String>();
    
    private Map<String,String> extraLabels = null;
    
    @OnWebSocketConnect
    public void onWebSocketConnect(Session session) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_REQUESTS, sensisionLabels, 1);
    }
    
    @OnWebSocketMessage
    public void onWebSocketMessage(Session session, String message) throws Exception {
      
      try {
        if (null != WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)) {
          throw new IOException(String.valueOf(WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)));
        }

        //
        // Split message on whitespace boundary
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
          long nowms = System.currentTimeMillis();
          
          // Atomic boolean to track if attributes were parsed
          AtomicBoolean hadAttributes = this.handler.ingress.parseAttributes ? new AtomicBoolean(false) : null;

          try {
            GTSEncoder lastencoder = null;
            GTSEncoder encoder = null;

            BufferedReader br = new BufferedReader(new StringReader(message));
            
            boolean lastHadAttributes = false;
            
            do {
              
              if (this.handler.ingress.parseAttributes) {
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
                continue;
              }
              
              if (null == this.wtoken) {
                throw new IOException("Missing token.");
              }

              try {
                encoder = GTSHelper.parse(lastencoder, line, extraLabels, now, this.handler.ingress.maxValueSize, hadAttributes);
              } catch (ParseException pe) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_PARSEERRORS, sensisionLabels, 1);
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
                
                if (null != lastencoder && lastencoder.size() > 0) {
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
                  encoder.setClassId(GTSHelper.classId(this.handler.ingress.classKey, encoder.getMetadata().getName()));
                  encoder.setLabelsId(GTSHelper.labelsId(this.handler.ingress.labelsKey, encoder.getMetadata().getLabels()));
                  
                  byte[] bytes = new byte[16];
                  GTSHelper.fillGTSIds(bytes, 0, encoder.getClassId(), encoder.getLabelsId());
                  BigInteger metadataCacheKey = new BigInteger(bytes);

                  boolean pushMeta = false;
                  if (!this.handler.ingress.metadataCache.containsKey(metadataCacheKey)) {
                    pushMeta = true;
                  } else if (this.handler.ingress.activityTracking && this.handler.ingress.updateActivity) {
                    Long lastActivity = this.handler.ingress.metadataCache.get(metadataCacheKey);
                      
                    if (null == lastActivity) {
                      pushMeta = true;
                    } else if (nowms - lastActivity > this.handler.ingress.activityWindow) {
                      pushMeta = true;
                    }
                  }
                  
                  if (pushMeta) {
                    Metadata metadata = new Metadata(encoder.getMetadata());
                    metadata.setSource(Configuration.INGRESS_METADATA_SOURCE);
                    if (this.handler.ingress.activityTracking && this.handler.ingress.updateActivity) {
                      metadata.setLastActivity(nowms);
                    }
                    this.handler.ingress.pushMetadataMessage(metadata);
                    synchronized(this.handler.ingress.metadataCache) {
                      this.handler.ingress.metadataCache.put(metadataCacheKey, (this.handler.ingress.activityTracking && this.handler.ingress.updateActivity) ? nowms : null);
                    }
                  }                  
                }

                if (null != lastencoder) {
                  lastencoder.setClassId(GTSHelper.classId(this.handler.ingress.classKey, lastencoder.getName()));
                  lastencoder.setLabelsId(GTSHelper.labelsId(this.handler.ingress.labelsKey, lastencoder.getLabels()));
                  this.handler.ingress.pushDataMessage(lastencoder);
                  count += lastencoder.getCount();
                  
                  if (this.handler.ingress.parseAttributes && lastHadAttributes) {
                    // We need to push lastencoder's metadata update as they were updated since the last
                    // metadata update message sent
                    Metadata meta = new Metadata(lastencoder.getMetadata());
                    meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
                    this.handler.ingress.pushMetadataMessage(meta);
                    lastHadAttributes = false;
                  }
                }
                
                if (encoder != lastencoder) {
                  // This is the case when we just parsed either the first input line or one for a different
                  // GTS than the previous one.

                  lastencoder = encoder;                  
                } else {
                  // This is the case when lastencoder and encoder are identical, but lastencoder was too big and needed
                  // to be flushed

                  //lastencoder = null
                  //
                  // Allocate a new GTSEncoder and reuse Metadata so we can
                  // correctly handle a continuation line if this is what occurs next
                  //
                  Metadata metadata = lastencoder.getMetadata();
                  lastencoder = new GTSEncoder(0L);
                  lastencoder.setMetadata(metadata);
                }
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

              lastencoder.setClassId(GTSHelper.classId(this.handler.ingress.classKey, lastencoder.getName()));
              lastencoder.setLabelsId(GTSHelper.labelsId(this.handler.ingress.labelsKey, lastencoder.getLabels()));
              this.handler.ingress.pushDataMessage(lastencoder);
              count += lastencoder.getCount();
              
              if (this.handler.ingress.parseAttributes && lastHadAttributes) {
                // Push a metadata UPDATE message so attributes are stored
                // Build metadata object to push
                Metadata meta = new Metadata(lastencoder.getMetadata());
                // Set source to indicate we
                meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
                this.handler.ingress.pushMetadataMessage(meta);
              }
            }                  
          } finally {
            this.handler.ingress.pushMetadataMessage(null);
            this.handler.ingress.pushDataMessage(null);
            nano = System.nanoTime() - nano;
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);          
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_MESSAGES, sensisionLabels, 1);
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_TIME_US, sensisionLabels, nano / 1000);  
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_DATAPOINTS_GLOBAL, Sensision.EMPTY_LABELS, count);
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
    
    public void setHandler(IngressStreamUpdateHandler handler) {
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
    }
  }
  
  public IngressStreamUpdateHandler(Ingress ingress) {
    super(StandaloneStreamUpdateWebSocket.class);

    this.ingress = ingress;
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
    
    final IngressStreamUpdateHandler self = this;

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
    if (this.ingress.properties.containsKey(Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE)) {
      factory.getPolicy().setMaxTextMessageSize((int) Long.parseLong(this.ingress.properties.getProperty(Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE)));
      factory.getPolicy().setMaxBinaryMessageSize((int) Long.parseLong(this.ingress.properties.getProperty(Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE)));
    }
    super.configure(factory);
  }    
}
