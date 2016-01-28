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
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.ingress.Ingress;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Class used to configure a plasma client and which fetches plasma messages in the
 * background.
 */
public class StandalonePlasmaClientHandler extends AbstractHandler {
  
  /**
   * How many bytes do we buffer before flushing to persistent storage
   */
  public static final int ENCODER_SIZE_THRESHOLD = 100000;
  
  private final KeyStore keyStore;
  private final StoreClient storeClient;
  private final StandaloneDirectoryClient directoryClient;
  
  private final byte[] classKey;
  private final byte[] labelsKey;  
  
  private final long[] classKeyLongs;
  private final long[] labelsKeyLongs;
  
  private Map<String,PlasmaClient> clients = new HashMap<String,PlasmaClient>();
  
  public StandalonePlasmaClientHandler(KeyStore keystore, StandaloneDirectoryClient directoryClient, StoreClient storeClient) {
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
    
    if (Constants.API_ENDPOINT_PLASMA_CLIENT.equals(target)) {
      baseRequest.setHandled(true);
    } else {
      return;
    }    
    
    long nano = System.nanoTime();
    
    BufferedReader br = null;
    
    try {      
      br = request.getReader();
    
      do {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        if (line.startsWith("SUBSCRIBE ")) {
          // Syntax is SUBSCRIBE <URL> <TOKEN> <SELECTOR>
          String[] tokens = line.split("\\s+");
          
          if (4 != tokens.length) {
            throw new WarpScriptException("Invalid SUBSCRIBE syntax, should be SUBSCRIBE <URL> <TOKEN> <SELECTOR>");
          }
          
          PlasmaClient client = null;
          
          synchronized (clients) {
             client = clients.get(tokens[1]);
             
             if (null == client) {
               client = new PlasmaClient(tokens[1]);
               clients.put(tokens[1], client);
             }
          }
          
          client.subscribe(tokens[2], tokens[3]);
        } else if ("SUBSCRIPTIONS".equals(line)) {
          
        } else if ("CLEAR".equals(line)) {
        } else if (line.startsWith("CLEAR ")) {
          
        } else if (line.startsWith("REFRESH ")) {
        } else if (line.startsWith("TOKEN ")) {
          WriteToken writeToken = Tokens.extractWriteToken(line.substring(6).trim());
        } else {
          throw new IOException("Invalid verb.");
        }
      } while(true);
      
      br.close();
    } catch (Exception e) {      
      throw new IOException(e);
    }

    response.setStatus(HttpServletResponse.SC_OK);
  }
  
  /**
   * Handle Metadata updating
   */
  public void handleMeta(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (target.equals(Constants.API_ENDPOINT_META)) {
      baseRequest.setHandled(true);
    } else {
      return;
    }
    
    //
    // Loop over the input lines.
    // Each has the following format:
    //
    // class{labels}{attributes}
    //
    
    //
    // TODO(hbs): Extract producer/owner from token
    //
    
    String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
        
    
    String producer = Tokens.UUIDByIngressToken.get(token);
    String owner = Tokens.OwnerByToken.get(token);
    
    if (null == producer || null == owner) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
      return;
    }
    
    //
    // Determine if content if gzipped
    //

    boolean gzipped = false;
        
    if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {      
      gzipped = true;
    }
    
    BufferedReader br = null;
        
    if (gzipped) {
      GZIPInputStream is = new GZIPInputStream(request.getInputStream());
      br = new BufferedReader(new InputStreamReader(is));
    } else {    
      br = request.getReader();
    }
    
    //
    // Loop on all lines
    //

    while(true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      Metadata metadata = MetadataUtils.parseMetadata(line);
      
      //
      // Force owner/producer
      //
      
      metadata.getLabels().put(Constants.PRODUCER_LABEL, producer);
      metadata.getLabels().put(Constants.OWNER_LABEL, owner);
      
      if (!MetadataUtils.validateMetadata(metadata)) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid metadata " + line);
        return;
      }
      
      metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
      this.directoryClient.register(metadata);
    }
    
    response.setStatus(HttpServletResponse.SC_OK);
  }
}
