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

package io.warp10.continuum;

import io.warp10.WarpConfig;
import io.warp10.WarpDist;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.Constants;
import io.warp10.crypto.KeyStore;
import io.warp10.quasar.filter.QuasarTokenFilter;
import io.warp10.quasar.filter.exception.QuasarTokenException;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.TokenType;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Tokens {
  
  private static final Map<String,Object> fileTokens = new HashMap<String,Object>();
  
  public static final Map<String,String> UUIDByIngressToken = new HashMap<String,String>();
  public static final Map<String,String> UUIDByEgressToken = new HashMap<String,String>();
  public static final Map<String,String> OwnerByToken = new HashMap<String,String>();
  public static final Map<String,String> ApplicationByUUID = new HashMap<String,String>();
  
  private static KeyStore keystore;
  
  private static QuasarTokenFilter tokenFilter;
  
  private static QuasarTokenFilter getTokenFilter() {
    if (null != tokenFilter) {
      return tokenFilter;
    }
    
    keystore = WarpDist.getKeyStore();
    if (null != keystore) {
      try {
        tokenFilter = new QuasarTokenFilter(WarpConfig.getProperties(), keystore);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    return tokenFilter;
  }
  
  public static ReadToken getReadToken(String token) {
    
    synchronized (fileTokens) {
      if (fileTokens.containsKey(token) && fileTokens.get(token) instanceof ReadToken) {
        return ((ReadToken) fileTokens.get(token)).deepCopy();
      }
    }
    
    if (!UUIDByEgressToken.containsKey(token)) {
      return null;
    }

    ReadToken rtoken = new ReadToken();
    
    UUID uuid = UUID.fromString(UUIDByEgressToken.get(token));

    ByteBuffer bb = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    bb.position(0);
    
    String app = ApplicationByUUID.get(UUIDByEgressToken.get(token));
        
    if (null != app) {
      rtoken.setAppName(app);
    } else {
      rtoken.setAppName("");
    }
    
    rtoken.setIssuanceTimestamp(0L);
    rtoken.setExpiryTimestamp(Long.MAX_VALUE);

    rtoken.setBilledId(bb.duplicate());
    rtoken.addToOwners(bb.duplicate());
    rtoken.addToProducers(bb.duplicate());
    
    return rtoken;
  }
  
  public static WriteToken getWriteToken(String token) {
    
    synchronized (fileTokens) {
      if (fileTokens.containsKey(token) && fileTokens.get(token) instanceof WriteToken) {
        return ((WriteToken) fileTokens.get(token)).deepCopy();
      }
    }

    if (!UUIDByIngressToken.containsKey(token)) {
      return null;
    }
    
    WriteToken wtoken = new WriteToken();
    
    UUID uuid = UUID.fromString(UUIDByIngressToken.get(token));

    ByteBuffer bb = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    bb.position(0);
    
    wtoken.setProducerId(bb.duplicate());
    wtoken.setOwnerId(bb.duplicate());
    
    String struuid = uuid.toString();
    
    if (null != ApplicationByUUID.get(struuid)) {
      wtoken.setAppName(ApplicationByUUID.get(struuid));
    }
    
    wtoken.setIssuanceTimestamp(0L);
    wtoken.setExpiryTimestamp(Long.MAX_VALUE);
    
    wtoken.setTokenType(TokenType.WRITE);
    
    return wtoken;
  }
  
  public static byte[] getUUID(String uuid) {
    UUID u = UUID.fromString(uuid);
    
    byte[] bytes = new byte[16];
    
    ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    
    bb.putLong(u.getMostSignificantBits());
    bb.putLong(u.getLeastSignificantBits());
    
    return bytes;
  }
  
  public static String getUUID(byte[] raw) {    
    if (null == raw) {
      return null;
    }
    
    return getUUID(ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN));
  }
  
  public static String getUUID(ByteBuffer buffer) {        
    ByteBuffer bb = buffer.duplicate();
    
    if (bb.remaining() < 16) {
      return null;
    }
    
    ByteOrder bo = bb.order();
    
    bb.order(ByteOrder.BIG_ENDIAN);

    long msb = bb.getLong();
    long lsb = bb.getLong();
    
    bb.order(bo);
    
    return new UUID(msb, lsb).toString();    
  }
  
  public static WriteToken extractWriteToken(String token) throws WarpScriptException {
    WriteToken wtoken = Tokens.getWriteToken(token);
    
    if (null != wtoken) {
      return wtoken;
    }
    
    // Attempt to decrypt a WriteToken
      
    try {
      QuasarTokenFilter qtf = getTokenFilter();
      
      if (null != qtf) {
        wtoken = qtf.getWriteToken(token);
      }
    } catch (QuasarTokenException qte) {
      throw new WarpScriptException(qte.getMessage());
    }

    if (null == wtoken) {
      throw new WarpScriptException("Invalid token.");
    }
    
    return wtoken;
  }
  
  public static ReadToken extractReadToken(String token) throws WarpScriptException {
    
    ReadToken rtoken = Tokens.getReadToken(token);
    
    if (null != rtoken) {
      return rtoken;
    }
      
    // Attempt to decrypt a ReadToken
      
    try {
      QuasarTokenFilter qtf = getTokenFilter();
      
      if (null != qtf) {
        rtoken = qtf.getReadToken(token);
      }
    } catch (QuasarTokenException qte) {
      throw new WarpScriptException(qte.getMessage());
    }

    if (null == rtoken) {
      throw new WarpScriptException("Invalid token.");
    }
    
    return rtoken;
  }
  
  /**
   * Return a map of selectors from the elements of the ReadToken
   * 
   * FIXME(hbs): if a ReadToken is a wildcard with a missing producer/owner/app, then
   * the returned map will have missing labels and therefore anything could be substituted for
   * those, thus leading potentially to data exposure
   * 
   */
  public static Map<String,String> labelSelectorsFromReadToken(ReadToken rtoken) {
    
    Map<String,String> labelSelectors = new HashMap<String,String>();
    
    List<String> owners = new ArrayList<String>();
    List<String> producers = new ArrayList<String>();
    Map<String, String> labels = new HashMap<String, String>();

    if (rtoken.getLabelsSize() > 0) {
      labels = rtoken.getLabels();
      if (!labels.isEmpty()) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
          switch (entry.getKey()) {
            case Constants.OWNER_LABEL:
            case Constants.APPLICATION_LABEL:
            case Constants.PRODUCER_LABEL:
              continue;
            default:
              labelSelectors.put(entry.getKey(),entry.getValue());
          }
        }
      }
    }

    if (rtoken.getOwnersSize() > 0) {
      for (ByteBuffer bb: rtoken.getOwners()) {
        owners.add(Tokens.getUUID(bb));
      }      
    }

    if (rtoken.getProducersSize() > 0) {
      for (ByteBuffer bb: rtoken.getProducers()) {
        producers.add(Tokens.getUUID(bb));
      }      
    }
    
    if (rtoken.getAppsSize() > 0) {
      if (1 == rtoken.getAppsSize()) {
        labelSelectors.put(Constants.APPLICATION_LABEL, "=" + rtoken.getApps().get(0));
      } else {
        StringBuilder sb = new StringBuilder();
        
        sb.append("~^(");
        boolean first = true;
        for (String app: rtoken.getApps()) {
          if (!first) {
            sb.append("|");
          }
          sb.append(app);
          first = false;
        }
        sb.append(")$");
        
        labelSelectors.put(Constants.APPLICATION_LABEL, sb.toString());        
      }
    }

    if (!owners.isEmpty()) {
      if (1 == owners.size()) {
        labelSelectors.put(Constants.OWNER_LABEL, "=" + owners.get(0));        
      } else {
        StringBuilder sb = new StringBuilder();
        
        sb.append("~^(");
        boolean first = true;
        for (String owner: owners) {
          if (!first) {
            sb.append("|");
          }
          sb.append(owner);
          first = false;
        }
        sb.append(")$");
        
        labelSelectors.put(Constants.OWNER_LABEL, sb.toString());
      }
    }
    
    if (!producers.isEmpty()) {
      if (1 == producers.size()) {
        labelSelectors.put(Constants.PRODUCER_LABEL, "=" + producers.get(0));        
      } else {
        StringBuilder sb = new StringBuilder();
                
        sb.append("~^(");
        boolean first = true;
        for (String producer: producers) {
          if (!first) {
            sb.append("|");
          }
          sb.append(producer);
          first = false;
        }
        sb.append(")$");
        
        labelSelectors.put(Constants.PRODUCER_LABEL, sb.toString());
      }
    }

    return labelSelectors;
  }
  
  /**
   * Load tokens from disk.

   * @param path Path to load tokens from
   */
  private static void loadTokens(String path) {
    
    if (null == path) {
      return;
    }
    
    File f = new File(path);
    
    if (!f.exists()) {
      return;
    }
    
    Map<String,ReadToken> readTokens = new HashMap<String,ReadToken>();
    Map<String,WriteToken> writeTokens = new HashMap<String,WriteToken>();
    
    Map<String,Object> tokens = new HashMap<String,Object>();
        
    try {
      BufferedReader br = new BufferedReader(new FileReader(path));
      
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        line = line.trim();
        
        if (!line.startsWith("token.")) {
          continue;
        }
        
        //
        // Extract token id
        //
        
        String id = line.substring(6).replaceAll("^(read|write)\\.", "").replaceAll("\\..*", "");
        String key = line.substring(6).replaceAll("^(read|write)\\.[^.]*\\.", "");
        String type = line.substring(6).replaceAll("\\..*", "");
        String value = key.replaceAll("^[^=]*=", "").trim();
        key = key.replaceAll("\\s*=.*","");
        
        ReadToken readToken = null;
        WriteToken writeToken = null;
        
        if ("read".equals(type)) {
          readToken = readTokens.get(id);
          if (null == readToken) {
            readToken = new ReadToken();
            readToken.setIssuanceTimestamp(0L);
            readTokens.put(id, readToken);
          }
          if ("producer".equals(key)) {
            readToken.addToProducers(ByteBuffer.wrap(Tokens.getUUID(value)));
          } else if ("owner".equals(key)) {
            readToken.addToOwners(ByteBuffer.wrap(Tokens.getUUID(value)));
          } else if ("app".equals(key)) {
            readToken.addToApps(value);
          } else if ("expiry".equals(key)) {
            readToken.setExpiryTimestamp(Long.valueOf(value));
          } else if ("billed".equals(key)) {
            readToken.setBilledId(Tokens.getUUID(value));
          } else if ("name".equals(key) || "id".equals(key)) {
            tokens.put(value, readToken);
          }
        } else if ("write".equals(type)) {
          writeToken = writeTokens.get(id);
          if (null == writeToken) {
            writeToken = new WriteToken();
            writeToken.setIssuanceTimestamp(0L);
            writeTokens.put(id, writeToken);
          }          
          if ("producer".equals(key)) {
            writeToken.setProducerId(Tokens.getUUID(value));
          } else if ("owner".equals(key)) {
            writeToken.setOwnerId(Tokens.getUUID(value));
          } else if ("app".equals(key)) {
            writeToken.setAppName(value);
          } else if ("expiry".equals(key)) {
            writeToken.setExpiryTimestamp(Long.valueOf(value));
          } else if ("name".equals(key) || "id".equals(key)) {
            tokens.put(value, writeToken);
          } else if ("labels".equals(key)) {
            Map<String,String> labels = GTSHelper.parseLabels(value);
            writeToken.setLabels(labels);
          }
        }
      }
      
      br.close();

      //
      // Sanitize tokens
      //
      
      for (Object token: tokens.values()) {
        if (token instanceof ReadToken) {
          ReadToken rt = (ReadToken) token;
          if (!rt.isSetBilledId()) {
            rt.setBilledId(getUUID("00000000-0000-0000-0000-000000000000"));
          }
          rt.setTokenType(TokenType.READ);
          
          if (!rt.isSetExpiryTimestamp()) {
            rt.setExpiryTimestamp(Long.MAX_VALUE);
          }          
        } else if (token instanceof WriteToken) {
          WriteToken wt = (WriteToken) token;          
          wt.setTokenType(TokenType.WRITE);
          
          if (!wt.isSetExpiryTimestamp()) {
            wt.setExpiryTimestamp(Long.MAX_VALUE);
          }                    
        }
      }
      
      //
      // Replace 'fileTokens'
      //
      
      synchronized(fileTokens) {
        fileTokens.clear();
        
        fileTokens.putAll(tokens);
      }      
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }
  
  public static final void init(final String file) {
    Thread t = new Thread() {
      
      private long lastLoad = System.currentTimeMillis();
      
      @Override
      public void run() {
        Tokens.loadTokens(file);
        while(true) {
          long now = System.currentTimeMillis();
          if (now - lastLoad > 60000L) {
            Tokens.loadTokens(file);
            lastLoad = now;
          }
          try { Thread.sleep(60000L); } catch (InterruptedException ie) {}
        }
      };
    };
    
    t.setName("[Token Manager]");
    t.setDaemon(true);
    t.start();
  }
}
