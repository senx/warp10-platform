//
//   Copyright 2020  SenX S.A.S.
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

package io.warp10.standalone.datalog;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geoxp.oss.CryptoHelper;
import com.google.common.primitives.Longs;

import io.warp10.WarpConfig;
import io.warp10.continuum.egress.EgressExecHandler;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.DatalogMessage;
import io.warp10.continuum.store.thrift.data.DatalogMessageType;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.binary.ADD;
import io.warp10.script.functions.ECDH;
import io.warp10.script.functions.ECPRIVATE;
import io.warp10.script.functions.ECPUBLIC;
import io.warp10.script.functions.ECSIGN;
import io.warp10.script.functions.HASH;
import io.warp10.script.functions.REVERSE;
import io.warp10.script.functions.TOLONGBYTES;
import io.warp10.sensision.Sensision;

/**
 * Consumes a Datalog feed and updates a backend accordingly
 */
public class TCPDatalogConsumer extends Thread implements DatalogConsumer {
  
  private static final String SENSISION_CLASS_DATALOG_IGNORED = "datalog.ignored";
  private static final String SENSISION_LABEL_CONSUMER = "consumer";
  private static final String SENSISION_LABEL_FEEDER = "feeder";
  
  private WarpScriptStack stack;
  
  private ECPrivateKey eccPrivate;
  private ECPublicKey eccPublic;

  private String id;
  
  /**
   * Set of IDs whose messages are ignored
   */
  private Set<String> excluded;

  private static final Logger LOG = LoggerFactory.getLogger(TCPDatalogConsumer.class);

  private long[] CLASS_KEYS;
  private long[] LABELS_KEYS;
  
  /**
   * Optional encryption key
   */
  private byte[] AES_KEY;
  
  /**
   * Lists of inflight, failed and successful commit refs
   */
  private List<String> inflight = new ArrayList<String>();
  private List<String> successful = new ArrayList<String>();
  private List<String> failed = new ArrayList<String>();

  private String suffix;
  
  @Override
  public void run() {
      
    String host = "127.0.0.1";
    int port = 4321;
            
    while(true) {
      Socket socket = null;
      
      try {
        InetAddress addr = InetAddress.getByName(host);
        
        socket = new Socket(addr, port);

        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        
        //
        // Read welcome message
        //
        
        byte[] bytes = DatalogHelper.readBlob(in, 0);
        
        DatalogMessage msg = new DatalogMessage();
        DatalogHelper.deserialize(bytes, msg);
        
        if (DatalogMessageType.WELCOME != msg.getType()) {
          throw new IOException("Invalid message type " + msg.getType().name() + ", aborting.");
        }
        
        //
        // Compute signature and emit INIT message
        //
        
        stack.clear();
        stack.push(Longs.toByteArray(msg.getNonce()));
        stack.push(Longs.toByteArray(msg.getTimestamp()));
        stack.push(this.id.getBytes(StandardCharsets.UTF_8));
        ADD ADD = new ADD(WarpScriptLib.ADD);
        ADD.apply(stack);
        ADD.apply(stack);
        ECSIGN ECSIGN = new ECSIGN(WarpScriptLib.ECSIGN);
        stack.push("SHA512WITHECDSA");
        stack.push(eccPrivate);
        ECSIGN.apply(stack);
        byte[] sig = (byte[]) stack.pop();
        
        boolean encrypt = msg.isEncrypt();
        
        if (encrypt) {
          //
          // Compute the encryption key using ECDH
          //
          
          String feederpub = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_FEEDER_ECC_PUBLIC + suffix);
          
          String[] tokens = feederpub.split(":");
          Map<String,String> map = new HashMap<String,String>();
          map.put(Constants.KEY_CURVE, tokens[0]);
          map.put(Constants.KEY_Q, tokens[1]);
            
          ECPublicKey feederPublic;
          
          try {
            stack.push(map);
            new ECPUBLIC(WarpScriptLib.ECPUBLIC).apply(stack);
            feederPublic = (ECPublicKey) stack.pop();
          } catch (WarpScriptException wse) {
            throw new RuntimeException("Error extracting ECC public key.", wse);
          }        

          long nonce = msg.getNonce();
          long timestamp = msg.getTimestamp();
          
          //
          // Key is
          // ->LONGBYTES(SIPHASH(nonce,timestamp,secret),8)
          // + ->LONGBYTES(SIPHASH(timestamp,nonce,secret),8)
          // + ->LONGBYTES(SIPHASH(nonce,timestamp,CLONEREVERSE(secret)),8)
          // + ->LONGBYTES(SIPHASH(timestamp,nonce,CLONEREVERSE(secret)),8)
          //          

          stack.clear();
          stack.push(eccPrivate);
          stack.push(feederPublic);
          new ECDH(WarpScriptLib.ECDH).apply(stack);
          byte[] secret = Hex.decode((String) stack.pop());
          
          stack.clear();
          stack.push(secret);
          stack.push(nonce);
          stack.push(timestamp);
          new HASH(WarpScriptLib.HASH).apply(stack);
          stack.push(8L);
          new TOLONGBYTES(WarpScriptLib.TOLONGBYTES).apply(stack);
          stack.push(secret);
          stack.push(timestamp);
          stack.push(nonce);
          new HASH(WarpScriptLib.HASH).apply(stack);
          stack.push(8L);
          new TOLONGBYTES(WarpScriptLib.TOLONGBYTES).apply(stack);
          stack.push(secret);
          new REVERSE(WarpScriptLib.REVERSE, false).apply(stack);
          stack.push(nonce);
          stack.push(timestamp);
          new HASH(WarpScriptLib.HASH).apply(stack);
          stack.push(8L);
          new TOLONGBYTES(WarpScriptLib.TOLONGBYTES).apply(stack);
          stack.push(secret);
          new REVERSE(WarpScriptLib.REVERSE, false).apply(stack);
          stack.push(timestamp);
          stack.push(nonce);
          new HASH(WarpScriptLib.HASH).apply(stack);
          stack.push(8L);
          new TOLONGBYTES(WarpScriptLib.TOLONGBYTES).apply(stack);
          new ADD(WarpScriptLib.ADD).apply(stack);
          new ADD(WarpScriptLib.ADD).apply(stack);
          new ADD(WarpScriptLib.ADD).apply(stack);
          
          this.AES_KEY = (byte[]) stack.pop();          
        }
        
        msg.clear();
        msg.setType(DatalogMessageType.INIT);
        msg.setId(this.id);
        msg.setSig(sig);
        
        bytes = DatalogHelper.serialize(msg);
        // We do not encrypt the init message since the feeder does not yet know who we
        // are
        DatalogHelper.writeLong(out, bytes.length, 4);
        out.write(bytes);
        out.flush();

        //
        // Now emit a seek message
        //
        
        msg.clear();
        msg.setType(DatalogMessageType.TSEEK);
        msg.setSeekts(0);
        
        bytes = DatalogHelper.serialize(msg);
        if (encrypt) {
          bytes = CryptoHelper.wrapBlob(AES_KEY, bytes);
        }
        DatalogHelper.writeLong(out, bytes.length, 4);
        out.write(bytes);
        out.flush();
        
        //
        // Now retrieve the DATA messages and push them to a worker
        //
        
        DatalogRecord record = new DatalogRecord();
        
        Map<String,String> labels = new HashMap<String,String>();
        labels.put(SENSISION_LABEL_CONSUMER, this.id);
        labels.put(SENSISION_LABEL_FEEDER, host + ":" + port);
             
        while(true) {
          
          //
          // Check if we should emit a commit or seek message (in case of failure)
          //
          
          synchronized (failed) {
            // All inflight messages have been acknowledged
            
            if (!inflight.isEmpty() && inflight.size() == failed.size() + successful.size()) {
              //
              // Scan the inflight refs and commit up before the first failed ref
              //
              
              int idx = 0;
              
              while (idx < inflight.size()) {
                if (failed.contains(inflight.get(idx))) {
                  idx--;
                  break;                  
                }
                idx++;
              }
              
              // idx is < 0, this means the first commit ref failed
              if (idx < 0) {
                //System.out.println(">>>SEEK " + inflight.get(0));
                // Send SEEK message for first failed ref
                msg.clear();
                msg.setType(DatalogMessageType.SEEK);
                msg.setRef(inflight.get(0));
                bytes = DatalogHelper.serialize(msg);
                if (encrypt) {
                  bytes = CryptoHelper.wrapBlob(AES_KEY, bytes);
                }
                DatalogHelper.writeLong(out, bytes.length, 4);
                out.write(bytes);
                out.flush();
                inflight.clear();
                failed.clear();
                successful.clear();                
              } else if (idx >= inflight.size()) {
                //System.out.println(">>>COMMIT " + inflight.get(inflight.size() - 1));
                // idx > inflight.size(), so all refs were successfully handled
                // Send COMMIT message for last ref in infligt
                
                msg.clear();
                msg.setType(DatalogMessageType.COMMIT);
                msg.setRef(inflight.get(inflight.size() - 1));
                bytes = DatalogHelper.serialize(msg);
                if (encrypt) {
                  bytes = CryptoHelper.wrapBlob(AES_KEY, bytes);
                }
                DatalogHelper.writeLong(out, bytes.length, 4);
                out.write(bytes);
                out.flush();
                inflight.clear();
                failed.clear();
                successful.clear();
              } else {
                //System.out.println(">>>COMMIT " + inflight.get(idx));
                //System.out.println(">>>SEEK " + inflight.get(idx + 1));
                // some refs failed and some succeeded
                // Send COMMIT message for last successful ref
                msg.clear();
                msg.setType(DatalogMessageType.COMMIT);
                msg.setRef(inflight.get(idx));
                bytes = DatalogHelper.serialize(msg);
                if (encrypt) {
                  bytes = CryptoHelper.wrapBlob(AES_KEY, bytes);
                }
                DatalogHelper.writeLong(out, bytes.length, 4);
                out.write(bytes);
                out.flush();
                
                // Send SEEK message for first failed ref
                msg.clear();
                msg.setType(DatalogMessageType.SEEK);
                msg.setRef(inflight.get(idx + 1));
                bytes = DatalogHelper.serialize(msg);
                if (encrypt) {
                  bytes = CryptoHelper.wrapBlob(AES_KEY, bytes);
                }
                DatalogHelper.writeLong(out, bytes.length, 4);
                out.write(bytes);
                out.flush();
                inflight.clear();
                failed.clear();
                successful.clear();
              }
            }
          }
          
          //
          // If less than 4 bytes are available, skip reading
          //
          
          if (in.available() < 4) {
            LockSupport.parkNanos(10000000L);
            continue;
          }
          
          //
          // Read the next message on the wire
          //
          
          bytes = DatalogHelper.readBlob(in, 0);
          if (encrypt) {
            bytes = CryptoHelper.unwrapBlob(AES_KEY, bytes);
          }

          msg.clear();
          DatalogHelper.deserialize(bytes, msg);
          
          if (DatalogMessageType.DATA != msg.getType()) {
            throw new IOException("Invalid message type " + msg.getType() + ", expected " + DatalogMessageType.DATA.name());
          }
                    
          //
          // Extract the DatalogRecord
          //
        
          record.clear();
          DatalogHelper.deserialize(msg.getRecord(), record);
          
          //
          // If our id created this message, ignore it, it would create a loop
          //
          
          if (this.excluded.contains(record.getId())) {
            //System.out.println("SKIPPING " + msg.getCommitref());
            Sensision.update(SENSISION_CLASS_DATALOG_IGNORED, labels, 1);
            success(msg.getCommitref());
            inflight.add(msg.getCommitref());
            continue;
          }

          //
          // Recompute the class and labels id with our own keys
          //
          
          long classid = GTSHelper.classId(CLASS_KEYS, record.getMetadata().getName());
          long labelsid = GTSHelper.labelsId(LABELS_KEYS, record.getMetadata().getLabels());
          
          record.getMetadata().setClassId(classid);
          record.getMetadata().setLabelsId(labelsid);
          
          //
          // Offer the message
          //
          
          DatalogWorkers.offer(this, msg.getCommitref(), record);
          inflight.add(msg.getCommitref());
        }
      } catch (Exception e) {        
        LOG.error("", e);
      } finally {
        if (null != socket) {
          try {
            socket.close();
          } catch (IOException ioe) {            
          }
        }
      }
      
    }
  }
  
  @Override
  public void init(KeyStore ks, String name) {
    this.suffix = "." + name;
    
    this.CLASS_KEYS = SipHashInline.getKey(ks.getKey(KeyStore.SIPHASH_CLASS));
    this.LABELS_KEYS = SipHashInline.getKey(ks.getKey(KeyStore.SIPHASH_LABELS));
    
    this.stack = new MemoryWarpScriptStack(EgressExecHandler.getExposedStoreClient(), EgressExecHandler.getExposedDirectoryClient());
    
    //
    // Extract ECC private/public key
    //
        
    String eccpri = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_ECC_PRIVATE + suffix);
    String eccpub = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_ECC_PUBLIC + suffix);

    if (null == eccpri || null == eccpub) {
      throw new RuntimeException("Missing ECC keys.");
    }
    
    String[] tokens = eccpri.split(":");
    Map<String,String> map = new HashMap<String,String>();
    map.put(Constants.KEY_CURVE, tokens[0]);
    map.put(Constants.KEY_D, tokens[1]);
            
    try {
      stack.push(map);
      new ECPRIVATE(WarpScriptLib.ECPRIVATE).apply(stack);
      eccPrivate = (ECPrivateKey) stack.pop();
    } catch (WarpScriptException wse) {
      throw new RuntimeException("Error extracting ECC private key.", wse);
    }

    tokens = eccpub.split(":");
    map.clear();
    map.put(Constants.KEY_CURVE, tokens[0]);
    map.put(Constants.KEY_Q, tokens[1]);
      
    try {
      stack.push(map);
      new ECPUBLIC(WarpScriptLib.ECPUBLIC).apply(stack);
      eccPublic = (ECPublicKey) stack.pop();
    } catch (WarpScriptException wse) {
      throw new RuntimeException("Error extracting ECC public key.", wse);
    }        
     
    this.id = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_ID + suffix);
    
    if (null == this.id) {
      throw new RuntimeException("Missing Datalog Consumer id '" + FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_ID + suffix + "'.");
    }
    
    this.excluded = new HashSet<String>();
    
    if (null == WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_EXCLUDED + suffix)) {
      throw new RuntimeException("Excluded ids MUST be specified using '" + FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_EXCLUDED + suffix + "'.");
    }
    
    String[] ids = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_EXCLUDED + suffix).split(",");
    for (String id: ids) {
      excluded.add(id.trim());
    }
    
    this.setName("[Datalog Consumer " + name + "]");
    this.setDaemon(true);
    this.start();
  }
  
  @Override
  public void failure(String ref) {
    synchronized (failed) {
      failed.add(ref);
    }
  }
  @Override  
  public void success(String ref) {
    synchronized (failed) {
      successful.add(ref);
    }
  }
}