package io.warp10.standalone.datalog;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

import io.warp10.WarpConfig;
import io.warp10.continuum.egress.EgressExecHandler;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.DatalogMessage;
import io.warp10.continuum.store.thrift.data.DatalogMessageType;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.binary.ADD;
import io.warp10.script.functions.ECPRIVATE;
import io.warp10.script.functions.ECPUBLIC;
import io.warp10.script.functions.ECSIGN;

/**
 * Consumes a Datalog feed and updates a backend accordingly
 */
public class TCPDatalogConsumer extends Thread {
  
  private final WarpScriptStack stack;
  
  private final ECPrivateKey eccPrivate;
  private final ECPublicKey eccPublic;

  private final String id;
  
  private static final Logger LOG = LoggerFactory.getLogger(TCPDatalogConsumer.class);
  
  public TCPDatalogConsumer() {
    this.stack = new MemoryWarpScriptStack(EgressExecHandler.getExposedStoreClient(), EgressExecHandler.getExposedDirectoryClient());
    
    //
    // Extract ECC private/public key
    //
        
    String eccpri = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_ECC_PRIVATE);
    String eccpub = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_ECC_PUBLIC);

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
     
    this.id = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_ID);
    
    if (null == this.id) {
      throw new RuntimeException("Missing Datalog Consumer id '" + FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_ID + "'.");
    }
    
    this.setDaemon(true);
    this.start();
  }
  
  @Override
  public void run() {
       
    while(true) {
      Socket socket = null;
      
      try {
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        int port = 4321;
        
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
        
        msg.clear();
        msg.setType(DatalogMessageType.INIT);
        msg.setId(this.id);
        msg.setSig(sig);
        
        bytes = DatalogHelper.serialize(msg);
        DatalogHelper.writeLong(out, bytes.length, 4);
        out.write(bytes);

        //
        // Now emit a seek message
        //
        
        msg.clear();
        msg.setType(DatalogMessageType.TSEEK);
        msg.setSeekts(0);
        
        bytes = DatalogHelper.serialize(msg);
        DatalogHelper.writeLong(out, bytes.length, 4);
        out.write(bytes);
        out.flush();
        LockSupport.parkNanos(10000000000L);
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
}
