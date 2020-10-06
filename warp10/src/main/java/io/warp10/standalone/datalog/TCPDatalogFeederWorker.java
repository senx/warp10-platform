package io.warp10.standalone.datalog;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geoxp.oss.CryptoHelper;
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
import io.warp10.script.functions.ECPRIVATE;
import io.warp10.script.functions.ECPUBLIC;

public class TCPDatalogFeederWorker extends Thread {
  
  public static final int CMD_AUTH = 0x1;
  public static final int CMD_SEEK = 0x2;
  public static final int CMD_TSEEK = 0x3;
  public static final int CMD_COMMIT = 0x4;
  public static final int CMD_RECORD = 0x5;
  public static final int CMD_SHARDS = 0x6;
  public static final int CMD_NONCE = 0x7;
  
  static final int MAX_BLOB_SIZE = 1024;
  
  private static final long MAX_INFLIGHT_SIZE = 100000L;
  private static final int SOCKET_TIMEOUT = 300000;
  
  private static final Logger LOG = LoggerFactory.getLogger(TCPDatalogFeederWorker.class);
      
  private final FileBasedDatalogManager manager;
  
  /**
   * SequenceFile sync marks are 0xFFFFFFFF (4 bytes) followed by 16 bytes
   */
  private static final long SEQFILE_SYNC_MARKER_LEN = 20;
  
  private static final long MINSIZE = 200;
  private static final long STANDARD_WAIT = 5000000L;
  private final Socket socket;
  private final AtomicInteger clients;
  private final String checkmacro;
  
  public TCPDatalogFeederWorker(FileBasedDatalogManager manager, Socket socket, AtomicInteger clients, String checkmacro) {
    this.manager = manager;
    this.socket = socket;
    this.clients = clients;
    this.checkmacro = checkmacro;
    
    this.setDaemon(true);
    this.start();
  }
  

  @Override
  public void run() {
    try {
      this.socket.setSoTimeout(SOCKET_TIMEOUT);

      long mints = Long.MIN_VALUE;
      
      String dir = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_MANAGER_DIR);
            
      //
      // Allocate stack
      //
      
      WarpScriptStack stack = new MemoryWarpScriptStack(EgressExecHandler.getExposedStoreClient(), EgressExecHandler.getExposedDirectoryClient());
      
      boolean encrypt = "true".equals(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_ENCRYPT);
      
      //
      // Extract ECC private/public key
      //
      
      ECPrivateKey eccPrivate = null;
      ECPublicKey eccPublic = null;
      
      if (encrypt) {
        String eccpri = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_ECC_PRIVATE);
        String eccpub = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_ECC_PUBLIC);


        String[] tokens = eccpri.split(":");
        Map<String,String> map = new HashMap<String,String>();
        map.put(Constants.KEY_CURVE, tokens[0]);
        map.put(Constants.KEY_D, tokens[1]);
              
        try {
          stack.push(map);
          new ECPRIVATE(WarpScriptLib.ECPRIVATE).apply(stack);
          eccPrivate = (ECPrivateKey) stack.pop();
        } catch (WarpScriptException wse) {
          LOG.error("Error extracting ECC private key.", wse);
          return;
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
          LOG.error("Error extracting ECC public key.", wse);
          return;
        }        
      }
      
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", "file:///");
      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      FileSystem fs;
      
      try {
        fs = FileSystem.get(URI.create("/"), conf);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      
      long position = 0;
      String currentFile = null;
      // Get data from 10 minutes ago
      String previousFile = "000000000000000" + Long.toHexString(System.currentTimeMillis() - 600000L);
      previousFile = previousFile.substring(previousFile.length() - 16);
      
      long count = 0;
      
      SequenceFile.Reader reader = null;
      
      boolean newfile = true;

      InputStream in = socket.getInputStream();
      OutputStream out = socket.getOutputStream();
      
      //
      // Generate and emit WELCOME message
      //
      
      long nonce = ThreadLocalRandom.current().nextLong();
      long timestamp = System.currentTimeMillis();
      
      DatalogMessage msg = new DatalogMessage();
      msg.setType(DatalogMessageType.WELCOME);
      msg.setNonce(nonce);
      msg.setTimestamp(timestamp);
      msg.setEncrypt(encrypt);
            
      byte[] record = DatalogHelper.serialize(msg);
      DatalogHelper.writeLong(out, record.length, 4);
      out.write(record);

      //
      // Read the response, it MUST be an INIT message
      //
      
      record = DatalogHelper.readBlob(in, 0);

      msg.clear();
      DatalogHelper.deserialize(record, msg);
      
      if (DatalogMessageType.INIT != msg.getType()) {
        LOG.error("Invalid " + DatalogMessageType.INIT.name() + " message.");
        return;
      }
      
      //
      // Check signature and compute encryption key if encrypt is true
      // The check is performed by the DATALOG_CHECKMACRO macro
      // The macro is called with nonce, timestamp, id, sig, eccpriv
      // It returns an encryption key (or null if eccpriv is NULL) and throws an error (MSGFAIL / FAIL)
      // if the signature is invalid.
      //
      
      byte[] aesKey = null;
      
      try {
        stack.clear();
        stack.push(nonce);
        stack.push(timestamp);
        stack.push(msg.getId());
        stack.push(msg.getSig());
        stack.push(eccPrivate);
        stack.run(checkmacro);
        aesKey = (byte[]) stack.pop();
        
        if (encrypt && null == aesKey) {
          throw new WarpScriptException("Missing encryption key.");
        }
      } catch (WarpScriptException wse) {
        LOG.error("Error validating peer " + DatalogMessageType.INIT.name() + " message.", wse);
        return;
      }
      
      // Store the shards if they were defined
      
      int[] modulus = new int[msg.getShardsSize()];
      int[] remainder = new int[modulus.length];
      
      for (int i = 0; i< modulus.length; i++) {
        long shard = msg.getShards().get(i);
        modulus[i] = (int) ((shard >>> 32) & 0xFFFFFFFFL);
        remainder[i] = (int) (shard & 0xFFFFFFFFL);
      }
      
      // This is the number of bits to shift the <classID><labelsID> combo to the left, defaults to 48
      // to retain 32 bits covering 16 bits of classId and 16 bits of labelsId.
      long shardShift = 48;
      if (msg.isSetShardShift()) {
        shardShift = msg.getShardShift();
      }
      
      //
      // Wait for SEEK or TSEEK message
      //
      
      record = DatalogHelper.readBlob(in, 0);
      
      if (encrypt) {
        record = CryptoHelper.unwrapBlob(aesKey, record);
      }
      
      msg.clear();
      DatalogHelper.deserialize(record, msg);
      
      boolean checkts = false;
      
      System.out.println("SEEK "+ msg);

      if (DatalogMessageType.SEEK.equals(msg.getType())) {
        // Build file name from ts/uuid
        currentFile = new String(Hex.encode(Longs.toByteArray(msg.getFilets())), StandardCharsets.US_ASCII);
        UUID uuid = new UUID(msg.getMsb(), msg.getLsb());
        currentFile += "." + uuid.toString() + FileBasedDatalogManager.SUFFIX;
        String file = this.manager.getNextFile(currentFile);
        if (!currentFile.equals(file)) {
          position = 0L;
        } else {
          position = msg.getPosition();
        }        
      } else if (DatalogMessageType.TSEEK.equals(msg.getType())) {
        String hexts = new String(Hex.encode(Longs.toByteArray(msg.getSeekts())), StandardCharsets.US_ASCII);
        System.out.println("CHECKING " + hexts);
        currentFile = this.manager.getNextFile(hexts);

        if (null == currentFile || !currentFile.startsWith(hexts)) {
          currentFile = this.manager.getPreviousFile(hexts);
          
          // If there is no file before 'hexts', use the one after
          if (null == currentFile) {
            currentFile = this.manager.getNextFile(hexts);
          }
          
          checkts = true;        
        }
        
        mints = msg.getSeekts();
      } else {
        LOG.error("Invalid message type " + msg.getType().name() + ", expected " + DatalogMessageType.SEEK.name() + " or " + DatalogMessageType.TSEEK.name());
        return;
      }

      System.out.println("SEEK "+ msg + " >>> " + currentFile);

      //
      // List of inflight records (file:pos:size)
      //
      
      List<String> inflight = new ArrayList<String>();
      long size = 0;
      boolean limit = false;
            
      while(true) {
        try {          
          if (null == currentFile) {
            currentFile = this.manager.getNextFile(previousFile);
            newfile = true;
            checkts = false;
            position = 0;
          }
          
          // There is no file tracked by the manager
          if (null == currentFile) {
            LockSupport.parkNanos(STANDARD_WAIT);
            continue;
          }
          
          //
          // If the current file no longer exists, advance to the next and continue
          //
          
          Path path = new Path(dir, currentFile);
          
          if (!fs.exists(path)) {
            previousFile = currentFile;
            currentFile = this.manager.getNextFile(currentFile);
            newfile = true;
            position = 0;
            LockSupport.parkNanos(STANDARD_WAIT);
            continue;
          }
          
          //
          // Retrieve the size of the current file
          //
          
          long len = fs.getFileStatus(path).getLen();
          
          //
          // If the file length is less than the position then unless it is the last file, skip to the next
          //
          
          if (len < position) {
            String next = this.manager.getNextFile(currentFile);
            if (null != next && !currentFile.equals(next)) {
              // Check the size, if it is still less than the position, skip to the next
              if (len == fs.getFileStatus(path).getLen()) {
                previousFile = currentFile;
                currentFile = next;
                newfile = true;
                checkts = false;
                position = 0;
              }
            }
            LockSupport.parkNanos(STANDARD_WAIT);
            continue;
          }
          
          // If the file is less than MINSIZE, do not attempt to open it yet as it
          // might be the current Datalog file which did not record anything yet
          //
          
          if (len < MINSIZE || len < position + SEQFILE_SYNC_MARKER_LEN) {
            // Check if this file is the last, if it is not the case then it might be
            // a file that was left there without records and should be skipped.
            String next = this.manager.getNextFile(currentFile);
            if (null != next && !currentFile.equals(next)) {
              // Check the size, if it is still the same as len skip the file
              if (len == fs.getFileStatus(path).getLen()) {
                previousFile = currentFile;
                currentFile = next;
                newfile = true;
                checkts = false;
                position = 0;              
              }
            }
            LockSupport.parkNanos(STANDARD_WAIT);
            continue;
          }

          if (newfile) {
            // This is a newfile so reset checkts to false
            checkts = false;
            newfile = false;
            System.out.println("READ " + count + " FROM " + previousFile + " >>> " + currentFile);
            count = 0;
          }
          
          //
          // Open the current file
          //
          
          reader = new SequenceFile.Reader(conf,
              SequenceFile.Reader.file(path),
              SequenceFile.Reader.start(0));

          String fileref = path.getName().substring(0, 8 + 1 + 36);
          
          //
          // If we have a position other than 0, seek there
          //
          
          if (position > 0) {
            reader.seek(position);
          }
          
          BytesWritable key = new BytesWritable();
          BytesWritable val = new BytesWritable();

          //
          // Attempt to read records
          //
          
          while(!limit) {
            // Record the current position
            position = reader.getPosition();
            long prepos = position;
            if (!reader.next(key, val)) {
              break;
            }
            position = reader.getPosition();
            
            if (checkts) {
              byte[] k = key.getBytes();
              long timestamp2 = DatalogHelper.bytesToLong(k, 0, 8);
              // End the loop is timestamp is before mints
              if (timestamp2 < mints) {
                continue;
              }
            }
            
            //
            // Send the record and keep track of it in the list of in flight records
            //
            
            msg.clear();
            msg.setType(DatalogMessageType.DATA);
            ByteBuffer bb = ByteBuffer.wrap(val.getBytes(), 0, val.getLength());
            msg.setRecord(bb);

            String ref = fileref + ":" + prepos + ":" + val.getLength();
            msg.setCommitref(ref);

            inflight.add(ref);
            size += val.getLength();
            
            record = DatalogHelper.serialize(msg);
            if (encrypt) {
              record = CryptoHelper.wrapBlob(aesKey, record);
            }
            
            DatalogHelper.writeLong(out, record.length, 4);
            out.write(record);

            System.out.println(msg);
            if (size >= MAX_INFLIGHT_SIZE) {
              limit = true;
            }
            
            count++;            
          }
         
          //
          // If we voluntarily exited the loop due to the inflight limit being reached, wait for a
          // message from our peer, either SEEK/TSEEK or COMMIT
          //
          
          if (limit) {
            System.out.println("PAUSED AFTER " + size + " BYTES.");
            record = DatalogHelper.readBlob(in, 0);
            msg.clear();
            DatalogHelper.deserialize(record, msg);
            
            if (DatalogMessageType.COMMIT == msg.getType()) {
              // Find the index of the commit ref in the inflight list
              int idx = inflight.indexOf(msg.getRef());
              
              if (-1 == idx) {
                LOG.error("Invalid commit ref '" + msg.getRef() + "'.");
                return;
              }
              
              // Commit all messages up to 'ref', updating 'size' on the fly
              for (int i = 0; i < idx; i++) {
                size -= Long.parseLong(inflight.remove(0).replaceAll(".*:",""));
              }
            } else if (DatalogMessageType.SEEK == msg.getType()) {
              // Build file name from ts/uuid
              currentFile = new String(Hex.encode(Longs.toByteArray(msg.getFilets())), StandardCharsets.US_ASCII);
              UUID uuid = new UUID(msg.getMsb(), msg.getLsb());
              currentFile += "." + uuid.toString() + FileBasedDatalogManager.SUFFIX;
              String file = this.manager.getNextFile(currentFile);
              if (!currentFile.equals(file)) {
                position = 0L;
              } else {
                position = msg.getPosition();
              }        
              inflight.clear();
              size = 0;
            } else if (DatalogMessageType.TSEEK == msg.getType()) {
              String hexts = new String(Hex.encode(Longs.toByteArray(msg.getSeekts())), StandardCharsets.US_ASCII);
              currentFile = this.manager.getNextFile(hexts);

              if (null == currentFile || !currentFile.startsWith(hexts)) {
                currentFile = this.manager.getPreviousFile(hexts);
                checkts = true;        
              }
              
              mints = msg.getSeekts();
              inflight.clear();
              size = 0;
            } else {
              LOG.error("Invalid message type " + msg.getType().name() + ", expected " + DatalogMessageType.COMMIT + ", " + DatalogMessageType.SEEK + " or " + DatalogMessageType.TSEEK);
              return;
            }

            continue;
          }
          
          //
          // If the next file is not the current one, then if the current position is 20 bytes less than the file
          // size we indeed reached the EOF and we can go to the next file in the list
          //
          
          String next = this.manager.getNextFile(currentFile);
          
          if (!currentFile.equals(next)) {
            len = fs.getFileStatus(path).getLen();
            System.out.println("FILE=" + currentFile + " POS=" + position + " LEN=" + len + " NEXT=" + next);
            if (len == position + SEQFILE_SYNC_MARKER_LEN) {
              previousFile = currentFile;
              currentFile = next;
              newfile = true;
              position = 0;
            }
          }
          
          LockSupport.parkNanos(STANDARD_WAIT / 10);
        } catch (SocketTimeoutException ste) {
          LOG.error("Timeout exceeded while waiting for input.", ste);
          return;
        } catch (EOFException eofe) {
          LockSupport.parkNanos(STANDARD_WAIT / 10);
        } catch (Throwable t) {
          t.printStackTrace();
          return;
        } finally {
          if (null != reader) {
            try { reader.close(); } catch (Exception e) {}
            reader = null;
          }
        }
      }
    } catch (IOException ioe) {
      LOG.error("Error talking to Datalog consumer.", ioe);
    } finally {
      clients.decrementAndGet();
      if (null != this.socket) {
        try {
          this.socket.close();
        } catch (Exception e) {          
        }
      }
    }
  }
}
