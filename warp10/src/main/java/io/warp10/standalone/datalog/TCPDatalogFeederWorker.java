//
//   Copyright 2020-2023  SenX S.A.S.
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

import java.io.EOFException;
import java.io.FileNotFoundException;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.DatalogMessage;
import io.warp10.continuum.store.thrift.data.DatalogMessageType;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
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

public class TCPDatalogFeederWorker extends Thread {

  private static final String DEFAULT_MAXSIZE = Integer.toString(1024 * 1024);
  private static final String DEFAULT_INFLIGT = Long.toString(1000000L);
  private static final String DEFAULT_TIMEOUT = Integer.toString(300000);

  static final int MAX_BLOB_SIZE;
  private final long MAX_INFLIGHT_SIZE;
  private final int SOCKET_TIMEOUT;

  static {
    MAX_BLOB_SIZE = Integer.parseInt(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_MAXSIZE, DEFAULT_MAXSIZE));
  }

  private static final Logger LOG = LoggerFactory.getLogger(TCPDatalogFeederWorker.class);

  private final FileBasedDatalogManager manager;

  /**
   * SequenceFile sync marks are 0xFFFFFFFF (4 bytes) followed by 16 bytes
   */
  private static final long SEQFILE_SYNC_MARKER_LEN = 20;

  private static final long MINSIZE = 168;
  private static final long STANDARD_WAIT = 5000000L;
  private final Socket socket;
  private final AtomicInteger clients;
  private final String checkmacro;

  public TCPDatalogFeederWorker(FileBasedDatalogManager manager, Socket socket, AtomicInteger clients, String checkmacro) {
    this.manager = manager;
    this.socket = socket;
    this.clients = clients;
    this.checkmacro = checkmacro;

    this.MAX_INFLIGHT_SIZE = Long.parseLong(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_INFLIGHT, DEFAULT_INFLIGT));
    this.SOCKET_TIMEOUT = Integer.parseInt(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_TIMEOUT, DEFAULT_TIMEOUT));

    this.setName("[Datalog Feeder Worker]");
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

      boolean encrypt = "true".equals(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_ENCRYPT));

      String id = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_ID);

      Map<String,String> typeLabels = new LinkedHashMap<String,String>();
      typeLabels.put(SensisionConstants.SENSISION_LABEL_FEEDER, id);

      //
      // Extract ECC private/public key
      //

      ECPrivateKey eccPrivate = null;
      ECPublicKey eccPublic = null;

      String eccpri = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_ECC_PRIVATE);

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
      msg.setId(id);

      //
      // Sign nonce + timestamp + id
      //

      stack.clear();
      stack.push(Longs.toByteArray(msg.getNonce()));
      stack.push(Longs.toByteArray(msg.getTimestamp()));
      stack.push(id.getBytes(StandardCharsets.UTF_8));
      ADD ADD = new ADD(WarpScriptLib.ADD);
      ADD.apply(stack);
      ADD.apply(stack);
      ECSIGN ECSIGN = new ECSIGN(WarpScriptLib.ECSIGN);
      stack.push("SHA512WITHECDSA");
      stack.push(eccPrivate);
      ECSIGN.apply(stack);
      byte[] sig = (byte[]) stack.pop();

      msg.setSig(sig);

      byte[] bytes = DatalogHelper.serialize(msg);
      DatalogHelper.writeLong(out, bytes.length, 4);
      out.write(bytes);
      out.flush();

      //
      // Read the response, it MUST be an INIT message
      //

      bytes = DatalogHelper.readBlob(in, 0);

      msg.clear();
      DatalogHelper.deserialize(bytes, msg);

      if (DatalogMessageType.INIT != msg.getType()) {
        LOG.error("Invalid " + DatalogMessageType.INIT.name() + " message.");
        return;
      }

      typeLabels.put(SensisionConstants.SENSISION_LABEL_CONSUMER, msg.getId());
      typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.INIT.name());
      Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_FEEDER_MESSAGES_IN, typeLabels, 1);

      //
      // Check consumer signature and compute encryption key if encrypt is true
      // The check is performed by the DATALOG_CHECKMACRO macro
      // The macro is called with nonce, timestamp, id, sig
      // It returns the ECC public key associated with 'id' and throws an error (MSGFAIL / FAIL)
      // if the signature is invalid.
      //

      byte[] aesKey = null;

      try {
        stack.clear();
        stack.push(nonce);
        stack.push(timestamp);
        stack.push(msg.getId());
        stack.push(msg.getSig());
        stack.run(checkmacro);
        ECPublicKey consumerPub = (ECPublicKey) stack.pop();

        if (encrypt && null == consumerPub) {
          throw new WarpScriptException("Missing consumer public key for consumer '" + msg.getId() + "'.");
        }

        if (encrypt) {
          //
          // Key is
          // ->LONGBYTES(SIPHASH(nonce,timestamp,secret),8)
          // + ->LONGBYTES(SIPHASH(timestamp,nonce,secret),8)
          // + ->LONGBYTES(SIPHASH(nonce,timestamp,CLONEREVERSE(secret)),8)
          // + ->LONGBYTES(SIPHASH(timestamp,nonce,CLONEREVERSE(secret)),8)
          //

          stack.clear();
          stack.push(eccPrivate);
          stack.push(consumerPub);
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

          aesKey = (byte[]) stack.pop();
        }
      } catch (WarpScriptException wse) {
        LOG.error("Error validating peer " + DatalogMessageType.INIT.name() + " message.", wse);
        return;
      }

      // Store the shards if they were defined

      long[] modulus = new long[msg.getShardsSize()];
      long[] remainder = new long[modulus.length];

      boolean hasShards = msg.getShardsSize() > 0;
      for (int i = 0; i< modulus.length; i++) {
        long shard = msg.getShards().get(i);
        modulus[i] = ((shard >>> 32) & 0xFFFFFFFFL);
        remainder[i] = (shard & 0xFFFFFFFFL);
      }

      // Store the excluded ids

      List<String> excluded = null;

      if (msg.getExcludedSize() > 0) {
        excluded = new ArrayList<String>(msg.getExcluded());
      }

      // This is the number of bits to shift the <classID><labelsID> combo to the right, defaults to 48.
      // The shard is determined by shifting <classID><labelsID> to the right and keeping the lower 32 bits
      // on which a modulus is applied. The remainder of this operation is the shard id.

      long shardShift = 48;
      if (msg.isSetShardShift()) {
        shardShift = msg.getShardShift();
      }

      //
      // Wait for SEEK or TSEEK message
      //

      bytes = DatalogHelper.readBlob(in, 0);

      if (encrypt) {
        bytes = CryptoHelper.unwrapBlob(aesKey, bytes);
      }

      msg.clear();
      DatalogHelper.deserialize(bytes, msg);

      boolean checkts = false;

      //System.out.println("SEEK "+ msg);

      if (DatalogMessageType.SEEK.equals(msg.getType())) {        // Build file name from ts/uuid
        typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.SEEK.name());
        Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_FEEDER_MESSAGES_IN, typeLabels, 1);
        currentFile = msg.getRef().replaceAll(":.*","") + FileBasedDatalogManager.SUFFIX;
        String file = this.manager.getNextFile(currentFile);
        if (!currentFile.equals(file)) {
          position = 0L;
        } else {
          position = Long.parseLong(msg.getRef().replaceAll("[^:]*:",  "").replaceAll(":.*", ""));
        }
      } else if (DatalogMessageType.TSEEK.equals(msg.getType())) {
        typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.TSEEK.name());
        Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_FEEDER_MESSAGES_IN, typeLabels, 1);
        String hexts = new String(Hex.encode(Longs.toByteArray(msg.getSeekts())), StandardCharsets.US_ASCII);
        //System.out.println("CHECKING " + hexts);
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

      //System.out.println("SEEK "+ msg + " >>> " + currentFile);

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

          long len = 0;

          try {
            len = fs.getFileStatus(path).getLen();
          } catch (FileNotFoundException fnfe) {
            // This could happen when the file was cycled between the exists test above and the check
            previousFile = currentFile;
            currentFile = this.manager.getNextFile(currentFile);
            newfile = true;
            position = 0;
            LockSupport.parkNanos(STANDARD_WAIT);
            continue;
          }

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
            //System.out.println("READ " + count + " FROM " + previousFile + " >>> " + currentFile);
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

          limit = size >= MAX_INFLIGHT_SIZE;

          long batch = count;

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
            // We leave any more specific sharding to the consumer side. Allowing the execution of
            // a consumer provided macro in the feeder should be considered an elevated security risk and therefore avoided.
            //

            if (hasShards) {
              byte[] k = key.getBytes();
              long classId = DatalogHelper.bytesToLong(k, 8, 8);
              long labelsId = DatalogHelper.bytesToLong(k, 16, 8);

              long sid = DatalogHelper.getShardId(classId, labelsId, shardShift);

              // Now check all shards until one matches
              boolean matched = false;

              for (int i = 0; i < modulus.length; i++) {
                if (0 != modulus[i] && remainder[i] == sid % modulus[i]) {
                  matched = true;
                  break;
                }
              }
              if (!matched) {
                continue;
              }
            }

            if (null != excluded) {
              DatalogRecord record = new DatalogRecord();
              DatalogHelper.deserialize(val.getBytes(), 0, val.getLength(), record);

              // Ignore the record if it contains the id which created this record
              if (excluded.contains(record.getId())) {
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

            bytes = DatalogHelper.serialize(msg);
            if (encrypt) {
              bytes = CryptoHelper.wrapBlob(aesKey, bytes);
            }

            DatalogHelper.writeLong(out, bytes.length, 4);
            out.write(bytes);
            out.flush();

            //System.out.println("SENDING " + msg.getCommitref());
            if (size >= MAX_INFLIGHT_SIZE) {
              limit = true;
            }

            count++;
          }

          typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.DATA.name());
          Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_FEEDER_MESSAGES_OUT, typeLabels, count - batch);

          //
          // If we voluntarily exited the loop due to the inflight limit being reached, wait for a
          // message from our peer, either SEEK/TSEEK or COMMIT
          // Also wait for such a message if there are inflight messages and there is input to read on the socket
          //

          if (limit || (inflight.size() > 0 && in.available() > 0)) {
            //System.out.println("PAUSED AFTER " + size + " BYTES.");
            bytes = DatalogHelper.readBlob(in, 0);
            if (encrypt) {
              bytes = CryptoHelper.unwrapBlob(aesKey, bytes);
            }
            msg.clear();
            DatalogHelper.deserialize(bytes, msg);

            if (DatalogMessageType.COMMIT == msg.getType()) {
              typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.COMMIT.name());
              Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_FEEDER_MESSAGES_IN, typeLabels, 1);

              // Find the index of the commit ref in the inflight list
              int idx = inflight.indexOf(msg.getRef());

              if (-1 == idx) {
                LOG.error("Invalid commit ref '" + msg.getRef() + "'.");
                return;
              }

              // Commit all messages up to 'ref', updating 'size' on the fly
              for (int i = 0; i <= idx; i++) {
                size -= Long.parseLong(inflight.remove(0).replaceAll(".*:",""));
              }
              //System.out.println("FEEDER INFLIGHT=" + inflight.size());
            } else if (DatalogMessageType.SEEK == msg.getType()) {
              typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.SEEK.name());
              Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_FEEDER_MESSAGES_IN, typeLabels, 1);

              currentFile = msg.getRef().replaceAll(":.*","") + FileBasedDatalogManager.SUFFIX;
              String file = this.manager.getNextFile(currentFile);
              if (!currentFile.equals(file)) {
                position = 0L;
              } else {
                position = Long.parseLong(msg.getRef().replaceAll("[^:]*:",  "").replaceAll(":.*", ""));
              }
              inflight.clear();
              size = 0;
            } else if (DatalogMessageType.TSEEK == msg.getType()) {
              typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.TSEEK.name());
              Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_FEEDER_MESSAGES_IN, typeLabels, 1);

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
        } catch (EOFException|FileNotFoundException e) {
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
    } catch (WarpScriptException wse) {
      LOG.error("Error executing WarpScript.", wse);
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
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
