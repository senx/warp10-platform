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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geoxp.oss.CryptoHelper;
import com.google.common.primitives.Longs;

import io.warp10.WarpConfig;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.DatalogMessage;
import io.warp10.continuum.store.thrift.data.DatalogMessageType;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
import io.warp10.continuum.store.thrift.data.DatalogRecordType;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.TokenType;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.binary.ADD;
import io.warp10.script.ext.token.TOKENDUMP;
import io.warp10.script.ext.token.TOKENGEN;
import io.warp10.script.functions.ECDH;
import io.warp10.script.functions.ECPRIVATE;
import io.warp10.script.functions.ECPUBLIC;
import io.warp10.script.functions.ECSIGN;
import io.warp10.script.functions.ECVERIFY;
import io.warp10.script.functions.HASH;
import io.warp10.script.functions.REVERSE;
import io.warp10.script.functions.RUN;
import io.warp10.script.functions.TOLONGBYTES;
import io.warp10.sensision.Sensision;

/**
 * Consumes a Datalog feed and updates a backend accordingly
 */
public class TCPDatalogConsumer extends Thread implements DatalogConsumer {

  private static final RUN RUN = new RUN(WarpScriptLib.RUN);

  private WarpScriptStack stack;

  private ECPrivateKey eccPrivate;

  private String id;

  /**
   * Set of IDs whose messages are ignored
   */
  private Set<String> excluded;

  private static final Logger LOG = LoggerFactory.getLogger(TCPDatalogConsumer.class);

  private long[] CLASS_KEYS;
  private long[] LABELS_KEYS;

  private String feeder;

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

  /**
   * Path to the file where last committed offset is kept track of
   */
  private File offsetFile;

  /**
   * Delay (in ms) between offset commits to offsetFile. 0 means write every commit.
   */
  private long offsetDelay;
  private long lastsync = 0;

  private String suffix;

  @Override
  public void run() {

    String host = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_FEEDER_HOST + suffix, TCPDatalogFeeder.DEFAULT_HOST);
    int port = Integer.parseInt(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_FEEDER_PORT + suffix, TCPDatalogFeeder.DEFAULT_PORT));

    String feederShardsSpec = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_FEEDER_SHARDS + suffix);

    long[] feederShards = null;
    int feederShardShift = Integer.parseInt(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_FEEDER_SHARDSHIFT + suffix, "48"));

    if (null != feederShardsSpec) {
      String[] tokens = feederShardsSpec.split(",");
      feederShards = new long[tokens.length];
      for (int i = 0; i < tokens.length; i++) {
        String token = tokens[i];
        String[] subtokens = token.trim().split(":");
        if (2 != subtokens.length) {
          throw new RuntimeException("Invalid feeder shard spec " + token + " in " + FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_FEEDER_SHARDS + suffix);
        }
        feederShards[i] = (Long.parseLong(subtokens[0]) << 32) | (Long.parseLong(subtokens[1]) & 0xFFFFFFFFL);
      }
    }

    long[] modulus = null;
    long[] remainder = null;

    String shardSpec = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_SHARDS + suffix);

    boolean hasShards = false;
    int shardShift = Integer.parseInt(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_SHARDSHIFT + suffix, "48"));

    if (null != shardSpec) {
      hasShards = true;
      String[] tokens = feederShardsSpec.split(",");
      modulus = new long[tokens.length];
      remainder = new long[tokens.length];
      for (int i = 0; i < tokens.length; i++) {
        String token = tokens[i];
        String[] subtokens = token.trim().split(":");
        if (2 != subtokens.length) {
          throw new RuntimeException("Invalid shard spec " + token + " in " + FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_SHARDS + suffix);
        }
        modulus[i] = Long.parseLong(subtokens[0]);
        remainder[i] = Long.parseLong(subtokens[1]);
      }
    }

    String macro = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_MACRO + suffix);
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    boolean macroData = "true".equals(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_MACRO_DATA + suffix));
    boolean macroToken = "true".equals(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_MACRO_TOKEN + suffix));

    Map<String,String> labels = new HashMap<String,String>();
    labels.put(SensisionConstants.SENSISION_LABEL_CONSUMER, this.id);
    labels.put(SensisionConstants.SENSISION_LABEL_FEEDER, this.feeder);
    Map<String,String> typeLabels = new LinkedHashMap<String,String>(labels);

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

        typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.WELCOME.name());
        Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_CONSUMER_MESSAGES_IN, typeLabels, 1);

        //
        // Retrieve feeder ECC public key
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
          throw new RuntimeException("Error extracting ECC public key for feeder '" + this.feeder + "'.", wse);
        }

        //
        // Check signature of feeder
        //

        stack.show();
        stack.clear();
        stack.push(Longs.toByteArray(msg.getNonce()));
        stack.push(Longs.toByteArray(msg.getTimestamp()));
        stack.push(msg.getId().getBytes(StandardCharsets.UTF_8));
        ADD ADD = new ADD(WarpScriptLib.ADD);
        ADD.apply(stack);
        ADD.apply(stack);
        stack.push(msg.getSig());
        stack.push("SHA512WITHECDSA");
        stack.push(feederPublic);
        ECVERIFY ECVERIFY = new ECVERIFY(WarpScriptLib.ECVERIFY);
        ECVERIFY.apply(stack);

        if (!Boolean.TRUE.equals(stack.pop())) {
          throw new RuntimeException("Invalid signature from feeder '" + this.feeder + "'.");
        }

        //
        // Compute signature and emit INIT message
        //

        stack.show();
        stack.clear();
        stack.push(Longs.toByteArray(msg.getNonce()));
        stack.push(Longs.toByteArray(msg.getTimestamp()));
        stack.push(this.id.getBytes(StandardCharsets.UTF_8));
        ADD.apply(stack);
        ADD.apply(stack);
        ECSIGN ECSIGN = new ECSIGN(WarpScriptLib.ECSIGN);
        stack.push("SHA512WITHECDSA");
        stack.push(eccPrivate);
        ECSIGN.apply(stack);
        byte[] sig = (byte[]) stack.pop();

        boolean encrypt = msg.isEncrypt();

        //
        // Compute the encryption key using ECDH
        //

        if (encrypt) {
          long nonce = msg.getNonce();
          long timestamp = msg.getTimestamp();

          //
          // Key is
          // ->LONGBYTES(SIPHASH(nonce,timestamp,secret),8)
          // + ->LONGBYTES(SIPHASH(timestamp,nonce,secret),8)
          // + ->LONGBYTES(SIPHASH(nonce,timestamp,CLONEREVERSE(secret)),8)
          // + ->LONGBYTES(SIPHASH(timestamp,nonce,CLONEREVERSE(secret)),8)
          //

          stack.show();
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

        if (null != feederShards) {
          for (long shard: feederShards) {
            msg.addToShards(shard);
          }
          msg.setShardShift(feederShardShift);
        }

        if (null != excluded) {
          for (String ex: excluded) {
            msg.addToExcluded(ex);
          }
        }

        bytes = DatalogHelper.serialize(msg);
        // We do not encrypt the init message since the feeder does not yet know who we
        // are
        DatalogHelper.writeLong(out, bytes.length, 4);
        out.write(bytes);
        out.flush();

        typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.INIT.name());
        Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_CONSUMER_MESSAGES_OUT, typeLabels, 1);

        //
        // Now emit a seek message
        //

        msg.clear();
        if (this.offsetFile.exists()) {
          String seek = FileUtils.readFileToString(offsetFile, StandardCharsets.UTF_8).replaceAll("\n.*","").trim();
          // if seek is a number, issue a TSEEK message, otherwise a seek one
          try {
            long ts = Long.parseLong(seek);
            msg.setType(DatalogMessageType.TSEEK);
            msg.setSeekts(ts);
            typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.TSEEK.name());
          } catch (NumberFormatException nfe) {
            msg.setType(DatalogMessageType.SEEK);
            msg.setRef(seek);
            typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.SEEK.name());
          }
        } else {
          // If the offset file does not exist, start the feed at the current time
          msg.setType(DatalogMessageType.TSEEK);
          msg.setSeekts(System.currentTimeMillis());
          typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.TSEEK.name());
        }

        bytes = DatalogHelper.serialize(msg);
        if (encrypt) {
          bytes = CryptoHelper.wrapBlob(AES_KEY, bytes);
        }
        DatalogHelper.writeLong(out, bytes.length, 4);
        out.write(bytes);
        out.flush();

        Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_CONSUMER_MESSAGES_OUT, typeLabels, 1);

        //
        // Now retrieve the DATA messages and push them to a worker
        //

        DatalogRecord record = null;

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
                typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.SEEK.name());
                Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_CONSUMER_MESSAGES_OUT, typeLabels, 1);

                inflight.clear();
                failed.clear();
                successful.clear();
              } else if (idx >= inflight.size()) {
                // idx > inflight.size(), so all refs were successfully handled
                // Send COMMIT message for last ref in infligt

                msg.clear();
                msg.setType(DatalogMessageType.COMMIT);
                msg.setRef(inflight.get(inflight.size() - 1));
                syncCommit(msg.getRef());
                bytes = DatalogHelper.serialize(msg);
                if (encrypt) {
                  bytes = CryptoHelper.wrapBlob(AES_KEY, bytes);
                }
                DatalogHelper.writeLong(out, bytes.length, 4);
                out.write(bytes);
                out.flush();
                typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.COMMIT.name());
                Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_CONSUMER_MESSAGES_OUT, typeLabels, 1);
                inflight.clear();
                failed.clear();
                successful.clear();

              } else {
                // some refs failed and some succeeded
                // Send COMMIT message for last successful ref
                msg.clear();
                msg.setType(DatalogMessageType.COMMIT);
                msg.setRef(inflight.get(idx));
                syncCommit(msg.getRef());
                bytes = DatalogHelper.serialize(msg);
                if (encrypt) {
                  bytes = CryptoHelper.wrapBlob(AES_KEY, bytes);
                }
                DatalogHelper.writeLong(out, bytes.length, 4);
                out.write(bytes);
                out.flush();
                typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.COMMIT.name());
                Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_CONSUMER_MESSAGES_OUT, typeLabels, 1);

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
                typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.SEEK.name());
                Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_CONSUMER_MESSAGES_OUT, typeLabels, 1);

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

          typeLabels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogMessageType.DATA.name());
          Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_CONSUMER_MESSAGES_IN, typeLabels, 1);

          //
          // Extract the DatalogRecord
          //

          record = new DatalogRecord();
          DatalogHelper.deserialize(msg.getRecord(), record);

          //
          // If our id or an exluded one created this message, ignore it, it would otherwise create a loop
          //

          if (id.equals(record.getId()) || this.excluded.contains(record.getId())) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_IGNORED, labels, 1);
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
          // Check shards if needed
          //

          if (hasShards) {
            // compute shard id
            long sid = DatalogHelper.getShardId(classid, labelsid, shardShift);

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

          //
          // Execute the filtering macro if set.
          // If 'datalog.consumer.macro.data' is false, the macro is
          // expected to return a boolean which, if true, will
          // accept the message.
          // The macro is fed with a GTS Encoder with the metadata
          // of the GTS subject of the message and a STRING with
          // the type of message (From DatalogRecordType, UPDATE, DELETE, REGISTER, UNREGISTER).
          // If 'datalog.consumer.macro.data' is true, the macro
          // is fed with an encoder as above but with the actual data included for UPDATE messages.
          // The macro will then have the ability to alter the encoder to update metadata
          // and/or data. It is then expected to return a boolean or an encoder.
          //
          // The macro is called in the consumer thread, not in the worker threads so metadata alterations
          // are reflected in the choice of the worker to respect the sequence of messages per GTS
          //

          if (null != macro) {
            stack.show();
            stack.clear();
            Map<String,Object> tokenMap = null;

            if (macroToken && null != record.getToken()) {
              tokenMap = TOKENDUMP.mapFromToken(record.getToken());
            }

            if (!macroData) {
              GTSEncoder encoder = new GTSEncoder(0L);
              encoder.setMetadata(record.getMetadata());

              stack.push(tokenMap);
              stack.push(encoder);
              stack.push(record.getType().name());
              stack.push(macro);
              RUN.apply(stack);
              if (0 == stack.depth() || !Boolean.TRUE.equals(stack.peek())) {
                stack.show();
                stack.clear();
                Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_SKIPPED, labels, 1);
                continue;
              }
            } else {
              GTSEncoder encoder;

              if (DatalogRecordType.UPDATE.equals(record.getType())) {
                GTSDecoder decoder = new GTSDecoder(record.getBaseTimestamp(), record.bufferForEncoder());
                decoder.next();
                encoder = decoder.getEncoder();
              } else {
                encoder = new GTSEncoder(0L);
              }
              encoder.setMetadata(record.getMetadata());

              // Call the macro with the token, the encoder and the record type
              stack.push(tokenMap);
              stack.push(encoder);
              stack.push(record.getType().name());
              stack.push(macro);
              RUN.apply(stack);

              // Skip if the macro did not return a boolean TRUE
              boolean skip = !(stack.depth() >= 1 && Boolean.TRUE.equals(stack.peek()));

              boolean updateIds = false;

              if (!skip) {
                // Discard the boolean
                stack.pop();

                // The macro returned a GTS or an ENCODER, update the record with the
                // metadata and the content (for UPDATE records)
                GTSEncoder forwardEncoder = null;
                if (stack.depth() > 0) {
                  if (stack.peek() instanceof GeoTimeSerie) {
                    GeoTimeSerie gts = (GeoTimeSerie) stack.pop();
                    encoder = new GTSEncoder(0L);
                    encoder.setMetadata(gts.getMetadata());
                    encoder.encode(gts);
                    updateIds = true;
                  } else if (stack.peek() instanceof GTSEncoder) {
                    encoder = (GTSEncoder) stack.pop();
                    updateIds = true;
                  } else if (stack.peek() instanceof List) {
                    // The macro returned a list of two elements, either GTS or ENCODER. The first one is the
                    // content to use for the action (UPDATE/REGISTER/UNREGISTER/DELETE), the second one is
                    // the content to forward.
                    List<Object> l = (List<Object>) stack.pop();
                    if (2 == l.size()) {
                      Object update = l.get(0);
                      Object forward = l.get(1);

                      if (update instanceof GeoTimeSerie) {
                        GeoTimeSerie gts = (GeoTimeSerie) update;
                        encoder = new GTSEncoder(0L);
                        encoder.setMetadata(gts.getMetadata());
                        encoder.encode(gts);
                        updateIds = true;
                      } else if (update instanceof GTSEncoder) {
                        encoder = (GTSEncoder) update;
                        updateIds = true;
                      } else {
                        skip = true;
                      }

                      // Create 'forward' wrapper
                      if (forward instanceof GeoTimeSerie) {
                        GeoTimeSerie gts = (GeoTimeSerie) forward;
                        forwardEncoder = new GTSEncoder(0L);
                        forwardEncoder.setMetadata(gts.getMetadata());
                        forwardEncoder.encode(gts);
                        updateIds = true;
                      } else if (forward instanceof GTSEncoder) {
                        forwardEncoder = (GTSEncoder) forward;
                        updateIds = true;
                      } else {
                        skip = true;
                      }
                    } else {
                      skip = true;
                    }

                    // Copy the forwardEncoder as a wrapper in the record. This will trigger the storing of this
                    // encoder instead of the one in 'encoder' but will forward the latter if forwarding should happen.
                    if (null != forwardEncoder) {
                      GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(forwardEncoder, false);
                      record.setForward(wrapper);
                    }
                  } else {
                    skip = true;
                  }

                  if (!skip) {
                    // Copy the encoder
                    record.setMetadata(encoder.getMetadata());
                    if (DatalogRecordType.UPDATE.equals(record.getType())) {
                      record.setBaseTimestamp(encoder.getBaseTimestamp());
                      record.setEncoder(encoder.getBytes());
                    }

                    // If there is a map left on the stack, update the token
                    if (stack.depth() > 0 && stack.peek() instanceof Map) {
                      tokenMap = (Map<String,Object>) stack.pop();
                      // force token type
                      tokenMap.put(TOKENGEN.KEY_TYPE, TokenType.WRITE.toString());
                      record.setToken((WriteToken) TOKENGEN.tokenFromMap(tokenMap, "Datalog", Long.MAX_VALUE));
                    }
                    updateIds = true;
                  }
                }
              }

              if (skip) {
                stack.show();
                stack.clear();
                Sensision.update(SensisionConstants.SENSISION_CLASS_DATALOG_SKIPPED, labels, 1);
                continue;
              }

              //
              // Update the class and label ids if metadata were possibly changed.
              // This is so the feeders later have a valid set of ids to use for sharding.
              //

              if (updateIds) {
                long cid = GTSHelper.classId(CLASS_KEYS, record.getMetadata().getName());
                long lid = GTSHelper.labelsId(LABELS_KEYS, record.getMetadata().getLabels());

                record.getMetadata().setClassId(cid);
                record.getMetadata().setLabelsId(lid);

                if (record.isSetForward()) {
                  cid = GTSHelper.classId(CLASS_KEYS, record.getForward().getMetadata().getName());
                  lid = GTSHelper.labelsId(LABELS_KEYS, record.getForward().getMetadata().getLabels());

                  record.getForward().getMetadata().setClassId(cid);
                  record.getForward().getMetadata().setLabelsId(lid);
                }
              }
            }
            stack.show();
            stack.clear();
          }

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
    this.feeder = name;
    this.suffix = "." + name;

    this.CLASS_KEYS = SipHashInline.getKey(ks.getKey(KeyStore.SIPHASH_CLASS));
    this.LABELS_KEYS = SipHashInline.getKey(ks.getKey(KeyStore.SIPHASH_LABELS));

    this.stack = new MemoryWarpScriptStack(null, null);

    //
    // Extract ECC private/public key
    //

    String eccpri = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_ECC_PRIVATE + suffix);

    if (null == eccpri) {
      throw new RuntimeException("Missing ECC private key.");
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

    if (null == WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_OFFSETFILE + suffix)) {
      throw new RuntimeException("Missing offset file '" + FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_OFFSETFILE + suffix + "'.");
    }

    this.offsetFile = new File(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_OFFSETFILE + suffix));

    this.offsetDelay = Long.parseLong(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_OFFSETDELAY + suffix, "0"));

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

  private void syncCommit(String ref) throws IOException {
    long now = System.currentTimeMillis();
    if (now - lastsync >= offsetDelay) {
      //
      // We must ensure that the workers have performed a flush first
      //
      DatalogWorkers.offer(this, ref, null);
      FileUtils.write(offsetFile, ref, StandardCharsets.UTF_8, false);
      lastsync = now;
    }
  }
}
