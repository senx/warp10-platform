//
//   Copyright 2018-2023  SenX S.A.S.
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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.google.common.collect.MapMaker;

import io.warp10.BytesUtils;
import io.warp10.SmartPattern;
import io.warp10.ThriftUtils;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.DirectoryUtil;
import io.warp10.continuum.egress.ThriftDirectoryClient;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.Directory;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsResponse;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.fdb.FDBClear;
import io.warp10.fdb.FDBContext;
import io.warp10.fdb.FDBKVScanner;
import io.warp10.fdb.FDBMutation;
import io.warp10.fdb.FDBScan;
import io.warp10.fdb.FDBSet;
import io.warp10.fdb.FDBUtils;
import io.warp10.sensision.Sensision;

public class StandaloneDirectoryClient implements DirectoryClient {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneDirectoryClient.class);

  private static final String DIRECTORY_INIT_NTHREADS_DEFAULT = "4";

  private static final byte[] METADATA_PREFIX = "M".getBytes(StandardCharsets.US_ASCII);

  private static final int MAX_BATCH_SIZE = 500000;

  //
  // FoundationDB related fields
  //

  private final FDBContext fdbContext;
  private final long fdbRetryLimit;

  private Database fdb;
  private final boolean useFDB;

  private final DB db;
  private final KeyStore keystore;

  private final byte[] classKey;
  private final byte[] labelsKey;

  private final long[] classLongs;
  private final long[] labelsLongs;

  private final byte[] aesKey;

  private final int initNThreads;

  private final boolean syncwrites;
  private final double syncrate;

  private long LIMIT_CLASS_CARDINALITY = 100;
  private long LIMIT_LABELS_CARDINALITY = 100;

  private static final Map<String,Long> classids = new ConcurrentHashMap<String,Long>();


  private static final Comparator<String> CLASS_COMPARATOR = new Comparator<String>() {
    @Override
    public int compare(String o1, String o2) {
      Long id1 = classids.get(o1);
      Long id2 = classids.get(o2);

      // A key may be missing during a find if a concurrent unregister is done.
      if (null == id1) {
        if (null == id2) {
          return 0;
        }
        return -1;
      } else if (null == id2) {
        return 1;
      } else {
        return Directory.ID_COMPARATOR.compare(id1, id2);
      }
    }
  };

  /**
   * Maps of class name to labelsId to metadata
   */
  // 128BITS
  private static final Map<String,Map<Long,Metadata>> metadatas = new ConcurrentSkipListMap<String,Map<Long,Metadata>>(CLASS_COMPARATOR);

  private static final Map<BigInteger,Metadata> metadatasById = new MapMaker().concurrencyLevel(64).makeMap();

  private long activityWindow = 0L;

  public static interface ShardFilter {
    public boolean exclude(long classId, long labelsId);
  }

  public StandaloneDirectoryClient() {
    this.db = null;
    this.keystore = null;
    this.classKey = null;
    this.labelsKey = null;
    this.classLongs = null;
    this.labelsLongs = null;
    this.aesKey = null;
    this.initNThreads = 0;
    this.syncwrites = false;
    this.syncrate = 0.0F;
    this.fdbContext = null;
    this.fdbRetryLimit = 0;
    this.useFDB = false;
  }

  public StandaloneDirectoryClient(Object db, final KeyStore keystore) {

    String classMaxCardinalityProp = WarpConfig.getProperty(Configuration.DIRECTORY_STATS_CLASS_MAXCARDINALITY);
    if (null != classMaxCardinalityProp) {
      this.LIMIT_CLASS_CARDINALITY = Long.parseLong(classMaxCardinalityProp);
    }

    String labelsMaxCardinalityProp = WarpConfig.getProperty(Configuration.DIRECTORY_STATS_LABELS_MAXCARDINALITY);
    if (null != labelsMaxCardinalityProp) {
      this.LIMIT_LABELS_CARDINALITY = Long.parseLong(labelsMaxCardinalityProp);
    }

    this.activityWindow = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_ACTIVITY_WINDOW, "0"));

    this.initNThreads = Integer.parseInt(WarpConfig.getProperty(Configuration.DIRECTORY_INIT_NTHREADS, DIRECTORY_INIT_NTHREADS_DEFAULT));

    if (db instanceof DB) {
      this.db = (DB) db;
      this.fdbContext = null;
      this.fdbRetryLimit = 0L;
      this.useFDB = false;
    } else if (db instanceof FDBContext) {
      this.db = null;
      this.fdbContext = (FDBContext) db;
      this.fdbRetryLimit = Long.parseLong(WarpConfig.getProperty(io.warp10.continuum.Configuration.DIRECTORY_FDB_RETRYLIMIT, Directory.DEFAULT_FDB_RETRYLIMIT));
      this.useFDB = true;
    } else if (null == db) {
      this.db = null;
      this.fdbContext = null;
      this.fdbRetryLimit = 0L;
      this.useFDB = false;
    } else {
      throw new RuntimeException("Invalid DB specification.");
    }

    this.keystore = keystore;

    this.aesKey = this.keystore.getKey(KeyStore.AES_LEVELDB_METADATA);
    this.classKey = this.keystore.getKey(KeyStore.SIPHASH_CLASS);
    this.classLongs = SipHashInline.getKey(this.classKey);

    this.labelsKey = this.keystore.getKey(KeyStore.SIPHASH_LABELS);
    this.labelsLongs = SipHashInline.getKey(this.labelsKey);

    syncrate = Math.min(1.0D, Math.max(0.0D, Double.parseDouble(WarpConfig.getProperty(Configuration.LEVELDB_DIRECTORY_SYNCRATE, "1.0"))));
    syncwrites = 0.0 < syncrate && syncrate < 1.0;

    //
    // Read metadata from DB
    //

    if (null == this.db && null == this.fdbContext) {
      return;
    }

    Iterator iter = null;

    if (null != this.db) {
      DBIterator dbiter = this.db.iterator();
      dbiter.seek(METADATA_PREFIX);
      iter = dbiter;
    } else {
      this.fdb = this.fdbContext.getDatabase();
      FDBScan scan = new FDBScan();
      try {
        scan.setTenantPrefix(fdbContext.getTenantPrefix());
        scan.setStartKey(Constants.FDB_METADATA_KEY_PREFIX);
        scan.setEndKey(FDBUtils.getNextKey(Constants.FDB_METADATA_KEY_PREFIX));
        scan.setReverse(false);
        iter = scan.getScanner(fdbContext, this.fdb, StreamingMode.WANT_ALL);
      } catch (IOException ioe) {
        throw new RuntimeException("Error while initializing FDB scanner.");
      }
    }

    byte[] stop = "N".getBytes(StandardCharsets.US_ASCII);

    long count = 0;

    Thread[] initThreads = new Thread[this.initNThreads];
    final AtomicBoolean[] stopMarkers = new AtomicBoolean[this.initNThreads];
    final LinkedBlockingQueue<Entry<byte[],byte[]>> resultQ = new LinkedBlockingQueue<Entry<byte[],byte[]>>(initThreads.length * 8192);

    for (int i = 0; i < initThreads.length; i++) {
      stopMarkers[i] = new AtomicBoolean(false);
      final AtomicBoolean stopMe = stopMarkers[i];
      initThreads[i] = new Thread(new Runnable() {
        @Override
        public void run() {

          byte[] bytes = new byte[16];

          AESWrapEngine engine = null;
          PKCS7Padding padding = null;

          if (null != keystore.getKey(KeyStore.AES_LEVELDB_METADATA)) {
            engine = new AESWrapEngine();
            CipherParameters params = new KeyParameter(keystore.getKey(KeyStore.AES_LEVELDB_METADATA));
            engine.init(false, params);

            padding = new PKCS7Padding();
          }

          TDeserializer deserializer = ThriftUtils.getTDeserializer(new TCompactProtocol.Factory());

          while (!stopMe.get()) {
            try {

              Entry<byte[],byte[]> result = resultQ.poll(100, TimeUnit.MILLISECONDS);

              if (null == result) {
                continue;
              }

              byte[] key = result.getKey();
              byte[] value = result.getValue();

              //
              // Unwrap
              //

              byte[] unwrapped = null != engine ? engine.unwrap(value, 0, value.length) : value;

              //
              // Unpad
              //

              int padcount = null != padding ? padding.padCount(unwrapped) : 0;
              byte[] unpadded = null != padding ? Arrays.copyOf(unwrapped, unwrapped.length - padcount) : unwrapped;

              //
              // Deserialize
              //

              Metadata metadata = new Metadata();
              deserializer.deserialize(metadata, unpadded);

              String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
              Map<String,String> sensisionLabels = new HashMap<String,String>();
              sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);

              //
              // Compute classId/labelsId and compare it to the values in the row key
              //

              // 128BITS
              long classId = GTSHelper.classId(classLongs, metadata.getName());
              long labelsId = GTSHelper.labelsId(labelsLongs, metadata.getLabels());

              ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
              bb.position(1);
              long hbClassId = bb.getLong();
              long hbLabelsId = bb.getLong();

              // If classId/labelsId are incoherent, skip metadata
              if (classId != hbClassId || labelsId != hbLabelsId) {
                LOG.error("Incoherent class/labels Id for " + metadata);
                continue;
              }

              // 128BITS
              metadata.setClassId(classId);
              metadata.setLabelsId(labelsId);

              if (!metadata.isSetAttributes()) {
                metadata.setAttributes(new HashMap<String,String>());
              }

              //
              // Internalize Strings
              //

              GTSHelper.internalizeStrings(metadata);

              Map<Long, Metadata> metadatasForClassName;
              synchronized(metadatas) {
                if (!classids.containsKey(metadata.getName())) {
                  classids.put(metadata.getName(), metadata.getClassId());
                  metadatasForClassName = new ConcurrentSkipListMap<Long, Metadata>(Directory.ID_COMPARATOR);
                  metadatas.put(metadata.getName(), metadatasForClassName);
                } else {
                  metadatasForClassName = metadatas.get(metadata.getName());
                }
              }

              synchronized(metadatasForClassName) {
                if (!metadatasForClassName.containsKey(labelsId)) {
                  metadatasForClassName.put(labelsId, metadata);

                  //
                  // Store Metadata under 'id'
                  //
                  // 128BITS
                  GTSHelper.fillGTSIds(bytes, 0, classId, labelsId);
                  BigInteger id = new BigInteger(bytes);
                  metadatasById.put(id, metadata);

                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, 1);

                  continue;
                }
              }

              LOG.error("Duplicate labelsId for classId " + classId + ": " + metadata);
              continue;

            } catch (InvalidCipherTextException icte) {
              throw new RuntimeException(icte);
            } catch (TException te) {
              throw new RuntimeException(te);
            } catch (InterruptedException ie) {

            }

          }
        }
      });

      initThreads[i].setDaemon(true);
      initThreads[i].setName("[Directory initializer #" + i + "]");
      initThreads[i].start();
    }

    try {

      long nano = System.nanoTime();

      while(iter.hasNext()) {
        Entry<byte[],byte[]> kv = null;

        if (!this.useFDB) {
          kv = (Entry<byte[], byte[]>) iter.next();
        } else {
          kv = ((FDBKVScanner) iter).next();
        }

        byte[] key = kv.getKey();
        if (BytesUtils.compareTo(key, stop) >= 0) {
          break;
        }

        boolean interrupted = true;

        while(interrupted) {
          interrupted = false;
          try {
            resultQ.put(kv);
            count++;
            if (0 == count % 1000) {
              Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, count);
            }
          } catch (InterruptedException ie) {
            interrupted = true;
          }
        }
      }

      //
      // Wait until resultQ is empty
      //

      while(!resultQ.isEmpty()) {
        try { Thread.sleep(100L); } catch (InterruptedException ie) {}
      }

      //
      // Notify the init threads to stop
      //

      for (int i = 0; i < initNThreads; i++) {
        stopMarkers[i].set(true);
      }

      nano = System.nanoTime() - nano;

      LOG.info("Loaded " + count + " GTS in " + (nano / 1000000.0D) + " ms");
    } finally {
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, count);
      try {
        if (this.useFDB) {
          ((FDBKVScanner) iter).close();
        } else {
          ((DBIterator) iter).close();
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<Metadata> find(DirectoryRequest request) {

    List<String> classExpr = request.getClassSelectors();
    List<Map<String,String>> labelsExpr = request.getLabelsSelectors();

    boolean hasActiveAfter = request.isSetActiveAfter();
    long activeAfter = request.getActiveAfter();

    boolean hasQuietAfter = request.isSetQuietAfter();
    long quietAfter = request.getQuietAfter();

    //
    // Build patterns from expressions
    //

    SmartPattern classSmartPattern;

    Collection<Metadata> requestedMetadatas;

    if (classExpr.size() > 1) {
      requestedMetadatas = new LinkedHashSet<Metadata>();
    } else {
      requestedMetadatas = new ArrayList<Metadata>();
    }

    Set<String> classNames = null;
    List<String> missingLabels = Constants.ABSENT_LABEL_SUPPORT ? new ArrayList<String>() : null;

    for (int i = 0; i < classExpr.size(); i++) {

      String exactClassName = null;

      if (classExpr.get(i).startsWith("=") || !classExpr.get(i).startsWith("~")) {
        exactClassName = classExpr.get(i).startsWith("=") ? classExpr.get(i).substring(1) : classExpr.get(i);
        classSmartPattern = new SmartPattern(exactClassName);
      } else {
        classSmartPattern = new SmartPattern(Pattern.compile(classExpr.get(i).substring(1)));
      }

      Map<String,SmartPattern> labelPatterns = new LinkedHashMap<String,SmartPattern>();

      if (null != missingLabels) {
        missingLabels.clear();
      }

      if (null != labelsExpr.get(i)) {
        for (Entry<String,String> entry: labelsExpr.get(i).entrySet()) {
          String label = entry.getKey();
          String expr = entry.getValue();
          Pattern pattern;

          if (null != missingLabels && ("=".equals(expr) || "".equals(expr))) {
            missingLabels.add(label);
            continue;
          }

          if (expr.startsWith("=") || !expr.startsWith("~")) {
            labelPatterns.put(label, new SmartPattern(expr.startsWith("=") ? expr.substring(1) : expr));
          } else {
            pattern = Pattern.compile(expr.substring(1));
            labelPatterns.put(label,  new SmartPattern(pattern));
          }
        }
      }

      if (null != exactClassName) {
        if (!classids.containsKey(exactClassName)) {
          continue;
        }
        classNames = new LinkedHashSet<String>();
        classNames.add(exactClassName);
      } else {
        classNames = metadatas.keySet();
      }

      //
      // Create arrays to check the labels, this is to speed up discard
      //

      List<String> labelNames = new ArrayList<String>(labelPatterns.size());
      List<SmartPattern> labelSmartPatterns = new ArrayList<SmartPattern>(labelPatterns.size());
      String[] labelValues = null;

      for(Entry<String,SmartPattern> entry: labelPatterns.entrySet()) {
        labelNames.add(entry.getKey());
        labelSmartPatterns.add(entry.getValue());
      }

      labelValues = new String[labelNames.size()];

      //
      // Loop over the class names to find matches
      //

      for (String className: classNames) {

        //
        // If class matches, check all labels for matches
        //

        if (classSmartPattern.matches(className)) {
          Map<Long, Metadata> metadatasForClassname = metadatas.get(className);

          // Check for nullity because of possible concurrent unregistration.
          if(null != metadatasForClassname) {
            for (Metadata metadata: metadatasForClassname.values()) {

              //
              // Check activity
              //

              if (hasActiveAfter && metadata.getLastActivity() < activeAfter) {
                continue;
              }

              if (hasQuietAfter && metadata.getLastActivity() >= quietAfter) {
                continue;
              }


              boolean exclude = false;

              if (null != missingLabels) {
                for (String missing: missingLabels) {
                  // If the Metadata contain one of the missing labels, exclude the entry
                  if (null != metadata.getLabels().get(missing)) {
                    exclude = true;
                    break;
                  }
                }
                // Check attributes
                if (!exclude && metadata.getAttributesSize() > 0) {
                  for (String missing: missingLabels) {
                    // If the Metadata contain one of the missing labels, exclude the entry
                    if (null != metadata.getAttributes().get(missing)) {
                      exclude = true;
                      break;
                    }
                  }
                }
                if (exclude) {
                  continue;
                }
              }

              int idx = 0;

              for (String labelName: labelNames) {
                //
                // Immediately exclude metadata which do not contain one of the
                // labels for which we have patterns either in labels or in attributes
                //

                String labelValue = metadata.getLabels().get(labelName);

                if (null == labelValue) {
                  labelValue = metadata.getAttributes().get(labelName);
                  if (null == labelValue) {
                    exclude = true;
                    break;
                  }
                }

                labelValues[idx++] = labelValue;
              }

              // If we did not collect enough label/attribute values, exclude the GTS
              if (idx < labelNames.size()) {
                exclude = true;
              }

              if (exclude) {
                continue;
              }

              //
              // Check if the label value matches, if not, exclude the GTS
              //

              for (int j = 0; j < labelNames.size(); j++) {
                if (!labelSmartPatterns.get(j).matches(labelValues[j])) {
                  exclude = true;
                  break;
                }
              }

              if (exclude) {
                continue;
              }

              //
              // We have a match, rebuild metadata
              //
              // FIXME(hbs): include a 'safe' mode to expose the internal Metadata instances?
              //

              Metadata meta = new Metadata();
              meta.setName(className);
              meta.setLabels(Collections.unmodifiableMap(metadata.getLabels()));
              meta.setAttributes(Collections.unmodifiableMap(metadata.getAttributes()));
              // 128BITS
              if (metadata.isSetClassId()) {
                meta.setClassId(metadata.getClassId());
              } else {
                meta.setClassId(GTSHelper.classId(classKey, meta.getName()));
              }
              if (metadata.isSetLabelsId()) {
                meta.setLabelsId(metadata.getLabelsId());
              } else {
                meta.setLabelsId(GTSHelper.labelsId(labelsKey, meta.getLabels()));
              }

              meta.setLastActivity(metadata.getLastActivity());
              requestedMetadatas.add(meta);
            }
          }
        }
      }
    }

    List<Metadata> metas = null;
    if (requestedMetadatas instanceof List) {
      metas = (List<Metadata>) requestedMetadatas;
    } else {
      metas = new ArrayList<Metadata>(requestedMetadatas);
    }
    return metas;
  };

  public boolean register(Metadata metadata) throws IOException {

    boolean stored = false;

    //
    // Special case of null means flush leveldb/fdb
    //

    if (null == metadata) {
      store(null, null);
      stored = true;
      return stored;
    }

    //
    // If the metadata are not known, register them
    //

    if (Configuration.INGRESS_METADATA_SOURCE.equals(metadata.getSource())) {
      Map<Long, Metadata> metadatasForClassname = metadatas.get(metadata.getName());

      if (null == metadatasForClassname) {
        store(metadata);
        stored = true;
      } else {
        // Compute labelsId
        // 128BITS
        long labelsId = GTSHelper.labelsId(this.labelsLongs, metadata.getLabels());

        if (!metadatasForClassname.containsKey(labelsId)) {
          // Metadata is unknown so we know the Metadata should be stored
          store(metadata);
        } else {
          // Check that we do not have a collision
          if (!metadatasForClassname.get(labelsId).getLabels().equals(metadata.getLabels())) {
            LOG.warn("LabelsId collision under class '" + metadata.getName() + "' " + metadata.getLabels() + " and " + metadatas.get(metadata.getName()).get(labelsId).getLabels());
            Sensision.update(SensisionConstants.CLASS_WARP_DIRECTORY_LABELS_COLLISIONS, Sensision.EMPTY_LABELS, 1);
          }

          //
          // Check activity of the GTS, storing it if the activity window has passed
          if (activityWindow > 0) {
            //
            // If the currently stored lastactivity is more than 'activityWindow' before the one in 'metadata',
            // store the metadata
            //
            long currentLastActivity = metadatasForClassname.get(labelsId).getLastActivity();
            if (metadata.getLastActivity() - currentLastActivity >= activityWindow) {
              store(metadata);
            }
          }
        }
      }
    } else {
      //
      // Metadata registration is not from Ingress, this means we can update the value as it comes from the directory service or a metadata update
      //

      // When it is a metadata update request, only store the metadata if the GTS is already known
      if (Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())
          || Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())) {
        Map<Long, Metadata> metadatasForClassname = metadatas.get(metadata.getName());

        if (null != metadatasForClassname) {
          // 128BITS
          long labelsId = GTSHelper.labelsId(this.labelsLongs, metadata.getLabels());
          if (metadatasForClassname.containsKey(labelsId)) {
            // Check the activity so we only increase it
            // 128 bits
            Metadata meta = metadatasForClassname.get(labelsId);
            long currentLastActivity = meta.getLastActivity();
            if (metadata.getLastActivity() < currentLastActivity) {
              metadata.setLastActivity(currentLastActivity);
            }

            store(metadata);
            stored = true;
          }
        }
      } else {
        store(metadata);
        stored = true;
      }
    }

    return stored;
  }

  public void unregister(Metadata metadata) throws IOException {

    if (null == metadata) {
      return;
    }

    // Always compute the labelsId, even if the method early returns before needing it. This is because this operation
    // can be CPU-intensive and if done inside the synchronized(metadatas) block, would block other threads also
    // synchronizing on metadatas. As unregistering unknown metadata should be rare, this is an acceptable compromise.
    // 128BITS
    long labelsId = GTSHelper.labelsId(this.labelsLongs, metadata.getLabels());

    synchronized(metadatas) {
      if (!classids.containsKey(metadata.getName())) {
        return;
      }
      if (!metadatas.get(metadata.getName()).containsKey(labelsId)) {
        return;
      }
      metadatas.get(metadata.getName()).remove(labelsId);
      if (metadatas.get(metadata.getName()).isEmpty()) {
        metadatas.remove(metadata.getName());
        classids.remove(metadata.getName());
      }
    }

    String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
    Map<String,String> sensisionLabels = new HashMap<String,String>();
    sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);

    // 128BITS
    long classId = GTSHelper.classId(this.classLongs, metadata.getName());

    // Remove Metadata indexed by id
    byte[] idbytes = new byte[16];
    GTSHelper.fillGTSIds(idbytes, 0, classId, labelsId);
    metadatasById.remove(new BigInteger(idbytes));

    //
    // Remove entry from DB if need be
    //

    if (null == this.db && null == this.fdb) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, -1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, -1);
      return;
    }


    byte[] bytes = new byte[1 + 8 + 8];
    System.arraycopy(METADATA_PREFIX, 0, bytes, 0, METADATA_PREFIX.length);

    GTSHelper.fillGTSIds(bytes, METADATA_PREFIX.length, classId, labelsId);

    if (useFDB) {
      boolean retry = false;
      long retries = fdbRetryLimit;

      Transaction txn = null;

      do {
        try {
          retry = false;
          txn = this.fdb.createTransaction();
          // Allow RAW access because we may manually force a tenant key prefix without actually setting a tenant
          txn.options().setRawAccess();

          FDBMutation delete = new FDBClear(this.fdbContext.getTenantPrefix(), bytes);
          delete.apply(txn);
          txn.commit().get();
        } catch (Throwable t) {
          FDBUtils.errorMetrics("directory", t.getCause());
          if (t.getCause() instanceof FDBException && ((FDBException) t.getCause()).isRetryable() && retries-- > 0) {
            retry = true;
          } else {
            throw new RuntimeException("Error while commiting to FoundationDB.", t);
          }
        } finally {
          if (null != txn) {
            txn.close();
          }
        }
      } while(retry);
    } else {
      this.db.delete(bytes);
    }

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, -1);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, -1);
  }

  private ThreadLocal<WriteBatch> perThreadWriteBatch = new ThreadLocal<WriteBatch>() {
    protected WriteBatch initialValue() {
      return db.createWriteBatch();
    };
  };

  private ThreadLocal<List<FDBMutation>> perThreadMutations = new ThreadLocal<List<FDBMutation>>() {
    protected List<FDBMutation> initialValue() {
      return new ArrayList<FDBMutation>();
    };
  };

  private ThreadLocal<AtomicLong> perThreadWriteBatchSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong(0L);
    };
  };

  private void store(byte[] key, byte[] value) throws IOException {

    if (null == this.db && null == this.fdb) {
      return;
    }

    WriteBatch batch = null;
    List<FDBMutation> mutations = null;

    if (null != this.db) {
      batch = perThreadWriteBatch.get();
    } else {
      mutations = perThreadMutations.get();
    }

    AtomicLong size = perThreadWriteBatchSize.get();

    boolean written = false;

    WriteOptions options = new WriteOptions().sync(null == key || null == value || 1.0 == syncrate);

    try {
      if (null != key && null != value) {
        if (null != batch) {
          batch.put(key, value);
          size.addAndGet(key.length + value.length);
        } else {
          FDBMutation mutation = new FDBSet(fdbContext.getTenantPrefix(), key, value);
          mutations.add(mutation);
          size.addAndGet(mutation.size());
        }
      }

      if (null == key || null == value || size.get() > MAX_BATCH_SIZE) {
        if (syncwrites && !options.sync()) {
          options = new WriteOptions().sync(Math.random() < syncrate);
        }

        if (null != this.db) {
          if (size.get() > 0) {
            this.db.write(batch, options);
          }
          size.set(0L);
          perThreadWriteBatch.remove();
        } else {
          boolean retry = false;
          long retries = fdbRetryLimit;

          Transaction txn = null;

          do {
            try {
              retry = false;
              if (!mutations.isEmpty()) {
                txn = this.fdb.createTransaction();
                // Allow RAW access because we may manually force a tenant key prefix without actually setting a tenant
                txn.options().setRawAccess();

                for (FDBMutation mutation: mutations) {
                  mutation.apply(txn);
                }
                txn.commit().get();
              }
              size.set(0L);
            } catch (Throwable t) {
              FDBUtils.errorMetrics("directory", t.getCause());
              if (t.getCause() instanceof FDBException && ((FDBException) t.getCause()).isRetryable() && retries-- > 0) {
                retry = true;
              } else {
                throw new RuntimeException("Error while commiting to FoundationDB.", t);
              }
            } finally {
              if (null != txn) {
                txn.close();
              }
            }
          } while(retry);
        }
        written = true;
      }
    } finally {
      if (written) {
        if (null != batch) {
          batch.close();
        } else if (null != mutations) {
          mutations.clear();
        }
      }
    }
  }

  private void store(Metadata metadata) throws IOException {
    // Compute labelsId and classId
    // 128BITS
    long classId = GTSHelper.classId(this.classLongs, metadata.getName());
    long labelsId = GTSHelper.labelsId(this.labelsLongs, metadata.getLabels());

    String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
    Map<String,String> sensisionLabels = new HashMap<String,String>();
    sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);

    //ByteBuffer bb = ByteBuffer.wrap(new byte[1 + 8 + 8]).order(ByteOrder.BIG_ENDIAN);
    //bb.put(METADATA_PREFIX);
    //bb.putLong(classId);
    //bb.putLong(labelsId);

    byte[] bytes = new byte[1 + 8 + 8];
    System.arraycopy(METADATA_PREFIX, 0, bytes, 0, METADATA_PREFIX.length);

    GTSHelper.fillGTSIds(bytes, METADATA_PREFIX.length, classId, labelsId);

    metadata.setClassId(classId);
    metadata.setLabelsId(labelsId);

    if (Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())){
      if (metadata.getAttributesSize() > 0) {
        // Update the attributes
        Map<Long, Metadata> metadatasForClassname = metadatas.get(metadata.getName());
        if (null != metadatasForClassname) {
          Metadata oldmeta = metadatasForClassname.get(labelsId);
          if (null != oldmeta) {
            for (Entry<String, String> attr: metadata.getAttributes().entrySet()) {
              if ("".equals(attr.getValue())) {
                oldmeta.getAttributes().remove(attr.getKey());
              } else {
                oldmeta.putToAttributes(attr.getKey(), attr.getValue());
              }
            }
            metadata.setAttributes(new HashMap<String, String>(oldmeta.getAttributes()));
          } else {
            // Remove the attributes with an empty value
            Set<String> names = new HashSet<String>(metadata.getAttributes().keySet());
            for (String name: names) {
              if ("".equals(metadata.getAttributes().get(name))) {
                metadata.getAttributes().remove(name);
              }
            }
          }
        }
      }
    } else if (null == metadata.getAttributes() || !Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())) {
      metadata.setAttributes(new HashMap<String,String>());

      // If we are not updating the attributes, copy the attributes from the directory as we are probably
      // registering the GTS due to its recent activity.
      if (!Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())) {
        // Copy the attributes if the Metadata is already known, which can happen when
        // tracking the activity
        Map<Long, Metadata> metadataForClassname = metadatas.get(metadata.getName());
        if (null != metadataForClassname) {
          Metadata oldmeta = metadataForClassname.get(labelsId);
          if (null != oldmeta && oldmeta.getAttributesSize() > 0) {
            metadata.getAttributes().putAll(oldmeta.getAttributes());
          }
        }
      }
    }

    TSerializer serializer = ThriftUtils.getTSerializer(new TCompactProtocol.Factory());

    try {
      if (null != this.db || null != this.fdb) {
        byte[] serialized = serializer.serialize(metadata);
        if (null != this.aesKey) {
          serialized = CryptoUtils.wrap(this.aesKey, serialized);
        }

        //this.db.put(bb.array(), serialized);
        //this.db.put(bytes, serialized);
        store(bytes, serialized);
      }

      synchronized(metadatas) {
        if (!classids.containsKey(metadata.getName())) {
          classids.put(metadata.getName(), metadata.getClassId());
          metadatas.put(metadata.getName(), new ConcurrentSkipListMap<Long, Metadata>(Directory.ID_COMPARATOR));
        }
        if (null == metadatas.get(metadata.getName()).put(labelsId, metadata)) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, 1);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, 1);
        }
      }
      //
      // Store Metadata under 'id'
      //

      byte[] idbytes = new byte[16];
      GTSHelper.fillGTSIds(idbytes, 0, classId, labelsId);
      BigInteger id = new BigInteger(idbytes);
      metadatasById.put(id, metadata);

    } catch (TException te) {
      throw new RuntimeException(te);
    }
  }

  public Metadata getMetadataById(BigInteger id) {
    return this.metadatasById.get(id);
  }

  @Override
  public Map<String,Object> stats(DirectoryRequest dr) throws IOException {
    return stats(dr, null);
  }

  public Map<String,Object> stats(DirectoryRequest dr, ShardFilter filter) throws IOException {
    final DirectoryStatsRequest request = new DirectoryStatsRequest();
    request.setTimestamp(System.currentTimeMillis());
    request.setClassSelector(dr.getClassSelectors());
    request.setLabelsSelectors(dr.getLabelsSelectors());

    try {
      final DirectoryStatsResponse response = stats(request, filter);

      List<Future<DirectoryStatsResponse>> responses = new ArrayList<Future<DirectoryStatsResponse>>();
      Future<DirectoryStatsResponse> f = new Future<DirectoryStatsResponse>() {
        @Override
        public DirectoryStatsResponse get() throws InterruptedException, ExecutionException {
          return response;
        }
        @Override
        public DirectoryStatsResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
          return response;
        }
        @Override
        public boolean isDone() { return true; }
        @Override
        public boolean isCancelled() { return false; }
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) { return false; }
      };

      responses.add(f);

      return ThriftDirectoryClient.mergeStatsResponses(responses);
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  private DirectoryStatsResponse stats(DirectoryStatsRequest request) throws TException {
    return stats(request, null);
  }

  private DirectoryStatsResponse stats(DirectoryStatsRequest request, ShardFilter filter) throws TException {
    return DirectoryUtil.stats(request, filter, metadatas, null, LIMIT_CLASS_CARDINALITY, LIMIT_LABELS_CARDINALITY,-1, classLongs, labelsLongs, null);
  }

  @Override
  public MetadataIterator iterator(DirectoryRequest request) throws IOException {
    List<Metadata> metadatas = find(request);

    final Iterator<Metadata> iter = metadatas.iterator();

    return new MetadataIterator() {
      @Override
      public void close() throws Exception {}

      @Override
      public boolean hasNext() { return iter.hasNext(); }

      @Override
      public Metadata next() { return iter.next(); }
    };
  }

  public void setActivityWindow(long activityWindow) {
    this.activityWindow = activityWindow;
  }

}
