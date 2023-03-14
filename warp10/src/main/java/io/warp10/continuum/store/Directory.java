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

package io.warp10.continuum.store;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.ServiceType;

import io.warp10.SmartPattern;
import io.warp10.continuum.DirectoryUtil;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.KafkaOffsetCounters;
import io.warp10.continuum.LogUtil;
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.MetadataUtils.MetadataID;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsResponse;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.fdb.FDBClear;
import io.warp10.fdb.FDBContext;
import io.warp10.fdb.FDBKVScanner;
import io.warp10.fdb.FDBKeyValue;
import io.warp10.fdb.FDBMutation;
import io.warp10.fdb.FDBScan;
import io.warp10.fdb.FDBSet;
import io.warp10.fdb.FDBUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.PARSESELECTOR;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.DirectoryPlugin;
import io.warp10.warp.sdk.DirectoryPlugin.GTS;
import io.warp10.warp.sdk.TrustedDirectoryPlugin;

/**
 * Manages Metadata for a subset of known GTS.
 * Listens to Kafka to get updates of and new Metadatas
 */
public class Directory extends AbstractHandler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Directory.class);

  public static final String DEFAULT_FDB_RETRYLIMIT = Long.toString(4);

  /**
   * Maximum size of the input URI
   */
  public static final int DIRECTORY_REQUEST_HEADER_SIZE = 64 * 1024;

  /**
   * Comparator which sorts the IDs in their lexicographical order suitable for scanning DB keys
   */
  public static final Comparator<Long> ID_COMPARATOR = new Comparator<Long>() {
    @Override
    public int compare(Long o1, Long o2) {
      if (Long.signum(o1) == Long.signum(o2)) {
        return o1.compareTo(o2);
      } else {
        //
        // 0 is the first key
        //
        if (0 == o1) {
          return -1;
        } else if (0 == o2) {
          return 1;
        } else {
          //
          // If the two numbers have different signs, then the positive values MUST appear before the negative ones
          //
          if (o1 > 0) {
            return -1;
          } else {
            return 1;
          }
        }
      }
    }
  };

  public static final String PAYLOAD_MODULUS_KEY = "modulus";
  public static final String PAYLOAD_REMAINDER_KEY = "remainder";
  public static final String PAYLOAD_STREAMING_PORT_KEY = "streaming.port";

  /**
   * Values of P and P' for the HyperLogLogPlus estimators
   */
  public static final int ESTIMATOR_P = 14;
  public static final int ESTIMATOR_PPRIME = 25;

  /**
   * Allow individual tracking of 100 class names
   */
  private long LIMIT_CLASS_CARDINALITY = 100;

  /**
   * Allow tracking of 100 label names
   */
  private long LIMIT_LABELS_CARDINALITY = 100;

  private final KeyStore keystore;

  /**
   * Name under which the directory service is registered in ZK
   */
  public static final String DIRECTORY_SERVICE = "io.warp10.directory";

  private static final String DIRECTORY_INIT_NTHREADS_DEFAULT = "4";

  private final int modulus;
  private final int remainder;
  private String host;
  private int streamingport;
  private int streamingTcpBacklog;
  private int streamingselectors;
  private int streamingacceptors;

  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    io.warp10.continuum.Configuration.DIRECTORY_ZK_QUORUM,
    io.warp10.continuum.Configuration.DIRECTORY_ZK_ZNODE,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_NTHREADS,
    io.warp10.continuum.Configuration.DIRECTORY_PARTITION,
    io.warp10.continuum.Configuration.DIRECTORY_HOST,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_BOOTSTRAP_SERVERS,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_TOPIC,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_GROUPID,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_COMMITPERIOD,
    io.warp10.continuum.Configuration.DIRECTORY_PSK,
    io.warp10.continuum.Configuration.DIRECTORY_MAXAGE,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_PORT,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_SELECTORS,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_ACCEPTORS,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_IDLE_TIMEOUT,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_THREADPOOL,
    io.warp10.continuum.Configuration.DIRECTORY_REGISTER,
    io.warp10.continuum.Configuration.DIRECTORY_INIT,
    io.warp10.continuum.Configuration.DIRECTORY_STORE,
    io.warp10.continuum.Configuration.DIRECTORY_DELETE,
  };

  //
  // FoundationDB related fields
  //

  private final FDBContext fdbContext;
  private final long fdbRetryLimit;

  private Database db;


  /**
   * How often to commit Kafka offsets
   */
  private final long commitPeriod;

  /**
   * How big do we allow the mutation list to grow
   */
  private final long maxPendingMutationsSize;

  /**
   * CyclicBarrier instance to synchronize consuming threads prior to committing offsets
   */
  private CyclicBarrier barrier;

  /**
   * Flag for signaling abortion of consuming process
   */
  private final AtomicBoolean abort = new AtomicBoolean(false);

  /**
   * Maps of class name to labelsId to metadata
   */
  private final Map<String,Map<Long,Metadata>> metadatas = new MapMaker().concurrencyLevel(64).makeMap();

  /**
   * Map of classId to class names
   */
  //private final Map<Long,String> classNames = new MapMaker().concurrencyLevel(64).makeMap();
  private final Map<Long,String> classNames = new ConcurrentSkipListMap<Long, String>(ID_COMPARATOR);

  private final Map<String,Map<Long,String>> classesPerOwner = new MapMaker().concurrencyLevel(64).makeMap();

  private final ReentrantLock metadatasLock = new ReentrantLock(true);

  private final AtomicBoolean cachePopulated = new AtomicBoolean(false);
  private final AtomicBoolean fullyInitialized = new AtomicBoolean(false);

  private final Properties properties;

  private final ServiceDiscovery<Map> sd;

  private final long[] SIPHASH_CLASS_LONGS;
  private final long[] SIPHASH_LABELS_LONGS;
  private final long[] SIPHASH_PSK_LONGS;

  /**
   * Maximum age of signed requests
   */
  private final long maxage;

  private final int initNThreads;

  private final long idleTimeout;

  /**
   * Should we register our service in ZK
   */
  private final boolean register;

  /**
   * Service instance
   */
  private ServiceInstance<Map> instance = null;

  /**
   * Thread used as shutdown hook for deregistering instance
   */
  private Thread deregisterHook = null;

  /**
   * Should we initialize Directory upon startup by reading from DB
   */
  private final boolean init;

  /**
   * Should we store in DB metadata we receive via Kafka
   */
  private final boolean store;

  /**
   * Should we delete in DB
   */
  private final boolean delete;

  /**
   * Do we track activity of GeoTimeSeries?
   */
  private final boolean trackingActivity;

  /**
   * Activity window for activity tracking
   */
  private final long activityWindow;

  /**
   * Directory plugin to use
   */
  private final DirectoryPlugin plugin;
  private boolean trustedPlugin = false;

  private final String sourceAttribute;

  private int METADATA_CACHE_SIZE = 1000000;

  /**
   * Cache to keep a serialized version of recently returned Metadata.
   */
  final Map<MetadataID, byte[]> serializedMetadataCache = new LinkedHashMap<MetadataID, byte[]>(100, 0.75F, true) {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<MetadataID, byte[]> eldest) {
      Sensision.set(SensisionConstants.CLASS_WARP_DIRECTORY_METADATA_CACHE_SIZE, Sensision.EMPTY_LABELS, this.size());
      return this.size() > METADATA_CACHE_SIZE;
    }
  };

  public Directory(KeyStore keystore, final Properties props) throws IOException {
    this.keystore = keystore;

    SIPHASH_CLASS_LONGS = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_CLASS));
    SIPHASH_LABELS_LONGS = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_LABELS));

    this.properties = (Properties) props.clone();

    this.sourceAttribute = props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_PLUGIN_SOURCEATTR);

    //
    // Check mandatory parameters
    //

    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(required), "Missing configuration parameter '%s'.", required);
    }

    this.fdbContext = FDBUtils.getContext(props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_FDB_CLUSTERFILE), props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_FDB_TENANT));
    this.fdbRetryLimit = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_FDB_RETRYLIMIT, DEFAULT_FDB_RETRYLIMIT));

    this.db = fdbContext.getDatabase();

    this.register = "true".equals(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_REGISTER));
    this.init = "true".equals(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_INIT));
    this.store = "true".equals(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STORE));
    this.delete = "true".equals(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_DELETE));

    this.activityWindow = Long.parseLong(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_ACTIVITY_WINDOW, "0"));
    this.trackingActivity = this.activityWindow > 0;

    //
    // Extract parameters
    //

    if (null != props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_METADATA_CACHE_SIZE)) {
      this.METADATA_CACHE_SIZE = Integer.valueOf(props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_METADATA_CACHE_SIZE));
    }

    idleTimeout = Long.parseLong(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_IDLE_TIMEOUT));

    if (properties.containsKey(io.warp10.continuum.Configuration.DIRECTORY_STATS_CLASS_MAXCARDINALITY)) {
      this.LIMIT_CLASS_CARDINALITY = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STATS_CLASS_MAXCARDINALITY));
    }

    if (properties.containsKey(io.warp10.continuum.Configuration.DIRECTORY_STATS_LABELS_MAXCARDINALITY)) {
      this.LIMIT_LABELS_CARDINALITY = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STATS_LABELS_MAXCARDINALITY));
    }

    this.initNThreads = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_INIT_NTHREADS, DIRECTORY_INIT_NTHREADS_DEFAULT));

    String partition = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_PARTITION);
    String[] tokens = partition.split(":");
    this.modulus = Integer.parseInt(tokens[0]);
    this.remainder = Integer.parseInt(tokens[1]);

    this.maxage = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_MAXAGE));

    final String topic = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_TOPIC);
    final int nthreads = Integer.valueOf(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_NTHREADS));

    //
    // Extract keys
    //

    extractKeys(properties);

    SIPHASH_PSK_LONGS = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_DIRECTORY_PSK));

    //
    // Load Directory plugin
    //

    if (this.properties.containsKey(io.warp10.continuum.Configuration.DIRECTORY_PLUGIN_CLASS)) {
      try {
        ClassLoader pluginCL = this.getClass().getClassLoader();

        Class pluginClass = Class.forName(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_PLUGIN_CLASS), true, pluginCL);
        this.plugin = (DirectoryPlugin) pluginClass.newInstance();
        if (this.plugin instanceof TrustedDirectoryPlugin) {
          this.trustedPlugin = true;
        }

        //
        // Now call the 'init' method of the plugin
        //

        this.plugin.init(new Properties(properties));
      } catch (Exception e) {
        throw new RuntimeException("Unable to instantiate plugin class", e);
      }
    } else {
      this.plugin = null;
    }

    //
    // Create Curator framework and service discovery
    //

    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_ZK_QUORUM))
        .build();
    curatorFramework.start();

    this.sd = ServiceDiscoveryBuilder.builder(Map.class)
        .basePath(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_ZK_ZNODE))
        .client(curatorFramework)
        .build();

    //
    // Launch a Thread which will populate the metadata cache
    // We don't do that in the constructor otherwise it might take too long to return
    //

    final Directory self = this;

    if (this.init) {

      Thread[] initThreads = new Thread[this.initNThreads];
      final AtomicBoolean[] stopMarkers = new AtomicBoolean[this.initNThreads];

      final LinkedBlockingQueue<FDBKeyValue> kvQ = new LinkedBlockingQueue<FDBKeyValue>(initThreads.length * 8192);

      for (int i = 0; i < initThreads.length; i++) {
        stopMarkers[i] = new AtomicBoolean(false);
        final AtomicBoolean stopMe = stopMarkers[i];
        initThreads[i] = new Thread(new Runnable() {
          @Override
          public void run() {
            AESWrapEngine engine = null;
            if (null != self.keystore.getKey(KeyStore.AES_FDB_METADATA)) {
              engine = new AESWrapEngine();
              CipherParameters params = new KeyParameter(self.keystore.getKey(KeyStore.AES_FDB_METADATA));
              engine.init(false, params);
            }

            PKCS7Padding padding = new PKCS7Padding();

            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

            while (!stopMe.get()) {
              try {

                FDBKeyValue kv = kvQ.poll(100, TimeUnit.MILLISECONDS);

                if (null == kv) {
                  continue;
                }

                byte[] value = kv.getValueArray();

                if (null != engine) {
                  //
                  // Unwrap
                  //

                  byte[] unwrapped = engine.unwrap(value, kv.getValueOffset(), kv.getValueLength());

                  //
                  // Unpad
                  //

                  int padcount = padding.padCount(unwrapped);
                  value = Arrays.copyOf(unwrapped, unwrapped.length - padcount);
                }

                //
                // Deserialize
                //

                Metadata metadata = new Metadata();
                deserializer.deserialize(metadata, value);

                //
                // Compute classId/labelsId and compare it to the values in the row key
                //

                long classId = GTSHelper.classId(self.SIPHASH_CLASS_LONGS, metadata.getName());
                long labelsId = GTSHelper.labelsId(self.SIPHASH_LABELS_LONGS, metadata.getLabels());

                //
                // Recheck labelsid so we don't retain GTS with invalid labelsid in the row key (which may have happened due
                // to bugs)
                //

                int rem = ((int) ((labelsId >>> 56) & 0xffL)) % self.modulus;

                if (self.remainder != rem) {
                  continue;
                }
                ByteBuffer bb = ByteBuffer.wrap(kv.getKeyArray(), kv.getKeyOffset(), kv.getKeyLength()).order(ByteOrder.BIG_ENDIAN);
                // Skip key prefix
                bb.position(bb.position() + Constants.FDB_METADATA_KEY_PREFIX.length);
                long hbClassId = bb.getLong();
                long hbLabelsId = bb.getLong();

                // If classId/labelsId are incoherent, skip metadata
                if (classId != hbClassId || labelsId != hbLabelsId) {
                  LOG.warn("Incoherent class/labels Id for " + metadata);
                  continue;
                }

                metadata.setClassId(classId);
                metadata.setLabelsId(labelsId);

                if (!metadata.isSetAttributes()) {
                  metadata.setAttributes(new HashMap<String,String>());
                }

                //
                // Internalize Strings
                //

                GTSHelper.internalizeStrings(metadata);

                //
                // Let the DirectoryPlugin handle the Metadata
                //

                if (null != plugin) {

                  long nano = 0;

                  try {
                    if (!trustedPlugin) {
                      //
                      // Directory plugins have no provision for delta attribute updates
                      //
                      GTS gts = new GTS(
                          new UUID(metadata.getClassId(), metadata.getLabelsId()),
                          metadata.getName(),
                          metadata.getLabels(),
                          metadata.getAttributes());

                      if (null != sourceAttribute) {
                        gts.getAttributes().put(sourceAttribute, metadata.getSource());
                      }

                      nano = System.nanoTime();

                      if (!plugin.store(null, gts)) {
                        throw new RuntimeException("Error storing GTS " + gts + " using directory plugin.");
                      }
                    } else {
                      nano = System.nanoTime();

                      if (!((TrustedDirectoryPlugin) plugin).store(metadata)) {
                        throw new RuntimeException("Error storing GTS " + metadata + " using trusted directory plugin.");
                      }
                    }
                  } finally {
                    nano = System.nanoTime() - nano;
                    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_CALLS, Sensision.EMPTY_LABELS, 1);
                    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_TIME_NANOS, Sensision.EMPTY_LABELS, nano);
                  }
                  continue;
                }

                try {
                  metadatasLock.lockInterruptibly();
                  if (!metadatas.containsKey(metadata.getName())) {
                    metadatas.put(metadata.getName(), new ConcurrentSkipListMap<Long, Metadata>(ID_COMPARATOR));
                    classNames.put(classId, metadata.getName());
                  }
                } finally {
                  if (metadatasLock.isHeldByCurrentThread()) {
                    metadatasLock.unlock();
                  }
                }

                //
                // Store per owner class name. We use the name since it has been internalized,
                // therefore we only consume the HashNode and the HashSet overhead
                //

                String owner = metadata.getLabels().get(Constants.OWNER_LABEL);

                String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
                Map<String,String> sensisionLabels = new HashMap<String,String>();
                sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);

                synchronized(classesPerOwner) {
                  Map<Long,String> classes = classesPerOwner.get(owner);

                  if (null == classes) {
                    classes = new ConcurrentSkipListMap<Long,String>(ID_COMPARATOR);
                    classesPerOwner.put(owner, classes);
                  }

                  classes.put(metadata.getClassId(), metadata.getName());
                }

                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, 1);
                Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_OWNERS, Sensision.EMPTY_LABELS, classesPerOwner.size());

                synchronized(metadatas.get(metadata.getName())) {
                  if (!metadatas.get(metadata.getName()).containsKey(labelsId)) {
                    metadatas.get(metadata.getName()).put(labelsId, metadata);
                    continue;
                  } else if (!metadatas.get(metadata.getName()).get(labelsId).getLabels().equals(metadata.getLabels())) {
                    LOG.warn("LabelsId collision under class '" + metadata.getName() + "' " + metadata.getLabels() + " and " + metadatas.get(metadata.getName()).get(labelsId).getLabels());
                    Sensision.update(SensisionConstants.CLASS_WARP_DIRECTORY_LABELS_COLLISIONS, Sensision.EMPTY_LABELS, 1);
                  }
                }

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

      Thread populator = new Thread(new Runnable() {

        @Override
        public void run() {

          long nano = System.nanoTime();

          long count = 0L;

          boolean done = false;

          byte[] lastrow = null;
          int lastrowoffset = 0;
          int lastrowlength = 0;

          while(!done) {

            FDBKVScanner scanner = null;

            try {
              //
              // Populate the metadata cache with initial data from FoundationDB
              //

              FDBScan scan = new FDBScan();
              scan.setTenantPrefix(fdbContext.getTenantPrefix());

              if (null != lastrow) {
                scan.setStartKey(FDBUtils.getNextKey(lastrow, lastrowoffset, lastrowlength));
              } else {
                scan.setStartKey(Constants.FDB_METADATA_KEY_PREFIX);
              }

              scan.setEndKey(FDBUtils.getNextKey(Constants.FDB_METADATA_KEY_PREFIX));
              scan.setReverse(false);

              scanner = scan.getScanner(fdbContext, db, StreamingMode.WANT_ALL);

              while(scanner.hasNext()) {
                FDBKeyValue kv = scanner.next();

                int r = (((int) kv.getKeyArray()[kv.getKeyOffset() + Constants.FDB_METADATA_KEY_PREFIX.length + 8]) & 0xff) % self.modulus;

                // Skip metadata if its modulus is not the one we expect
                if (self.remainder != r) {
                  continue;
                }

                //
                // Store the current row so we can restart from there if an exception occurs
                //

                boolean interrupted = true;

                while(interrupted) {
                  interrupted = false;
                  try {
                    kvQ.put(kv);
                    count++;
                    if (0 == count % 1000 && null == plugin) {
                      // We do not update this metric when using a Directory plugin
                      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, count);
                    }
                    lastrow = kv.getKeyArray();
                    lastrowoffset = kv.getKeyOffset();
                    lastrowlength = kv.getKeyLength();
                  } catch (InterruptedException ie) {
                    interrupted = true;
                  }
                }
              }

              done = true;
            } catch (Throwable t) {
              if (t.getCause() instanceof FDBException) {
                try {
                  db.close();
                } catch (Exception e) {}
                db = fdbContext.getDatabase();
              }
              LOG.error("Caught exception in scanning loop, will attempt to continue where we stopped", t);
            } finally {
              if (!done) {
                // Try reopening FoundationDB database when an error was encountered
                if (null != scanner) {
                  try {
                    scanner.close();
                  } catch (Exception e) {
                    LOG.error("Error closing FoundationDB scanner, will continue anyway.", e);
                  }
                }
                if (null != db) {
                  try {
                    db.close();
                  } catch (Exception e) {
                    LOG.error("Error closing FoundationDB transaction context, will continue anyway.", e);
                  }
                }
                db = fdbContext.getDatabase();
              } else {
                try {
                  scanner.close();
                } catch (Exception e) {
                  LOG.error("Error closing FoundationDB scanner, will continue anyway.", e);
                }
              }

              if (null == plugin) {
                // We do not update this metric when using a Directory plugin
                Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, count);
              }
            }
          }

          //
          // Wait until resultQ is empty
          //

          while(!kvQ.isEmpty()) {
            LockSupport.parkNanos(100000000L);
          }

          //
          // Notify the init threads to stop
          //

          for (int i = 0; i < initNThreads; i++) {
            stopMarkers[i].set(true);
          }

          self.cachePopulated.set(true);

          if (null != self.plugin && self.trustedPlugin) {
            ((TrustedDirectoryPlugin) self.plugin).initialized();
          }

          nano = System.nanoTime() - nano;

          LOG.info("Loaded " + count + " GTS in " + (nano / 1000000.0D) + " ms");
        }
      });

      populator.setName("Warp Directory Populator");
      populator.setDaemon(true);
      populator.start();
    } else {
      LOG.info("Skipped initialization");
      this.cachePopulated.set(true);
    }

    this.commitPeriod = Long.valueOf(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_COMMITPERIOD));

    this.maxPendingMutationsSize = (long) Math.min(FDBUtils.MAX_TXN_SIZE * 0.95, Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_FDB_METADATA_PENDINGMUTATIONS_MAXSIZE)));

    this.host = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HOST);
    this.streamingport = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_PORT));
    this.streamingTcpBacklog = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_TCP_BACKLOG, "0"));
    this.streamingacceptors = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_ACCEPTORS));
    this.streamingselectors = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_SELECTORS));

    int streamingMaxThreads = Integer.parseInt(props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_THREADPOOL));

    final String groupid = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_GROUPID);

    final KafkaOffsetCounters counters = new KafkaOffsetCounters(topic, groupid, this.commitPeriod * 2);

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {

        //
        // Wait until directory is fully initialized
        //

        while(!self.fullyInitialized.get()) {
          LockSupport.parkNanos(1000000000L);
        }

        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_CLASSES, Sensision.EMPTY_LABELS, classNames.size());

        //
        // Enter an endless loop which will spawn 'nthreads' threads
        // each time the Kafka consumer is shut down (which will happen if an error
        // happens while talking to FDB, to get a chance to re-read data from the
        // previous snapshot).
        //

        while (true) {
          try {
            Map<String,Integer> topicCountMap = new HashMap<String, Integer>();

            topicCountMap.put(topic, nthreads);

            Properties props = new Properties();

            // Load explicit configuration
            props.putAll(io.warp10.continuum.Configuration.extractPrefixed(properties, properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_CONF_PREFIX)));

            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_BOOTSTRAP_SERVERS));
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
            if (null != properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_CLIENTID)) {
              props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_CLIENTID));
            }
            if (null != properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY)) {
              props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY));
            }
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            if (null != properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_AUTO_OFFSET_RESET)) {
              props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_AUTO_OFFSET_RESET));
            }

            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

            KafkaConsumer<byte[], byte[]>[] consumers = null;

            self.barrier = new CyclicBarrier(nthreads + 1);

            ExecutorService executor = Executors.newFixedThreadPool(nthreads);

            //
            // now create runnables which will consume messages
            //

            // Reset counters
            counters.reset();

            consumers = new KafkaConsumer[nthreads];

            Collection<String> topics = Collections.singletonList(topic);
            for (int i = 0; i < nthreads; i++) {
              consumers[i] = new KafkaConsumer<>(props);
              executor.submit(new DirectoryConsumer(self, consumers[i], counters, topics));
            }

            while(!abort.get() && !Thread.currentThread().isInterrupted()) {
              try {
                if (nthreads == barrier.getNumberWaiting()) {
                  //
                  // Check if we should abort, which could happen when
                  // an exception was thrown when flushing the commits just before
                  // entering the barrier
                  //

                  if (abort.get()) {
                    break;
                  }

                  //
                  // All processing threads are waiting on the barrier, this means we can flush the offsets because
                  // they have all processed data successfully for the given activity period
                  //

                  // Commit offsets
                  try {
                    for (KafkaConsumer consumer: consumers) {
                      consumer.commitSync();
                    }
                  } catch (Throwable t) {
                    throw t;
                  } finally {
                    // We instruct the counters that we committed the offsets, in the worst case
                    // we will experience a backward leap which is not fatal, whereas missing
                    // a commit will lead to a forward leap which will make the Directory fail
                    counters.commit();
                  }

                  counters.sensisionPublish();

                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_COMMITS, Sensision.EMPTY_LABELS, 1);

                  // Release the waiting threads
                  try {
                    barrier.await();
                  } catch (Exception e) {
                    break;
                  }
                }
              } catch (Throwable t) {
                // We need to catch possible errors in commitOffsets
                LOG.error("", t);
                abort.set(true);
              }

              LockSupport.parkNanos(1000000L);
            }

            //
            // We exited the loop, this means one of the threads triggered an abort,
            // we will shut down the executor and shut down the connector to start over.
            //

            executor.shutdownNow();
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_DIRECTORY_KAFKA_SHUTDOWNS, Sensision.EMPTY_LABELS, 1);

            for (KafkaConsumer consumer: consumers) {
              try {
                consumer.close();
              } catch (Exception e) {
              }
            }
          } catch (Throwable t) {
            LOG.error("Caught throwable in spawner.", t);
          } finally {
            abort.set(false);
            LockSupport.parkNanos(1000000000L);
          }
        }
      }
    });

    t.setName("Warp Directory Spawner");
    t.setDaemon(true);
    t.start();

    t = new Thread(this);
    t.setName("Warp Directory");
    t.setDaemon(true);
    t.start();

    //
    // Start Jetty for the streaming service
    //

    //
    // Start Jetty server for the streaming service
    //

    BlockingArrayQueue<Runnable> queue = null;

    if (props.containsKey(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_MAXQUEUESIZE)) {
      int queuesize = Integer.parseInt(props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_MAXQUEUESIZE));
      queue = new BlockingArrayQueue<Runnable>(queuesize);
    }

    QueuedThreadPool queuedThreadPool = new QueuedThreadPool(streamingMaxThreads, 8, (int) idleTimeout, queue);
    queuedThreadPool.setName("Warp Directory Jetty Thread");
    Server server = new Server(queuedThreadPool);

    //
    // Iterate over the properties to find those starting with DIRECTORY_STREAMING_JETTY_ATTRIBUTE and set
    // the Jetty attributes accordingly
    //

    for (Entry<Object,Object> entry: props.entrySet()) {
      if (entry.getKey().toString().startsWith(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_JETTY_ATTRIBUTE_PREFIX)) {
        server.setAttribute(entry.getKey().toString().substring(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_JETTY_ATTRIBUTE_PREFIX.length()), entry.getValue().toString());
      }
    }

    //ServerConnector connector = new ServerConnector(server, this.streamingacceptors, this.streamingselectors);
    HttpConfiguration config = new HttpConfiguration();
    config.setRequestHeaderSize(DIRECTORY_REQUEST_HEADER_SIZE);
    HttpConnectionFactory factory = new HttpConnectionFactory(config);
    ServerConnector connector = new ServerConnector(server,null,null,null,this.streamingacceptors, this.streamingselectors,factory);

    connector.setIdleTimeout(idleTimeout);
    connector.setPort(this.streamingport);
    connector.setHost(host);
    connector.setAcceptQueueSize(this.streamingTcpBacklog);
    connector.setName("Directory Streaming Service");

    server.setConnectors(new Connector[] { connector });

    server.setHandler(this);

    JettyUtil.setSendServerVersion(server, false);

    //
    // Wait for initialization to be done
    //

    while(!this.fullyInitialized.get()) {
      LockSupport.parkNanos(1000000000L);
    }

    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {

    //
    // Add a shutdown hook to unregister Directory
    //

    if (this.register) {
      final Directory self = this;
      this.deregisterHook = new Thread() {
        @Override
        public void run() {
          try {
            LOG.info("Unregistering from ZooKeeper.");
            self.sd.close();
            LOG.info("Directory successfully unregistered from ZooKeeper.");
          } catch (Exception e) {
            LOG.error("Error while unregistering Directory.", e);
          }
        }
      };
      Runtime.getRuntime().addShutdownHook(deregisterHook);
    }

    //
    // Wait until cache has been populated
    //

    while(!this.cachePopulated.get()) {
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_JVM_FREEMEMORY, Sensision.EMPTY_LABELS, Runtime.getRuntime().freeMemory());
      LockSupport.parkNanos(1000000000L);
    }

    //
    // Let's call GC once after populating so we take the trash out.
    //

    LOG.info("Triggering a GC to clean up after initial loading.");
    long nano = System.nanoTime();
    Runtime.getRuntime().gc();
    nano = System.nanoTime() - nano;
    LOG.info("GC performed in " + (nano / 1000000.0D) + " ms.");

    this.fullyInitialized.set(true);

    Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_JVM_FREEMEMORY, Sensision.EMPTY_LABELS, Runtime.getRuntime().freeMemory());

    //
    // Register the service
    //

    this.instance = null;

    try {
      InetAddress bindAddress = InetAddress.getByName(this.host);

      ServiceInstanceBuilder<Map> builder = ServiceInstance.builder();
      builder.port(this.streamingport);
      builder.address(bindAddress.getHostAddress());
      builder.id(UUID.randomUUID().toString());
      builder.name(DIRECTORY_SERVICE);
      builder.serviceType(ServiceType.DYNAMIC);
      Map<String,String> payload = new HashMap<String,String>();

      payload.put(PAYLOAD_MODULUS_KEY, Integer.toString(modulus));
      payload.put(PAYLOAD_REMAINDER_KEY, Integer.toString(remainder));
      payload.put(PAYLOAD_STREAMING_PORT_KEY, Integer.toString(this.streamingport));
      builder.payload(payload);

      this.instance = builder.build();

      if (this.register) {
        sd.start();
        sd.registerService(instance);
      }

      while(true) {
        LockSupport.parkNanos(Long.MAX_VALUE);
      }
    } catch (Exception e) {
      LOG.error("", e);
    } finally {
      if (null != instance) {
        try {
          sd.unregisterService(instance);
        } catch (Exception e) {
        }
      }
      if (null != deregisterHook) {
        try {
          Runtime.getRuntime().removeShutdownHook(deregisterHook);
        } catch (Exception e) {
        }
      }
    }
  }

  /**
   * Extract Directory related keys and populate the KeyStore with them.
   *
   * @param props Properties from which to extract the key specs
   */
  private void extractKeys(Properties props) {
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_KAFKA_METADATA, props, io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_MAC, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_KAFKA_METADATA, props, io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_AES, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_FDB_METADATA, props, io.warp10.continuum.Configuration.DIRECTORY_FDB_METADATA_AES, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_DIRECTORY_PSK, props, io.warp10.continuum.Configuration.DIRECTORY_PSK, 128);

    this.keystore.forget();
  }

  private static class DirectoryConsumer implements Runnable {

    private final Directory directory;
    private final KafkaConsumer<byte[],byte[]> consumer;
    private final Collection<String> topics;
    private final KafkaOffsetCounters counters;

    private final AtomicBoolean localabort = new AtomicBoolean(false);

    // Boolean indicating that the last call to poll returned records which
    // must be stored prior to committing the offsets, if pending is true
    // then we will not trigger a commit
    private final AtomicBoolean pending = new AtomicBoolean(false);


    public DirectoryConsumer(Directory directory, KafkaConsumer<byte[], byte[]> consumer, KafkaOffsetCounters counters, Collection<String> topics) {
      this.directory = directory;
      this.consumer = consumer;
      this.topics = topics;
      this.counters = counters;
    }

    private boolean resetPending() {
      pending.set(false);
      return true;
    }

    @Override
    public void run() {
      final FDBContext fdbContext = this.directory.fdbContext;
      Database db = fdbContext.getDatabase();

      try {
        this.consumer.subscribe(this.topics);

        byte[] siphashKey = directory.keystore.getKey(KeyStore.SIPHASH_KAFKA_METADATA);
        byte[] kafkaAESKey = directory.keystore.getKey(KeyStore.AES_KAFKA_METADATA);

        //
        // AtomicLong with the timestamp of the last Put or 0 if
        // none were added since the last flush
        //

        final AtomicLong lastAction = new AtomicLong(0L);

        final List<FDBMutation> mutations = new ArrayList<FDBMutation>();
        final ReentrantLock mutationsLock = new ReentrantLock(true);

        final AtomicLong mutationsSize = new AtomicLong(0L);

        // Boolean indicating that we should commit the offsets. This is used
        // to ensure that we do not call poll when a commit must be performed
        final AtomicBoolean mustCommit = new AtomicBoolean(false);

        //
        // Start the synchronization Thread
        //

        Thread synchronizer = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              long lastsync = System.currentTimeMillis();

              //
              // Check for how long we've been storing readings, if we've reached the commitperiod,
              // flush any pending commits and synchronize with the other threads so offsets can be committed
              //

              while(!localabort.get() && !Thread.currentThread().isInterrupted()) {
                long now = System.currentTimeMillis();

                if (now - lastsync > directory.commitPeriod || (mustCommit.get() && !pending.get())) {
                  //
                  // We synchronize on 'puts' so the main Thread does not add Puts to it
                  //

                  //synchronized (puts) {
                  try {
                    mutationsLock.lockInterruptibly();

                    //
                    // Attempt to flush
                    //

                    try {
                      if ((directory.store||directory.delete) && !mutations.isEmpty()) {
                        flushMutations();
                        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FDB_COMMITS, Sensision.EMPTY_LABELS, 1);
                      }

                      mutations.clear();
                      mutationsSize.set(0L);
                      // Reset lastPut to 0
                      lastAction.set(0L);
                    } catch (IOException ioe) {
                      // Clear list of Puts
                      mutations.clear();
                      mutationsSize.set(0L);
                      // If an exception is thrown, abort
                      directory.abort.set(true);
                      return;
                    }

                    mustCommit.set(true);
                    // Do not trigger the commit if there are pending records
                    if (pending.get()) {
                      continue;
                    }

                    //
                    // Now join the cyclic barrier which will trigger the
                    // commit of offsets
                    //
                    try {
                      directory.barrier.await();
                      mustCommit.set(false);
                      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_BARRIER_SYNCS, Sensision.EMPTY_LABELS, 1);
                    } catch (Exception e) {
                      directory.abort.set(true);
                      return;
                    } finally {
                      lastsync = System.currentTimeMillis();
                    }
                  } catch (InterruptedException ie) {
                    directory.abort.set(true);
                    return;
                  } finally {
                    if (mutationsLock.isHeldByCurrentThread()) {
                      mutationsLock.unlock();
                    }
                  }
                } else if (0 != lastAction.get() && (now - lastAction.get() > 500) || mutationsSize.get() > directory.maxPendingMutationsSize) {
                  //
                  // If the last commit was performed more than 500ms ago, force a flush
                  //

                  try {
                    //synchronized(puts) {
                    mutationsLock.lockInterruptibly();
                    try {
                      if (directory.store||directory.delete) {
                        flushMutations();
                        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FDB_COMMITS, Sensision.EMPTY_LABELS, 1);
                      }

                      mutations.clear();
                      mutationsSize.set(0L);
                      // Reset lastPut to 0
                      lastAction.set(0L);
                    } catch (IOException ioe) {
                      // Clear list of Puts
                      mutations.clear();
                      mutationsSize.set(0L);
                      directory.abort.set(true);
                      return;
                    }
                  } catch (InterruptedException ie) {
                    directory.abort.set(true);
                    return;
                  } finally {
                    if (mutationsLock.isHeldByCurrentThread()) {
                      mutationsLock.unlock();
                    }
                  }
                }

                LockSupport.parkNanos(1000000L);
              }
            } catch (Throwable t) {
              LOG.error("Caught exception in synchronizer", t);
              throw t;
            } finally {
              directory.abort.set(true);
            }
          }

          private void flushMutations() throws IOException {
            Transaction txn = null;

            boolean retry = false;
            long retries = directory.fdbRetryLimit;

            do {
              try {
                retry = false;
                txn = db.createTransaction();
                // Allow RAW access because we may manually force a tenant key prefix without actually setting a tenant
                txn.options().setRawAccess();

                for (FDBMutation mutation: mutations) {
                  mutation.apply(txn);
                }
                txn.commit().get();
              } catch (Throwable t) {
                FDBUtils.errorMetrics("directory", t.getCause());
                if (t.getCause() instanceof FDBException && ((FDBException) t.getCause()).isRetryable() && retries-- > 0) {
                  retry = true;
                } else {
                  throw new IOException("Error while commiting to FoundationDB.", t);
                }
              } finally {
                if (null != txn) {
                  txn.close();
                }
              }
            } while(retry);
          }
        });

        synchronizer.setName("Warp Directory Synchronizer");
        synchronizer.setDaemon(true);
        synchronizer.start();

        // TODO(hbs): allow setting of writeBufferSize

        MetadataID id = null;

        byte[] fdbAESKey = directory.keystore.getKey(KeyStore.AES_FDB_METADATA);

        Duration delay = Duration.of(500L, ChronoUnit.MILLIS);

        while (!directory.abort.get()) {
          Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_JVM_FREEMEMORY, Sensision.EMPTY_LABELS, Runtime.getRuntime().freeMemory());

          ConsumerRecords<byte[], byte[]> consumerRecords = null;

          //
          // We acquire actionsLock prior to calling poll, so we know when the synchronizer
          // thread is waiting on the barrier, as it holds the lock, no current call to poll
          // is ongoing so we can safely call commitSync on the consumer.
          //

          try {
            mutationsLock.lockInterruptibly();

            pending.set(false);

            // Do not call poll if we must commit the offsets
            if (mustCommit.get()) {
              continue;
            }

            consumerRecords = consumer.poll(delay);

            pending.set(consumerRecords.count() > 0);
          } finally {
            if (mutationsLock.isHeldByCurrentThread()) {
              mutationsLock.unlock();
            }
          }

          boolean first = true;

          Iterator<ConsumerRecord<byte[],byte[]>> iter = consumerRecords.iterator();
          while(resetPending() && iter.hasNext()) {

            ConsumerRecord<byte[],byte[]> record = null;

            try {
              mutationsLock.lockInterruptibly();

              // Do not read record if we must commit the offsets
              if (mustCommit.get()) {
                continue;
              }

              if (!first) {
                throw new RuntimeException("Invalid input, expected a single record, got " + consumerRecords.count());
              }

              first = false;

              record = iter.next();
              pending.set(true);
            } finally {
              if (mutationsLock.isHeldByCurrentThread()) {
                mutationsLock.unlock();
              }
            }

            if (!counters.safeCount(record.partition(), record.offset())) {
              continue;
            }

            //
            // We do an early selection check based on the Kafka key.
            // Since 20151104 we now correctly push the Kafka key (cf Ingress bug in pushMetadataMessage(k,v))
            //

            int r = (((int) record.key()[8]) & 0xff) % directory.modulus;

            if (directory.remainder != r) {
              continue;
            }

            //
            // We do not rely on the Kafka key for selection as it might have been incorrectly set.
            // We therefore unwrap all messages and decide later.
            //

            byte[] data = record.value();

            if (null != siphashKey) {
              data = CryptoUtils.removeMAC(siphashKey, data);
            }

            // Skip data whose MAC was not verified successfully
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_FAILEDMACS, Sensision.EMPTY_LABELS, 1);
              continue;
            }

            // Unwrap data if need be
            if (null != kafkaAESKey) {
              data = CryptoUtils.unwrap(kafkaAESKey, data);
            }

            // Skip data that was not unwrapped successfuly
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_FAILEDDECRYPTS, Sensision.EMPTY_LABELS, 1);
              continue;
            }

            //
            // TODO(hbs): We could check that metadata class/labels Id match those of the key, but
            // since it was wrapped/authenticated, we suppose it's ok.
            //

            //byte[] labelsBytes = Arrays.copyOfRange(data, 8, 16);
            //long labelsId = Longs.fromByteArray(labelsBytes);

            // 128bits
            byte[] metadataBytes = Arrays.copyOfRange(data, 16, data.length);
            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
            Metadata metadata = new Metadata();
            deserializer.deserialize(metadata, metadataBytes);

            //
            // Force Attributes
            //

            if (!metadata.isSetAttributes()) {
              metadata.setAttributes(new HashMap<String,String>());
            }

            //
            // Recompute labelsid and classid
            //

            // 128bits
            long classId = GTSHelper.classId(directory.SIPHASH_CLASS_LONGS, metadata.getName());
            long labelsId = GTSHelper.labelsId(directory.SIPHASH_LABELS_LONGS, metadata.getLabels());

            metadata.setLabelsId(labelsId);
            metadata.setClassId(classId);

            //
            // Recheck labelsid so we don't retain GTS with invalid labelsid in the row key (which may have happened due
            // to bugs)
            //

            int rem = ((int) ((labelsId >>> 56) & 0xffL)) % directory.modulus;

            if (directory.remainder != rem) {
              continue;
            }

            String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
            Map<String,String> sensisionLabels = new HashMap<String,String>();
            sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);

            //
            // Check the source of the metadata
            //

            //
            // If Metadata is from Delete, remove it from the cache AND from FoundationDB
            //

            if (io.warp10.continuum.Configuration.INGRESS_METADATA_DELETE_SOURCE.equals(metadata.getSource())) {

              //
              // Call external plugin
              //

              if (null != directory.plugin) {

                long nano = 0;

                try {
                  if (!directory.trustedPlugin) {
                    GTS gts = new GTS(
                        // 128bits
                        new UUID(metadata.getClassId(), metadata.getLabelsId()),
                        metadata.getName(),
                        metadata.getLabels(),
                        metadata.getAttributes());

                    nano = System.nanoTime();

                    if (!directory.plugin.delete(gts)) {
                      break;
                    }
                  } else {
                    nano = System.nanoTime();

                    if (!((TrustedDirectoryPlugin) directory.plugin).delete(metadata)) {
                      break;
                    }
                  }
                } finally {
                  nano = System.nanoTime() - nano;
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_DELETE_CALLS, Sensision.EMPTY_LABELS, 1);
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_DELETE_TIME_NANOS, Sensision.EMPTY_LABELS, nano);
                }

              } else {
                // If the name does not exist AND we actually loaded the metadata from FoundationDB, continue.
                // If we did not load from FoundationDB, handle the delete
                if (!directory.metadatas.containsKey(metadata.getName()) && directory.init) {
                  continue;
                }

                // Remove cache entry
                Map<Long,Metadata> metamap = directory.metadatas.get(metadata.getName());
                if (null != metamap && null != metamap.remove(labelsId)) {
                  if (metamap.isEmpty()) {
                    try {
                      directory.metadatasLock.lockInterruptibly();
                      metamap = directory.metadatas.get(metadata.getName());
                      if (null != metamap && metamap.isEmpty()) {
                        directory.metadatas.remove(metadata.getName());
                        directory.classNames.remove(metadata.getClassId());
                        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_CLASSES, Sensision.EMPTY_LABELS, -1);
                      }
                    } finally {
                      if (directory.metadatasLock.isHeldByCurrentThread()) {
                        directory.metadatasLock.unlock();
                      }
                    }
                  }
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, -1);
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, -1);
                }
              }

              //
              // Clear the cache
              //

              id = MetadataUtils.id(metadata);
              directory.serializedMetadataCache.remove(id);

              if (!directory.delete) {
                continue;
              }

              // Remove FoundationDB entry

              // Prefix + classId + labelsId
              byte[] rowkey = new byte[Constants.FDB_METADATA_KEY_PREFIX.length + 8 + 8];

              // 128bits
              ByteBuffer bb = ByteBuffer.wrap(rowkey).order(ByteOrder.BIG_ENDIAN);
              bb.put(Constants.FDB_METADATA_KEY_PREFIX);
              bb.putLong(classId);
              bb.putLong(labelsId);

              FDBClear clear = new FDBClear(fdbContext.getTenantPrefix(), rowkey);

              try {
                mutationsLock.lockInterruptibly();
                mutations.add(clear);
                mutationsSize.addAndGet(clear.size());
                lastAction.set(System.currentTimeMillis());
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FDB_DELETES, Sensision.EMPTY_LABELS, 1);
              } finally {
                if (mutationsLock.isHeldByCurrentThread()) {
                  mutationsLock.unlock();
                }
              }

              continue;
            } // if (io.warp10.continuum.Configuration.INGRESS_METADATA_DELETE_SOURCE.equals(metadata.getSource())

            //
            // Call external plugin
            //

            if (null != directory.plugin) {
              long nano = 0;

              try {
                if (!directory.trustedPlugin) {
                  GTS gts = new GTS(
                      // 128bits
                      new UUID(metadata.getClassId(), metadata.getLabelsId()),
                      metadata.getName(),
                      metadata.getLabels(),
                      metadata.getAttributes());

                  //
                  // If we are doing a metadata update and the GTS is not known, skip the call to store.
                  //

                  if ((io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource()) || io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())) && !directory.plugin.known(gts)) {
                    continue;
                  }

                  nano = System.nanoTime();

                  if (!directory.plugin.store(metadata.getSource(), gts)) {
                    // If we could not store the GTS, stop the directory consumer
                    break;
                  }
                } else {
                  //
                  // If we are doing a metadata update and the GTS is not known, skip the call to store.
                  //

                  if ((io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource()) || io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())) && !((TrustedDirectoryPlugin) directory.plugin).known(metadata)) {
                    continue;
                  }

                  nano = System.nanoTime();

                  if (!((TrustedDirectoryPlugin) directory.plugin).store(metadata)) {
                    // If we could not store the GTS, stop the directory consumer
                    break;
                  }
                }
              } finally {
                nano = System.nanoTime() - nano;
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_CALLS, Sensision.EMPTY_LABELS, 1);
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_TIME_NANOS, Sensision.EMPTY_LABELS, nano);
              }

            } else { // no directory plugin
              //
              // If Metadata comes from Ingress and it is already in the cache, do
              // nothing. Unless we are tracking activity in which case we need to check
              // if the last activity is more recent than the one we have in cache and then update
              // the attributes.
              //

              if (io.warp10.continuum.Configuration.INGRESS_METADATA_SOURCE.equals(metadata.getSource())
                  && directory.metadatas.containsKey(metadata.getName())
                  && directory.metadatas.get(metadata.getName()).containsKey(labelsId)) {

                if (!directory.trackingActivity) {
                  continue;
                }

                //
                // We need to keep track of the activity
                //

                Metadata meta = directory.metadatas.get(metadata.getName()).get(labelsId);

                // If none of the Metadata instances has a last activity recorded, do nothing
                if (!metadata.isSetLastActivity() && !meta.isSetLastActivity()) {
                  continue;
                }

                // If one of the Metadata instances does not have a last activity set, use the one which is defined

                if (!metadata.isSetLastActivity()) {
                  metadata.setLastActivity(meta.getLastActivity());
                } else if (!meta.isSetLastActivity()) {
                  meta.setLastActivity(metadata.getLastActivity());
                } else if (metadata.getLastActivity() -  meta.getLastActivity() < directory.activityWindow) {
                  // both Metadata have a last activity set, but the activity window has not yet passed, so do nothing
                  continue;
                }

                // Update last activity, replace serialized GTS so we have the correct activity timestamp AND
                // the correct attributes when storing. Clear the serialized metadata cache
                meta.setLastActivity(Math.max(meta.getLastActivity(), metadata.getLastActivity()));
                metadata.setLastActivity(meta.getLastActivity());

                // Copy attributes from the currently store Metadata instance
                metadata.setAttributes(meta.getAttributes());

                TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
                metadataBytes = serializer.serialize(meta);

                id = MetadataUtils.id(metadata);
                directory.serializedMetadataCache.remove(id);
              }

              //
              // If metadata is an update, only take it into consideration if the GTS is already known
              //

              if ((io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())
                  || io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource()))
                  && (!directory.metadatas.containsKey(metadata.getName())
                      || !directory.metadatas.get(metadata.getName()).containsKey(labelsId))) {
                continue;
              }
            }

            //
            // Clear the cache if it is an update
            //

            if (io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())
                || io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())) {
              id = MetadataUtils.id(metadata);
              directory.serializedMetadataCache.remove(id);

              //
              // If this is a delta update of attributes, consolidate them
              //

              if (io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())) {
                Metadata meta = directory.metadatas.get(metadata.getName()).get(labelsId);

                for (Entry<String,String> attr: metadata.getAttributes().entrySet()) {
                  if ("".equals(attr.getValue())) {
                    meta.getAttributes().remove(attr.getKey());
                  } else {
                    meta.putToAttributes(attr.getKey(), attr.getValue());
                  }
                }

                // We need to update the attributes with those from 'meta' so we
                // store the up to date version of the Metadata in FoundationDB
                metadata.setAttributes(new HashMap<String,String>(meta.getAttributes()));

                // We re-serialize metadata
                TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
                metadataBytes = serializer.serialize(metadata);
              }

              //
              // Update the last activity
              //

              if (null == directory.plugin) {
                Metadata meta = directory.metadatas.get(metadata.getName()).get(labelsId);

                boolean hasChanged = false;

                if (!metadata.isSetLastActivity() && meta.isSetLastActivity()) {
                  metadata.setLastActivity(meta.getLastActivity());
                  hasChanged = true;
                } else if (metadata.isSetLastActivity() && meta.isSetLastActivity() && meta.getLastActivity() > metadata.getLastActivity()) {
                  // Take the most recent last activity timestamp
                  metadata.setLastActivity(meta.getLastActivity());
                  hasChanged = true;
                }

                if (hasChanged) {
                  // We re-serialize metadata
                  TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
                  metadataBytes = serializer.serialize(metadata);
                }
              }
            }

            //
            // Write Metadata to FoundationDB as it is either new or an updated version\
            // WARNING(hbs): in case of an updated version, we might erase a newer version of
            // the metadata (in case we updated it already but the Kafka offsets were not committed prior to
            // a failure of this Directory process). This will eventually be corrected when the newer version is
            // later re-read from Kafka.
            //

            // Prefix + classId + labelsId
            byte[] rowkey = new byte[Constants.FDB_METADATA_KEY_PREFIX.length + 8 + 8];

            ByteBuffer bb = ByteBuffer.wrap(rowkey).order(ByteOrder.BIG_ENDIAN);
            bb.put(Constants.FDB_METADATA_KEY_PREFIX);
            bb.putLong(classId);
            bb.putLong(labelsId);

            //
            // Encrypt contents
            //

            FDBSet put = null;
            byte[] encrypted = null;

            if (directory.store) {
              if (null != fdbAESKey) {
                encrypted = CryptoUtils.wrap(fdbAESKey, metadataBytes);
                put = new FDBSet(fdbContext.getTenantPrefix(), rowkey, encrypted);
              } else {
                encrypted = metadataBytes;
                put = new FDBSet(fdbContext.getTenantPrefix(), rowkey, metadataBytes);
              }
            }

            try {
              mutationsLock.lockInterruptibly();
              //synchronized (puts) {
              if (directory.store) {
                mutations.add(put);
                mutationsSize.addAndGet(put.size());
                lastAction.set(System.currentTimeMillis());
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FDB_PUTS, Sensision.EMPTY_LABELS, 1);
              }

              if (null != directory.plugin) {
                continue;
              }

              //
              // Internalize Strings
              //

              GTSHelper.internalizeStrings(metadata);

              //
              // Store it in the cache (we dot that in the synchronized section)
              //

              //byte[] classBytes = Arrays.copyOf(data, 8);
              //long classId = Longs.fromByteArray(classBytes);

              try {
                directory.metadatasLock.lockInterruptibly();
                if (!directory.metadatas.containsKey(metadata.getName())) {
                  //directory.metadatas.put(metadata.getName(), new ConcurrentHashMap<Long,Metadata>());
                  // This is done under the synchronization of actionsLock
                  directory.metadatas.put(metadata.getName(), new ConcurrentSkipListMap<Long,Metadata>(ID_COMPARATOR));
                  directory.classNames.put(classId, metadata.getName());
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_CLASSES, Sensision.EMPTY_LABELS, 1);
                }
              } finally {
                if (directory.metadatasLock.isHeldByCurrentThread()) {
                  directory.metadatasLock.unlock();
                }
              }

              //
              // Store per owner class
              //

              String owner = metadata.getLabels().get(Constants.OWNER_LABEL);

              synchronized(directory.classesPerOwner) {
                Map<Long, String> classes = directory.classesPerOwner.get(owner);

                if (null == classes) {
                  classes = new ConcurrentSkipListMap<Long, String>(ID_COMPARATOR);
                  directory.classesPerOwner.put(owner, classes);
                }

                classes.put(metadata.getClassId(), metadata.getName());
              }

              Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_OWNERS, Sensision.EMPTY_LABELS, directory.classesPerOwner.size());

              //
              // Force classId/labelsId in Metadata, we will need them!
              //

              metadata.setClassId(classId);
              metadata.setLabelsId(labelsId);

              // 128bits
              if (null == directory.metadatas.get(metadata.getName()).put(labelsId, metadata)) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, 1);
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, 1);
              }

            } finally {
              if (mutationsLock.isHeldByCurrentThread()) {
                mutationsLock.unlock();
              }
            }
          }
        }
      } catch (Throwable t) {
        LOG.error("", t);
      } finally {
        // Set abort to true in case we exit the 'run' method
        directory.abort.set(true);
        this.localabort.set(true);
        if (null != db) {
          try { db.close(); } catch (Throwable t) {}
        }
      }
    }
  }

  public DirectoryStatsResponse stats(DirectoryStatsRequest request) throws TException {
    return DirectoryUtil.stats(request, null, metadatas, classesPerOwner, LIMIT_CLASS_CARDINALITY, LIMIT_LABELS_CARDINALITY, maxage, SIPHASH_CLASS_LONGS, SIPHASH_LABELS_LONGS, SIPHASH_PSK_LONGS);
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    handleStreaming(target, baseRequest, request, response);
    handleStats(target, baseRequest, request, response);
  }

  void handleStats(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (!Constants.API_ENDPOINT_DIRECTORY_STATS_INTERNAL.equals(target)) {
      return;
    }

    baseRequest.setHandled(true);

    //
    // Read DirectoryRequests from stdin
    //

    BufferedReader br = new BufferedReader(request.getReader());

    while (true) {
      String line = br.readLine();

      if (null == line) {
        break;
      }

      byte[] raw = OrderPreservingBase64.decode(line.getBytes(StandardCharsets.US_ASCII));

      // Extract DirectoryStatsRequest
      TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
      DirectoryStatsRequest req = new DirectoryStatsRequest();

      try {
        deser.deserialize(req, raw);
        DirectoryStatsResponse resp = stats(req);

        response.setContentType("text/plain");
        OutputStream out = response.getOutputStream();

        TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
        byte[] data = ser.serialize(resp);

        OrderPreservingBase64.encodeToStream(data, out);

        out.write('\r');
        out.write('\n');
      } catch (TException te) {
        throw new IOException(te);
      }
    }
  }

  public void handleStreaming(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (!Constants.API_ENDPOINT_DIRECTORY_STREAMING_INTERNAL.equals(target)) {
      return;
    }

    long now = System.currentTimeMillis();

    long nano = System.nanoTime();

    baseRequest.setHandled(true);

    //
    // Extract 'selector'
    //

    String selector = request.getParameter(Constants.HTTP_PARAM_SELECTOR);

    if (null == selector) {
      throw new IOException("Missing parameter '" + Constants.HTTP_PARAM_SELECTOR + "'.");
    }

    // Decode selector

    selector = new String(OrderPreservingBase64.decode(selector.getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8);

    //
    // Check request signature
    //

    String signature = request.getHeader(Constants.getHeader(io.warp10.continuum.Configuration.HTTP_HEADER_DIRECTORY_SIGNATURE));

    if (null == signature) {
      throw new IOException("Missing header '" + Constants.getHeader(io.warp10.continuum.Configuration.HTTP_HEADER_DIRECTORY_SIGNATURE) + "'.");
    }

    //
    // Signature has the form hex(ts):hex(hash)
    //

    String[] subelts = signature.split(":");

    if (2 != subelts.length) {
      throw new IOException("Invalid signature.");
    }

    long nowts = System.currentTimeMillis();
    long sigts = new BigInteger(subelts[0], 16).longValue();
    long sighash = new BigInteger(subelts[1], 16).longValue();

    if (nowts - sigts > this.maxage) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_EXPIRED, Sensision.EMPTY_LABELS, 1);
      throw new IOException("Signature has expired.");
    }

    // Recompute hash of ts:selector

    String tssel = Long.toString(sigts) + ":" + selector;

    byte[] bytes = tssel.getBytes(StandardCharsets.UTF_8);
    long checkedhash = SipHashInline.hash24(SIPHASH_PSK_LONGS[0], SIPHASH_PSK_LONGS[1], bytes, 0, bytes.length);

    if (checkedhash != sighash) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_INVALID, Sensision.EMPTY_LABELS, 1);
      throw new IOException("Corrupted signature");
    }

    boolean hasActiveAfter = null != request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER);
    long activeAfter = hasActiveAfter ? Long.parseLong(request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER)) : 0L;
    boolean hasQuietAfter = null != request.getParameter(Constants.HTTP_PARAM_QUIETAFTER);
    long quietAfter = hasQuietAfter ? Long.parseLong(request.getParameter(Constants.HTTP_PARAM_QUIETAFTER)) : 0L;

    //
    // Parse selector
    //

    Object[] tokens = null;

    try {
      tokens = PARSESELECTOR.parse(selector);
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }

    String classSelector = (String) tokens[0];
    Map<String,String> labelsSelector = (Map<String,String>) tokens[1];

    //
    // Loop over the Metadata, outputing the matching ones
    //

    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("text/plain");

    //
    // Delegate to the external plugin if it is defined
    //

    long count = 0;
    long hits = 0;

    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

    MetadataID id = null;

    OutputStream out = response.getOutputStream();

    if (null != this.plugin) {

      long nanofind = System.nanoTime();
      long time = 0;

      Metadata metadata = new Metadata();

      if (!this.trustedPlugin) {
        try (DirectoryPlugin.GTSIterator iter = this.plugin.find(this.remainder, classSelector, labelsSelector)) {

          while(iter.hasNext()) {
            GTS gts = iter.next();
            nanofind = System.nanoTime() - nanofind;
            time += nanofind;

            metadata.clear();
            metadata.setName(gts.getName());
            metadata.setLabels(gts.getLabels());
            metadata.setAttributes(gts.getAttributes());

            //
            // Recompute class/labels Id
            //

            long classId = GTSHelper.classId(this.SIPHASH_CLASS_LONGS, metadata.getName());
            long labelsId = GTSHelper.labelsId(this.SIPHASH_LABELS_LONGS, metadata.getLabels());

            metadata.setClassId(classId);
            metadata.setLabelsId(labelsId);

            try {
              //
              // Extract id
              //

              id = MetadataUtils.id(id, metadata);

              //
              // Attempt to retrieve serialized content from the cache
              //

              byte[] data = serializedMetadataCache.get(id);

              if (null == data) {
                data = serializer.serialize(metadata);
                synchronized(serializedMetadataCache) {
                  // cache content
                  serializedMetadataCache.put(MetadataUtils.id(metadata),data);
                }
              } else {
                hits++;
              }

              OrderPreservingBase64.encodeToStream(data, out);
              out.write('\r');
              out.write('\n');
              count++;
            } catch (TException te) {
            }
            nanofind = System.nanoTime();
          }

        } catch (Exception e) {
        } finally {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_CALLS, Sensision.EMPTY_LABELS, 1);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_RESULTS, Sensision.EMPTY_LABELS, count);
          Sensision.update(SensisionConstants.CLASS_WARP_DIRECTORY_METADATA_CACHE_HITS, Sensision.EMPTY_LABELS, hits);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_TIME_NANOS, Sensision.EMPTY_LABELS, time);
        }
      } else {
        DirectoryRequest drequest = new DirectoryRequest();
        List<String> csel = new ArrayList<String>(1);
        csel.add(classSelector);
        List<Map<String,String>> lsel = new ArrayList<Map<String,String>>(1);
        lsel.add(labelsSelector);
        drequest.setClassSelectors(csel);
        drequest.setLabelsSelectors(lsel);
        if (hasActiveAfter) {
          drequest.setActiveAfter(activeAfter);
        }
        if (hasQuietAfter) {
          drequest.setQuietAfter(quietAfter);
        }

        try (TrustedDirectoryPlugin.MetadataIterator iter = ((TrustedDirectoryPlugin) this.plugin).find(this.remainder, drequest)) {

          while(iter.hasNext()) {
            metadata = iter.next();
            nanofind = System.nanoTime() - nanofind;
            time += nanofind;

            //
            // Recompute class/labels Id
            //

            long classId = GTSHelper.classId(this.SIPHASH_CLASS_LONGS, metadata.getName());
            long labelsId = GTSHelper.labelsId(this.SIPHASH_LABELS_LONGS, metadata.getLabels());

            metadata.setClassId(classId);
            metadata.setLabelsId(labelsId);

            try {
              //
              // Extract id
              //

              id = MetadataUtils.id(id, metadata);

              //
              // Attempt to retrieve serialized content from the cache
              //

              byte[] data = serializedMetadataCache.get(id);

              if (null == data) {
                data = serializer.serialize(metadata);
                synchronized(serializedMetadataCache) {
                  // cache content
                  serializedMetadataCache.put(MetadataUtils.id(metadata),data);
                }
              } else {
                hits++;
              }

              OrderPreservingBase64.encodeToStream(data, out);
              out.write('\r');
              out.write('\n');
              count++;
            } catch (TException te) {
            }
            nanofind = System.nanoTime();
          }

        } catch (Exception e) {
        } finally {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_CALLS, Sensision.EMPTY_LABELS, 1);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_RESULTS, Sensision.EMPTY_LABELS, count);
          Sensision.update(SensisionConstants.CLASS_WARP_DIRECTORY_METADATA_CACHE_HITS, Sensision.EMPTY_LABELS, hits);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_TIME_NANOS, Sensision.EMPTY_LABELS, time);
        }
      }
      return;
    } else { // No Plugin

      String exactClassName = null;
      SmartPattern classSmartPattern;

      List<String> missingLabels = Constants.ABSENT_LABEL_SUPPORT ? new ArrayList<String>() : null;

      if (classSelector.startsWith("=") || !classSelector.startsWith("~")) {
        exactClassName = classSelector.startsWith("=") ? classSelector.substring(1) : classSelector;
        classSmartPattern = new SmartPattern(exactClassName);
      } else {
        classSmartPattern = new SmartPattern(Pattern.compile(classSelector.substring(1)));
      }

      Map<String,SmartPattern> labelPatterns = new LinkedHashMap<String,SmartPattern>();

      for (Entry<String,String> entry: labelsSelector.entrySet()) {
        String label = entry.getKey();
        String expr = entry.getValue();
        SmartPattern pattern;

        if (null != missingLabels && ("=".equals(expr) || "".equals(expr))) {
          missingLabels.add(label);
          continue;
        }

        if (expr.startsWith("=") || !expr.startsWith("~")) {
          pattern = new SmartPattern(expr.startsWith("=") ? expr.substring(1) : expr);
        } else {
          pattern = new SmartPattern(Pattern.compile(expr.substring(1)));
        }

        labelPatterns.put(label,  pattern);
      }

      //
      // Loop over the class names to find matches
      //

      Collection<String> classNames = new ArrayList<String>();

      if (null != exactClassName) {
        // If the class name is an exact match, check if it is known, if not, return
        if(!this.metadatas.containsKey(exactClassName)) {
          return;
        }
        classNames.add(exactClassName);
      } else {
        //
        // Extract per owner classes if owner selector exists
        //

        if (labelsSelector.size() > 0) {
          String ownersel = labelsSelector.get(Constants.OWNER_LABEL);

          if (null != ownersel && ownersel.startsWith("=")) {
            Map<Long,String> cpo = classesPerOwner.get(ownersel.substring(1));
            if (null != cpo) {
              classNames = cpo.values();
            } else {
              classNames = new ArrayList<String>();
            }
          } else {
            classNames = this.classNames.values();
          }
        } else {
          classNames = this.classNames.values();
        }
      }

      List<String> labelNames = new ArrayList<String>(labelPatterns.size());
      List<SmartPattern> labelSmartPatterns = new ArrayList<SmartPattern>(labelPatterns.size());
      List<String> labelValues = new ArrayList<String>(labelPatterns.size());

      for(Entry<String,SmartPattern> entry: labelPatterns.entrySet()) {
        labelNames.add(entry.getKey());
        labelSmartPatterns.add(entry.getValue());
        labelValues.add(null);
      }

      long labelsComparisons = 0;
      long classesInspected = 0;
      long classesMatched = 0;
      long metadataInspected = 0;

      for (String className: classNames) {
        classesInspected++;

        //
        // If class matches, check all labels for matches
        //

        if (classSmartPattern.matches(className)) {
          classesMatched++;
          Map<Long,Metadata> classMetadatas = this.metadatas.get(className);
          if (null == classMetadatas) {
            continue;
          }
          for (Metadata metadata: classMetadatas.values()) {
            metadataInspected++;
            boolean exclude = false;

            //
            // Check activity
            //

            if (hasActiveAfter && metadata.getLastActivity() < activeAfter) {
              continue;
            }

            if (hasQuietAfter && metadata.getLastActivity() >= quietAfter) {
              continue;
            }

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

              labelValues.set(idx++, labelValue);
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

            for (int i = 0; i < labelNames.size(); i++) {
              labelsComparisons++;
              if (!labelSmartPatterns.get(i).matches(labelValues.get(i))) {
                exclude = true;
                break;
              }
            }

            if (exclude) {
              continue;
            }

            try {
              //
              // Extract id
              //

              id = MetadataUtils.id(id, metadata);

              //
              // Attempt to retrieve serialized content from the cache
              //

              byte[] data = serializedMetadataCache.get(id);

              if (null == data) {
                data = serializer.serialize(metadata);
                synchronized(serializedMetadataCache) {
                  // cache content
                  serializedMetadataCache.put(MetadataUtils.id(metadata),data);
                }
              } else {
                hits++;
              }

              OrderPreservingBase64.encodeToStream(data, out);
              out.write('\r');
              out.write('\n');
              count++;
            } catch (TException te) {
              continue;
            }
          }
        }
      }

      long nownano = System.nanoTime();

      LoggingEvent event = LogUtil.setLoggingEventAttribute(null, LogUtil.DIRECTORY_SELECTOR, selector);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_REQUEST_TIMESTAMP, now);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_RESULTS, count);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_NANOS, nownano - nano);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_METADATA_INSPECTED, metadataInspected);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_CLASSES_INSPECTED, classesInspected);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_CLASSES_MATCHED, classesMatched);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_LABELS_COMPARISONS, labelsComparisons);

      String eventstr = LogUtil.serializeLoggingEvent(this.keystore, event);

      LOG.info("Search returned " + count + " results in " + ((nownano - nano) / 1000000.0D) + " ms, inspected " + metadataInspected + " metadatas in " + classesInspected + " classes (" + classesMatched + " matched) and performed " + labelsComparisons + " comparisons. EVENT=" + eventstr);
    }

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_REQUESTS, Sensision.EMPTY_LABELS, 1);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_RESULTS, Sensision.EMPTY_LABELS, count);
    Sensision.update(SensisionConstants.CLASS_WARP_DIRECTORY_METADATA_CACHE_HITS, Sensision.EMPTY_LABELS, hits);
    nano = System.nanoTime() - nano;
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_TIME_US, Sensision.EMPTY_LABELS, nano / 1000);
  }
}
