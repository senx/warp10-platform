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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.warp10.Revision;
import io.warp10.SSLUtils;
import io.warp10.WarpConfig;
import io.warp10.WarpDist;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.egress.CORSHandler;
import io.warp10.continuum.egress.EgressExecHandler;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.egress.EgressFindHandler;
import io.warp10.continuum.egress.EgressInteractiveHandler;
import io.warp10.continuum.egress.EgressMobiusHandler;
import io.warp10.continuum.ingress.DatalogForwarder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.ParallelGTSDecoderIteratorWrapper;
import io.warp10.continuum.store.StoreClient;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OSSKeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.UnsecureKeyStore;
import io.warp10.fdb.FDBContext;
import io.warp10.leveldb.WarpDB;
import io.warp10.script.ScriptRunner;
import io.warp10.script.WarpScriptLib;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.AbstractWarp10Plugin;

public class Warp extends WarpDist implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Warp.class);

  private static final String DEFAULT_THREADPOOL_SIZE = "200";

  private static final String DEFAULT_HTTP_ACCEPTORS = "2";
  private static final String DEFAULT_HTTP_SELECTORS = "4";

  private static WarpDB db;

  private static boolean standaloneMode = false;

  private static String backend = null;

  private static int port;

  private static String host;

  private static Set<Path> datalogSrcDirs = Collections.unmodifiableSet(new HashSet<Path>());

  private static final String[] REQUIRED_PROPERTIES = {
    Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE,
    Configuration.PLASMA_FRONTEND_WEBSOCKET_MAXMESSAGESIZE,
    Configuration.WARP_HASH_CLASS,
    Configuration.WARP_HASH_LABELS,
    Configuration.WARP_HASH_TOKEN,
    Configuration.WARP_HASH_APP,
    Configuration.WARP_AES_TOKEN,
    Configuration.WARP_AES_SCRIPTS,
    Configuration.CONFIG_WARPSCRIPT_UPDATE_ENDPOINT,
    Configuration.CONFIG_WARPSCRIPT_META_ENDPOINT,
    Configuration.WARP_TIME_UNITS,
  };

  public Warp() {
    // TODO Auto-generated constructor stub
  }

  public static void main(String[] args) throws Exception {
    // Indicate standalone mode is on
    standaloneMode = true;

    System.setProperty("java.awt.headless", "true");

    System.out.println();
    System.out.println(Constants.WARP10_BANNER);
    System.out.println("  Revision " + Revision.REVISION);
    System.out.println();

    if (StandardCharsets.UTF_8 != Charset.defaultCharset()) {
      throw new RuntimeException("Default encoding MUST be UTF-8 but it is " + Charset.defaultCharset() + ". Aborting.");
    }

    Map<String, String> labels = new HashMap<String, String>();
    labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "standalone");
    Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);

    setProperties(args);

    Properties properties = getProperties();

    backend = properties.getProperty(Configuration.BACKEND);

    boolean useLevelDB = false;
    boolean useFDB = false;
    boolean nullbackend = false;
    boolean plasmabackend = false;
    boolean inmemory = false;

    boolean analyticsEngineOnly = "true".equals(properties.getProperty(Configuration.ANALYTICS_ENGINE_ONLY));

    if (analyticsEngineOnly) {
      nullbackend = true;
      if (null != backend && !Constants.BACKEND_NULL.equals(backend)) {
        LOG.warn("Backend specification '" + backend + "' ignored since '" + Configuration.ANALYTICS_ENGINE_ONLY + " is true.");
      }
    } else {
      if (null == backend) {
        throw new RuntimeException("Backend specification '" + Configuration.BACKEND + "' MUST be set since Warp 10 3.0");
      }
      switch(backend) {
        case Constants.BACKEND_LEVELDB:
          useLevelDB = true;
          break;
        case Constants.BACKEND_FDB:
          useFDB = true;
          break;
        case Constants.BACKEND_NULL:
          nullbackend = true;
          break;
        case Constants.BACKEND_PLASMA:
          plasmabackend = true;
          break;
        case Constants.BACKEND_MEMORY:
          inmemory = true;
          break;
        default:
          throw new RuntimeException("Missing valid '" + Configuration.BACKEND + "' specification.");
      }
    }


    boolean accelerator = "true".equals(properties.getProperty(Configuration.ACCELERATOR));

    if (inmemory && accelerator) {
      throw new RuntimeException("Accelerator mode cannot be enabled when '" + Configuration.BACKEND + "' is set to '" + Constants.BACKEND_MEMORY + "'.");
    }

    boolean enablePlasma = !("true".equals(properties.getProperty(Configuration.WARP_PLASMA_DISABLE)));
    boolean enableMobius = !("true".equals(properties.getProperty(Configuration.WARP_MOBIUS_DISABLE)));
    boolean enableStreamUpdate = !("true".equals(properties.getProperty(Configuration.WARP_STREAMUPDATE_DISABLE)));
    boolean enableREL = !("true".equals(properties.getProperty(Configuration.WARP_INTERACTIVE_DISABLE)));

    for (String property: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(property), "Property '" + property + "' MUST be set.");
    }

    //
    // Initialize KeyStore
    //

    KeyStore keystore;

    if (properties.containsKey(Configuration.OSS_MASTER_KEY)) {
      keystore = new OSSKeyStore(properties.getProperty(Configuration.OSS_MASTER_KEY));
    } else {
      keystore = new UnsecureKeyStore();
    }

    //
    // Decode generic keys
    // We do that first so those keys do not have precedence over the specific
    // keys.
    //

    Map<String,String> secrets = new HashMap<String,String>();
    Set<Object> keys = new HashSet<Object>();
    for (Entry<Object,Object> entry: properties.entrySet()) {
      if (entry.getKey().toString().startsWith(Configuration.WARP_KEY_PREFIX)) {
        byte[] key = keystore.decodeKey(entry.getValue().toString());
        if (null == key) {
          throw new RuntimeException("Unable to decode key '" + entry.getKey() + "'.");
        }
        keystore.setKey(entry.getKey().toString().substring(Configuration.WARP_KEY_PREFIX.length()), key);
        keys.add(entry.getKey());
      } else if (entry.getKey().toString().startsWith(Configuration.WARP_SECRET_PREFIX)) {
        byte[] secret = keystore.decodeKey(entry.getValue().toString());
        if (null == secret) {
          throw new RuntimeException("Unable to decode secret '" + entry.getKey() + "'.");
        }
        // Encode secret as OPB64 and store it
        secrets.put(entry.getKey().toString().substring(Configuration.WARP_SECRET_PREFIX.length()), OrderPreservingBase64.encodeToString(secret));
        keys.add(entry.getKey());
      }
    }

    //
    // Remove keys and secrets from the properties
    //
    for (Object key: keys) {
      properties.remove(key);
    }

    // Inject the secrets
    properties.putAll(secrets);

    extractKeys(keystore, properties);

    setKeyStore(keystore);

    //
    // Initialize levelDB
    //

    Options options = null;

    if (useLevelDB) {
      options = new Options();
      options.createIfMissing("true".equals(properties.getProperty(Configuration.LEVELDB_CREATE_IF_MISSING)));

      options.errorIfExists("true".equals(properties.getProperty(Configuration.LEVELDB_ERROR_IF_EXISTS)));

      if (properties.containsKey(Configuration.LEVELDB_MAXOPENFILES)) {
        int maxOpenFiles = Integer.parseInt(properties.getProperty(Configuration.LEVELDB_MAXOPENFILES));
        options.maxOpenFiles(maxOpenFiles);
      }

      if (null != properties.getProperty(Configuration.LEVELDB_CACHE_SIZE)) {
        options.cacheSize(Long.parseLong(properties.getProperty(Configuration.LEVELDB_CACHE_SIZE)));
      }

      if (null != properties.getProperty(Configuration.LEVELDB_WRITEBUFFER_SIZE)) {
        options.writeBufferSize(Integer.parseInt(properties.getProperty(Configuration.LEVELDB_WRITEBUFFER_SIZE)));
      }

      if (null != properties.getProperty(Configuration.LEVELDB_COMPRESSION_TYPE)) {
        if ("snappy".equalsIgnoreCase(properties.getProperty(Configuration.LEVELDB_COMPRESSION_TYPE))) {
          options.compressionType(CompressionType.SNAPPY);
        } else {
          options.compressionType(CompressionType.NONE);
        }
      }

      if (null != properties.getProperty(Configuration.LEVELDB_BLOCK_SIZE)) {
        options.blockSize(Integer.parseInt(properties.getProperty(Configuration.LEVELDB_BLOCK_SIZE)));
      }

      if (null != properties.getProperty(Configuration.LEVELDB_BLOCK_RESTART_INTERVAL)) {
        options.blockRestartInterval(Integer.parseInt(properties.getProperty(Configuration.LEVELDB_BLOCK_RESTART_INTERVAL)));
      }

      if (null != properties.getProperty(Configuration.LEVELDB_VERIFY_CHECKSUMS)) {
        options.verifyChecksums("true".equals(properties.getProperty(Configuration.LEVELDB_VERIFY_CHECKSUMS)));
      }

      if (null != properties.getProperty(Configuration.LEVELDB_PARANOID_CHECKS)) {
        options.paranoidChecks("true".equals(properties.getProperty(Configuration.LEVELDB_PARANOID_CHECKS)));
      }

      //
      // Attempt to load JNI library, fallback to pure java in case of error
      //

      boolean nativedisabled = "true".equals(properties.getProperty(Configuration.LEVELDB_NATIVE_DISABLE));
      boolean javadisabled = "true".equals(properties.getProperty(Configuration.LEVELDB_JAVA_DISABLE));
      String home = properties.getProperty(Configuration.LEVELDB_HOME);
      db = new WarpDB(nativedisabled, javadisabled, home, options);
    }

    // Register shutdown hook to close the DB.
    Runtime.getRuntime().addShutdownHook(new Thread(new Warp()));

    WarpScriptLib.registerExtensions();

    //
    // Initialize ThrottlingManager
    //

    ThrottlingManager.init();

    //
    // Create Jetty server
    //

    int maxThreads = Integer.parseInt(properties.getProperty(Configuration.STANDALONE_JETTY_THREADPOOL, DEFAULT_THREADPOOL_SIZE));
    BlockingArrayQueue<Runnable> queue = null;

    if (properties.containsKey(Configuration.STANDALONE_JETTY_MAXQUEUESIZE)) {
      int queuesize = Integer.parseInt(properties.getProperty(Configuration.STANDALONE_JETTY_MAXQUEUESIZE));
      queue = new BlockingArrayQueue<Runnable>(queuesize);
    }

    QueuedThreadPool queuedThreadPool = new QueuedThreadPool(maxThreads, 8, 60000, queue);
    queuedThreadPool.setName("Warp Jetty Thread");
    Server server = new Server(queuedThreadPool);

    boolean useHTTPS = null != properties.getProperty(Configuration.STANDALONE_PREFIX + Configuration._SSL_PORT);
    boolean useHTTP = null != properties.getProperty(Configuration.STANDALONE_PORT);

    ServerConnector httpConnector = null;
    ServerConnector httpsConnector = null;

    if (!useHTTPS && !useHTTP) {
      throw new RuntimeException("Missing '" + Configuration.STANDALONE_PORT + "' or '" + Configuration.STANDALONE_PREFIX + Configuration._SSL_PORT + "' configuration");
    }

    List<Connector> connectors = new ArrayList<Connector>(2);

    if (useHTTP) {
      int acceptors = Integer.valueOf(properties.getProperty(Configuration.STANDALONE_ACCEPTORS, DEFAULT_HTTP_ACCEPTORS));
      int selectors = Integer.valueOf(properties.getProperty(Configuration.STANDALONE_SELECTORS, DEFAULT_HTTP_SELECTORS));
      port = Integer.valueOf(properties.getProperty(Configuration.STANDALONE_PORT));
      host = properties.getProperty(Configuration.STANDALONE_HOST, "0.0.0.0");
      int tcpBacklog = Integer.valueOf(properties.getProperty(Configuration.STANDALONE_TCP_BACKLOG, "0"));

      httpConnector = new ServerConnector(server, acceptors, selectors);

      httpConnector.setPort(port);
      httpConnector.setAcceptQueueSize(tcpBacklog);

      if (null != host) {
        httpConnector.setHost(host);
      }

      String idle = properties.getProperty(Configuration.STANDALONE_IDLE_TIMEOUT);

      if (null != idle) {
        httpConnector.setIdleTimeout(Long.parseLong(idle));
      }

      httpConnector.setName("Warp 10 Standalone HTTP Endpoint");

      connectors.add(httpConnector);
    }

    if (useHTTPS) {
      httpsConnector = SSLUtils.getConnector(server, Configuration.STANDALONE_PREFIX);
      httpsConnector.setName("Warp 10 Standalone HTTPS Endpoint");
      connectors.add(httpsConnector);
    }

    server.setConnectors(connectors.toArray(new Connector[connectors.size()]));

    HandlerList handlers = new HandlerList();

    Handler cors = new CORSHandler();
    handlers.addHandler(cors);

    StandaloneDirectoryClient sdc = null;
    StoreClient scc = null;

    if (inmemory) {
      sdc = new StandaloneDirectoryClient(null, keystore);

      sdc.setActivityWindow(Long.parseLong(properties.getProperty(Configuration.INGRESS_ACTIVITY_WINDOW, "0")));

      scc = new StandaloneChunkedMemoryStore(properties, keystore);
      ((StandaloneChunkedMemoryStore) scc).setDirectoryClient(sdc);
      ((StandaloneChunkedMemoryStore) scc).load();
    } else if (plasmabackend) {
      sdc = new StandaloneDirectoryClient(null, keystore);
      scc = new PlasmaStoreClient();
    } else if (nullbackend) {
      sdc = new NullDirectoryClient(keystore);
      scc = new NullStoreClient();
    } else {
      if (useLevelDB) {
        sdc = new StandaloneDirectoryClient(db, keystore);
        scc = new StandaloneStoreClient(db, keystore, properties);
      } else if (useFDB) {
        FDBContext fdbContext = new FDBContext(properties.getProperty(Configuration.DIRECTORY_FDB_CLUSTERFILE), properties.getProperty(Configuration.DIRECTORY_FDB_TENANT));
        sdc = new StandaloneDirectoryClient(fdbContext, keystore);
        scc = new StandaloneFDBStoreClient(keystore, properties);
      }

      if (accelerator) {
        scc = new StandaloneAcceleratedStoreClient(sdc, scc);
      }
    }

    if (null != WarpConfig.getProperty(Configuration.DATALOG_DIR) && null != WarpConfig.getProperty(Configuration.DATALOG_SHARDS)) {
      sdc = new StandaloneShardedDirectoryClientWrapper(keystore, sdc);
      scc = new StandaloneShardedStoreClientWrapper(keystore, scc);
    }

    // When using FDB, don't rely on Standalone's specific parallel scanner implementation, use the one from FDBStoreClient
    // otherwise we may end up in an endless loop attempting to schedule workers
    if (ParallelGTSDecoderIteratorWrapper.useParallelScanners() && !Constants.BACKEND_FDB.equals(backend)) {
      scc = new StandaloneParallelStoreClientWrapper(scc);
    }

    if (properties.containsKey(Configuration.RUNNER_ROOT)) {
      if (!properties.containsKey(Configuration.RUNNER_ENDPOINT)) {
        properties.setProperty(Configuration.RUNNER_ENDPOINT, "");
        StandaloneScriptRunner runner = new StandaloneScriptRunner(properties, keystore.clone(), scc, sdc, properties);
      } else {
        //
        // Allocate a normal runner
        //
        ScriptRunner runner = new ScriptRunner(keystore.clone(), properties);
      }
    }

    //
    // Start the Datalog Forwarders
    //

    if (!analyticsEngineOnly && properties.containsKey(Configuration.DATALOG_FORWARDERS)) {
      // Extract the names of the forwarders and start them all, ensuring we only start each one once
      String[] forwarders = properties.getProperty(Configuration.DATALOG_FORWARDERS).split(",");

      Set<String> names = new HashSet<String>();
      for (String name: forwarders) {
        names.add(name.trim());
      }

      Set<Path> srcDirs = new HashSet<Path>();

      Path datalogdir = new File(properties.getProperty(Configuration.DATALOG_DIR)).toPath().toRealPath();

      for (String name: names) {
        DatalogForwarder forwarder = new DatalogForwarder(name, keystore, properties);

        Path root = forwarder.getRootDir().toRealPath();

        if (datalogdir.equals(root)) {
          throw new RuntimeException("Datalog directory '" + datalogdir + "' cannot be used as a forwarder source.");
        }

        if (!srcDirs.add(root)) {
          throw new RuntimeException("Duplicate datalog source directory '" + root + "'.");
        }
      }

      datalogSrcDirs = Collections.unmodifiableSet(srcDirs);

    } else if (!analyticsEngineOnly && properties.containsKey(Configuration.DATALOG_FORWARDER_SRCDIR) && properties.containsKey(Configuration.DATALOG_FORWARDER_DSTDIR)) {
      Path datalogdir = new File(properties.getProperty(Configuration.DATALOG_DIR)).toPath().toRealPath();
      DatalogForwarder forwarder = new DatalogForwarder(keystore, properties);
      Path root = forwarder.getRootDir().toRealPath();
      if (datalogdir.equals(root)) {
        throw new RuntimeException("Datalog directory '" + datalogdir + "' cannot be used as the source directory of a forwarder.");
      }
    }

    //
    // Enable the ThrottlingManager (not
    //

    if (!analyticsEngineOnly) {
      ThrottlingManager.enable();
    }

    GzipHandler gzip = new GzipHandler();
    EgressExecHandler egressExecHandler = new EgressExecHandler(keystore, properties, sdc, scc);
    gzip.setHandler(egressExecHandler);
    gzip.setMinGzipSize(0);
    gzip.addIncludedMethods("POST");
    handlers.addHandler(gzip);
    setEgress(true);

    if (!analyticsEngineOnly) {
      gzip = new GzipHandler();
      StandaloneIngressHandler sih = new StandaloneIngressHandler(keystore, sdc, scc);
      gzip.setHandler(sih);
      gzip.setMinGzipSize(0);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);

      gzip = new GzipHandler();
      gzip.setHandler(new EgressFindHandler(keystore, sdc));
      gzip.setMinGzipSize(0);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);

      if ("true".equals(properties.getProperty(Configuration.STANDALONE_SPLITS_ENABLE))) {
        gzip = new GzipHandler();
        gzip.setHandler(new StandaloneSplitsHandler(keystore, sdc));
        gzip.setMinGzipSize(0);
        gzip.addIncludedMethods("POST");
        handlers.addHandler(gzip);
      }

      gzip = new GzipHandler();
      StandaloneDeleteHandler sdh = new StandaloneDeleteHandler(keystore, sdc, scc);
      sdh.setPlugin(sih.getPlugin());
      gzip.setHandler(sdh);
      gzip.setMinGzipSize(0);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);

      if (enablePlasma) {
        StandalonePlasmaHandler plasmaHandler = new StandalonePlasmaHandler(keystore, properties, sdc);
        scc.addPlasmaHandler(plasmaHandler);
        handlers.addHandler(plasmaHandler);
      }

      if (enableStreamUpdate) {
        StandaloneStreamUpdateHandler streamUpdateHandler = new StandaloneStreamUpdateHandler(keystore, properties, sdc, scc);
        streamUpdateHandler.setPlugin(sih.getPlugin());
        handlers.addHandler(streamUpdateHandler);
      }

      gzip = new GzipHandler();
      gzip.setHandler(new EgressFetchHandler(keystore, properties, sdc, scc));
      gzip.setMinGzipSize(0);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);
    }

    if (enableMobius) {
      EgressMobiusHandler mobiusHandler = new EgressMobiusHandler(scc, sdc, properties);
      handlers.addHandler(mobiusHandler);
    }

    if (enableREL) {
      EgressInteractiveHandler erel = new EgressInteractiveHandler(keystore, properties, sdc, scc);
      handlers.addHandler(erel);
    }

    server.setHandler(handlers);

    JettyUtil.setSendServerVersion(server, false);

    // Clear master key from memory
    keystore.forget();

    //
    // Register the plugins after we've cleared the master key
    //

    AbstractWarp10Plugin.registerPlugins();

    try {
      if (useHTTP) {
        LOG.info("#### standalone.endpoint " + InetAddress.getByName(host) + ":" + port);
      }
      if (useHTTPS) {
        LOG.info("#### standalone.ssl.endpoint " + InetAddress.getByName(httpsConnector.getHost()) + ":" + httpsConnector.getPort());
      }
      server.start();
    } catch (Exception e) {
      server.stop();
      throw new RuntimeException(e);
    }

    // Retrieve actual local port
    if (null != httpConnector) {
      port = httpConnector.getLocalPort();
    }

    WarpDist.setInitialized(true);
    LOG.info("## Your Warp 10 setup:");
    LOG.info("## - WARP10_HEAP:              " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory()));
    LOG.info("## - WARP10_HEAP_MAX:          " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().maxMemory()));
    LOG.info("## - " + Configuration.WARPSCRIPT_MAX_FETCH + ":      " + properties.getProperty(Configuration.WARPSCRIPT_MAX_FETCH));
    LOG.info("## - " + Configuration.WARPSCRIPT_MAX_FETCH_HARD + ": " + properties.getProperty(Configuration.WARPSCRIPT_MAX_FETCH_HARD));
    LOG.info("## - " + Configuration.WARPSCRIPT_MAX_OPS + ":        " + properties.getProperty(Configuration.WARPSCRIPT_MAX_OPS));
    LOG.info("## - " + Configuration.WARPSCRIPT_MAX_OPS_HARD + ":   " + properties.getProperty(Configuration.WARPSCRIPT_MAX_OPS_HARD));

    try {
      while (true) {
        try {
          Thread.sleep(60000L);
        } catch (InterruptedException ie) {
        }
      }
    } catch (Throwable t) {
      LOG.error(t.getMessage());
      server.stop();
    }
  }

  public static boolean isStandaloneMode() {
    return standaloneMode;
  }

  public static String getBackend() {
    return backend;
  }

  public static int getPort() {
    return port;
  }

  public static String getHost() {
    return host;
  }

  @Override
  public void run() {
    try {
      if (null != db) {
        synchronized (db) {
          db.close();
          db = null;
        }
        LOG.info("LevelDB was safely closed.");
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  /**
   * Extract Ingress related keys and populate the KeyStore with them.
   *
   * @param props Properties from which to extract the key specs
   */
  public static void extractKeys(KeyStore keystore, Properties props) {
    WarpDist.extractKeys(keystore, props);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_LEVELDB_METADATA, props, Configuration.LEVELDB_METADATA_AES, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_LEVELDB_DATA, props, Configuration.LEVELDB_DATA_AES, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_FETCH_PSK, props, Configuration.CONFIG_FETCH_PSK, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_RUNNER_PSK, props, Configuration.RUNNER_PSK, 128, 192, 256);
  }

  public static WarpDB getDB() {
    return db;
  }

  public static Set<Path> getDatalogSrcDirs() {
    return datalogSrcDirs;
  }
}
