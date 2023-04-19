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

package io.warp10.continuum.egress;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.google.common.base.Preconditions;

import io.warp10.SSLUtils;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.crypto.KeyStore;
import io.warp10.fdb.FDBStoreClient;

/**
 * This is the class which ingests metrics.
 */
public class Egress {

  private static final String DEFAULT_THREADPOOL_SIZE = "200";

  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    Configuration.EGRESS_HOST,
    Configuration.EGRESS_PORT,
    Configuration.EGRESS_ACCEPTORS,
    Configuration.EGRESS_SELECTORS,
    Configuration.EGRESS_IDLE_TIMEOUT,
    Configuration.EGRESS_FETCH_BATCHSIZE,
    Configuration.DIRECTORY_ZK_QUORUM,
    Configuration.DIRECTORY_ZK_ZNODE,
    Configuration.DIRECTORY_PSK,
  };

  private final Server server;

  private final KeyStore keystore;

  private final Properties properties;

  public Egress(KeyStore keystore, Properties props, boolean fetcher) throws Exception {

    this.properties = (Properties) props.clone();
    this.keystore = keystore;

    //
    // Make sure all required configuration is present
    //

    for (String required: REQUIRED_PROPERTIES) {
      if (fetcher) {
        if (Configuration.DIRECTORY_ZK_QUORUM.equals(required)) {
          continue;
        }
        if (Configuration.DIRECTORY_ZK_ZNODE.equals(required)) {
          continue;
        }
        if (Configuration.DIRECTORY_PSK.equals(required)) {
          continue;
        }
      }

      Preconditions.checkNotNull(props.getProperty(required), "Missing configuration parameter '%s'.", required);
    }

    //
    // Extract parameters from 'props'
    //

    boolean useHttp = (null != props.getProperty(Configuration.EGRESS_PORT));
    boolean useHttps = (null != props.getProperty(Configuration.EGRESS_PREFIX + Configuration._SSL_PORT));

    List<Connector> connectors = new ArrayList<Connector>();

    int maxThreads = Integer.parseInt(props.getProperty(Configuration.EGRESS_JETTY_THREADPOOL, DEFAULT_THREADPOOL_SIZE));
    BlockingArrayQueue<Runnable> queue = null;

    if (props.containsKey(Configuration.EGRESS_JETTY_MAXQUEUESIZE)) {
      int queuesize = Integer.parseInt(props.getProperty(Configuration.EGRESS_JETTY_MAXQUEUESIZE));
      queue = new BlockingArrayQueue<Runnable>(queuesize);
    }

    QueuedThreadPool queuedThreadPool = new QueuedThreadPool(maxThreads, 8, 60000, queue);
    queuedThreadPool.setName("Warp Egress Jetty Thread");
    server = new Server(queuedThreadPool);

    if (useHttp) {
      int port = Integer.valueOf(props.getProperty(Configuration.EGRESS_PORT));
      String host = props.getProperty(Configuration.EGRESS_HOST);
      int tcpBacklog = Integer.valueOf(props.getProperty(Configuration.EGRESS_TCP_BACKLOG, "0"));
      int acceptors = Integer.valueOf(props.getProperty(Configuration.EGRESS_ACCEPTORS));
      int selectors = Integer.valueOf(props.getProperty(Configuration.EGRESS_SELECTORS));
      long idleTimeout = Long.parseLong(props.getProperty(Configuration.EGRESS_IDLE_TIMEOUT));

      ServerConnector connector = new ServerConnector(server, acceptors, selectors);
      connector.setIdleTimeout(idleTimeout);
      connector.setPort(port);
      connector.setHost(host);
      connector.setAcceptQueueSize(tcpBacklog);
      connector.setName("Continuum Egress HTTP");

      connectors.add(connector);
    }

    if (useHttps) {
      ServerConnector connector = SSLUtils.getConnector(server, Configuration.EGRESS_PREFIX);
      connector.setName("Continuum Egress HTTPS");
      connectors.add(connector);
    }

    boolean enableMobius = !("true".equals(props.getProperty(Configuration.WARP_MOBIUS_DISABLE)));
    boolean enableREL = !("true".equals(properties.getProperty(Configuration.WARP_INTERACTIVE_DISABLE)));

    extractKeys(props);

    //
    // Start Jetty server
    //

    server.setConnectors(connectors.toArray(new Connector[connectors.size()]));

    HandlerList handlers = new HandlerList();

    StoreClient storeClient = new FDBStoreClient(this.keystore, this.properties);

    Handler cors = new CORSHandler();
    handlers.addHandler(cors);

    if (!fetcher) {
      DirectoryClient directoryClient = new ThriftDirectoryClient(this.keystore, this.properties);

      GzipHandler gzip = new GzipHandler();
      EgressExecHandler egressExecHandler = new EgressExecHandler(this.keystore, this.properties, directoryClient, storeClient);
      gzip.setHandler(egressExecHandler);
      gzip.setMinGzipSize(23);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);

      gzip = new GzipHandler();
      gzip.setHandler(new EgressFetchHandler(this.keystore, this.properties, directoryClient, storeClient));
      gzip.setMinGzipSize(23);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);

      gzip = new GzipHandler();
      gzip.setHandler(new EgressFindHandler(this.keystore, directoryClient));
      gzip.setMinGzipSize(23);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);

      gzip = new GzipHandler();
      gzip.setHandler(new EgressSplitsHandler(this.keystore, directoryClient, (FDBStoreClient) storeClient));
      gzip.setMinGzipSize(23);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);

      if (enableMobius) {
        EgressMobiusHandler mobiusHandler = new EgressMobiusHandler(storeClient, directoryClient, this.properties);
        handlers.addHandler(mobiusHandler);
      }

      if (enableREL) {
        EgressInteractiveHandler erel = new EgressInteractiveHandler(keystore, properties, directoryClient, storeClient);
        handlers.addHandler(erel);
      }
    } else {
      GzipHandler gzip = new GzipHandler();
      gzip.setHandler(new EgressFetchHandler(this.keystore, this.properties, null, storeClient));
      gzip.setMinGzipSize(23);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);
    }

    server.setHandler(handlers);

    JettyUtil.setSendServerVersion(server, false);

    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Extract Ingress related keys and populate the KeyStore with them.
   *
   * @param props Properties from which to extract the key specs
   */
  private void extractKeys(Properties props) {
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_FDB_DATA, props, Configuration.EGRESS_FDB_DATA_AES, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_FETCHER, props, Configuration.EGRESS_FETCHER_AES, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_DIRECTORY_PSK, props, Configuration.DIRECTORY_PSK, 128);

    this.keystore.forget();
  }
}
