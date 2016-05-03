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

package io.warp10.continuum.geo;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.KafkaOffsetCounters;
import io.warp10.continuum.KafkaSynchronizedConsumerPool;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.WarpException;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.ConsumerFactory;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.Hook;
import io.warp10.continuum.egress.ThriftDirectoryClient;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.KafkaDataMessage;
import io.warp10.continuum.store.thrift.data.KafkaDataMessageType;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.continuum.thrift.data.GeoDirectoryRequest;
import io.warp10.continuum.thrift.data.GeoDirectoryResponse;
import io.warp10.continuum.thrift.data.GeoDirectorySubscriptions;
import io.warp10.continuum.thrift.service.GeoDirectoryService;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.PARSESELECTOR;
import io.warp10.sensision.Sensision;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.CreateMode;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.python.jline.internal.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.geoxp.oss.jarjar.org.bouncycastle.util.encoders.Hex;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceInstanceBuilder;
import com.netflix.curator.x.discovery.ServiceType;

/**
 * This class manages indices of GTS (classid + labelsid) according to recent locations
 */
public class GeoDirectory extends AbstractHandler implements Runnable, GeoDirectoryService.Iface {

  public static final String INSTANCE_PAYLOAD_MODULUS = "modulus";
  public static final String INSTANCE_PAYLOAD_REMAINDER = "remainder";
  public static final String INSTANCE_PAYLOAD_GEODIR = "geodir";
  public static final String INSTANCE_PAYLOAD_THRIFT_MAXFRAMELEN = "thrift.maxframelen";
  
  private static final Logger LOG = LoggerFactory.getLogger(GeoDirectory.class);

  public static final String GEODIR_SERVICE = "com.cityzendata.warp.geodir";
  
  private final long[] SIPHASH_KAFKA_SUBS;
  private final long[] SIPHASH_KAFKA_DATA;
  private final long[] SIPHASH_FETCH_PSK;
  
  private final byte[] AES_KAFKA_SUBS;
  private final byte[] AES_KAFKA_DATA;
  private final byte[] AES_ZK_SUBS;

  private final long KAFKA_OUT_MAXSIZE;

  /**
   * Name of GeoDirectory we're serving
   */
  private final String name;

  /**
   * Out unique ID
   */
  private final String id;
  
  /**
   * Endpoint for fetching data
   */
  private final String fetchEndpoint;
  
  /**
   * CuratorFramework for storing Plasma subscriptions
   */
  private final CuratorFramework plasmaCurator;
  
  /**
   * CuratorFramework for storing GeoDir subscriptions
   */
  private final CuratorFramework subsCurator;

  /**
   * CuratorFramework for registering the Thrift service
   */
  private final CuratorFramework serviceCurator;
  
  private final DirectoryClient directoryClient;

  /**
   * Map of token to set of selectors.
   */
  private Map<String,Set<String>> selectors = new HashMap<String, Set<String>>();
  
  /**
   * Map of token to set of GTS ids
   */
  private Map<String,Set<String>> subscriptions = new HashMap<String, Set<String>>();
  
  /**
   * Map of token to billed id
   */
  private Map<String,String> billedId = new HashMap<String, String>();
  
  /**
   * Modulus to apply to labels ID to decide if we select them or not
   */
  private final long modulus;
  
  /**
   * Remainder of labelsId/modulus that we care for
   */
  private final long remainder;
  
  /**
   * How often to update the subscriptions by reading 'Directory'
   */
  private final long period;
  
  /**
   * When to wakeup next
   */
  private AtomicLong wakeup = new AtomicLong();
  
  /**
   * Current set of Plasma subscription znodes
   */
  private Set<String> currentPlasmaZnodes = new HashSet<String>();
  
  /**
   * Current set of Subscription znodes
   */
  private Set<String> currentSubsZnodes = new HashSet<String>();
  
  /**
   * Maximum size of data for each subscription znode in Plasma
   */
  private final int maxPlasmaZnodeSize;
  
  /**
   * Root znode for Plasma subscriptions
   */
  private final String plasmaZnodeRoot;
  
  /**
   * Root znode for storing subscriptions in ZK
   */
  private final String subsZnodeRoot;
  
  /**
   * Maximum size of data for each GeoDir subscription znode
   */
  private final int maxSubsZnodeSize;
  
  /**
   * Topic for Plasma
   */
  private final String plasmaTopic;
  
  /**
   * Topic for subscriptions
   */
  private final String subsTopic;
  
  /**
   * Flag indicating if selectors have been updated
   */
  private final AtomicBoolean selectorsChanged = new AtomicBoolean(false);
  
  private final Producer<byte[], byte[]> subsProducer;
  private final Producer<byte[], byte[]> plasmaProducer;
  
  private final GeoIndex index;

  /**
   * Maximum number of cells per shape
   */
  private final int maxcells;
  
  /**
   * Current list of pending Kafka messages
   */
  private final List<KeyedMessage<byte[], byte[]>> msglist = new ArrayList<KeyedMessage<byte[],byte[]>>();
  
  /**
   * Current size of pending message list
   */
  private final AtomicLong msgsize = new AtomicLong();

  private static final String[] REQUIRED_PROPERTIES = new String[] {
    Configuration.GEODIR_HTTP_HOST,
    Configuration.GEODIR_HTTP_PORT,
    Configuration.GEODIR_ACCEPTORS,
    Configuration.GEODIR_SELECTORS,
    Configuration.GEODIR_IDLE_TIMEOUT,
    Configuration.GEODIR_THRIFT_HOST,
    Configuration.GEODIR_THRIFT_PORT,
    Configuration.GEODIR_THRIFT_MAXTHREADS,
    Configuration.GEODIR_THRIFT_MAXFRAMELEN,
    Configuration.GEODIR_NAME,
    Configuration.GEODIR_ID,
    Configuration.GEODIR_CHUNK_DEPTH,
    Configuration.GEODIR_CHUNK_COUNT,
    Configuration.GEODIR_MODULUS,
    Configuration.GEODIR_REMAINDER,
    Configuration.GEODIR_PERIOD,
    Configuration.GEODIR_RESOLUTION,
    Configuration.GEODIR_MAXCELLS,
    Configuration.GEODIR_ZK_SUBS_QUORUM,
    Configuration.GEODIR_ZK_SUBS_ZNODE,
    Configuration.GEODIR_ZK_SUBS_MAXZNODESIZE,
    Configuration.GEODIR_ZK_SUBS_AES,
    Configuration.GEODIR_ZK_DIRECTORY_QUORUM,
    Configuration.GEODIR_ZK_DIRECTORY_ZNODE,
    Configuration.GEODIR_DIRECTORY_PSK,
    Configuration.GEODIR_FETCH_PSK,
    Configuration.GEODIR_FETCH_ENDPOINT,    
    Configuration.GEODIR_ZK_SERVICE_QUORUM,
    Configuration.GEODIR_ZK_SERVICE_ZNODE,
    Configuration.GEODIR_ZK_PLASMA_QUORUM,
    Configuration.GEODIR_ZK_PLASMA_ZNODE,
    Configuration.GEODIR_ZK_PLASMA_MAXZNODESIZE,
    Configuration.GEODIR_KAFKA_SUBS_ZKCONNECT,
    Configuration.GEODIR_KAFKA_SUBS_BROKERLIST,
    Configuration.GEODIR_KAFKA_SUBS_TOPIC,
    Configuration.GEODIR_KAFKA_SUBS_GROUPID,
    Configuration.GEODIR_KAFKA_SUBS_NTHREADS,
    Configuration.GEODIR_KAFKA_SUBS_COMMITPERIOD,
    //Configuration.GEODIR_KAFKA_SUBS_MAC,
    Configuration.GEODIR_KAFKA_SUBS_AES,
    Configuration.GEODIR_KAFKA_DATA_ZKCONNECT,
    Configuration.GEODIR_KAFKA_DATA_BROKERLIST,
    Configuration.GEODIR_KAFKA_DATA_TOPIC,
    Configuration.GEODIR_KAFKA_DATA_GROUPID,
    Configuration.GEODIR_KAFKA_DATA_NTHREADS,
    Configuration.GEODIR_KAFKA_DATA_COMMITPERIOD,
    Configuration.GEODIR_KAFKA_DATA_MAXSIZE,
    //Configuration.GEODIR_KAFKA_DATA_MAC,
    //Configuration.GEODIR_KAFKA_DATA_AES,
  };
  
  public GeoDirectory(KeyStore keystore, Properties properties) {
    
    //
    // Check required properties
    //
    
    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(required), "Missing configuration parameter '%s'.", required);          
    }

    this.maxPlasmaZnodeSize = Integer.parseInt(properties.getProperty(Configuration.GEODIR_ZK_PLASMA_MAXZNODESIZE, "65536"));    
    this.plasmaZnodeRoot = properties.getProperty(Configuration.GEODIR_ZK_PLASMA_ZNODE);    
    this.plasmaTopic = properties.getProperty(Configuration.GEODIR_KAFKA_DATA_TOPIC);
    
    this.maxSubsZnodeSize = Integer.parseInt(properties.getProperty(Configuration.GEODIR_ZK_SUBS_MAXZNODESIZE, "65536"));
    this.subsZnodeRoot = properties.getProperty(Configuration.GEODIR_ZK_SUBS_ZNODE);    
    this.subsTopic = properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_TOPIC);
    
    this.name = properties.getProperty(Configuration.GEODIR_NAME);
    
    this.id = properties.getProperty(Configuration.GEODIR_ID);
    
    this.modulus = Long.parseLong(properties.getProperty(Configuration.GEODIR_MODULUS));
    this.remainder = Long.parseLong(properties.getProperty(Configuration.GEODIR_REMAINDER));
    this.period = Long.parseLong(properties.getProperty(Configuration.GEODIR_PERIOD));
    
    this.maxcells = Integer.parseInt(properties.getProperty(Configuration.GEODIR_MAXCELLS));

    this.fetchEndpoint = properties.getProperty(Configuration.GEODIR_FETCH_ENDPOINT);
    
    this.KAFKA_OUT_MAXSIZE = Integer.parseInt(properties.getProperty(Configuration.GEODIR_KAFKA_DATA_MAXSIZE));
    
    //
    // Create actual index
    //
    
    long depth = Long.parseLong(properties.getProperty(Configuration.GEODIR_CHUNK_DEPTH));
    int chunks = Integer.parseInt(properties.getProperty(Configuration.GEODIR_CHUNK_COUNT));
    final int resolution = Integer.parseInt(properties.getProperty(Configuration.GEODIR_RESOLUTION));
    
    this.index = new GeoIndex(resolution, chunks, depth);
    
    final String dumpPrefix = properties.getProperty(Configuration.GEODIR_DUMP_PREFIX);
    
    final GeoDirectory self = this;
    
    if (null != dumpPrefix) {
      File path = new File(dumpPrefix + "." + self.id);
      try {
        this.index.loadLKPIndex(path);        
      } catch (IOException ioe) {
        LOG.error("Error while loading LKP '" + this.id + "' from " + path, ioe);
      }
            
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          File path = new File(dumpPrefix + "." + self.id);
          try {
            self.index.dumpLKPIndex(path);
          } catch (IOException ioe) {
            LOG.error("Error while dumping LKP '" + self.id + "' into " + path);
          }
        }
      });
      
      //
      // Make sure ShutdownHookManager is initialized, otherwise it will try to
      // register a shutdown hook during the shutdown hook we just registered...
      //
      
      ShutdownHookManager.get();      
    }
    
    //
    // Create the outbound Kafka producer for subscriptions
    //
    
    Properties subsProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    subsProps.setProperty("zookeeper.connect", properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_ZKCONNECT));
    subsProps.setProperty("metadata.broker.list", properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_BROKERLIST));
    if (null != properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_PRODUCER_CLIENTID)) {
      subsProps.setProperty("client.id", properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_PRODUCER_CLIENTID));
    }
    subsProps.setProperty("request.required.acks", "-1");
    subsProps.setProperty("producer.type","sync");
    subsProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    // We use the default partitioner
    //dataProps.setProperty("partitioner.class", ...);

    ProducerConfig subsConfig = new ProducerConfig(subsProps);
    this.subsProducer = new Producer<byte[], byte[]>(subsConfig);

    //
    // Create the outbound Kafka producer for data
    //
    
    Properties plasmaProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    plasmaProps.setProperty("zookeeper.connect", properties.getProperty(Configuration.GEODIR_KAFKA_DATA_ZKCONNECT));
    plasmaProps.setProperty("metadata.broker.list", properties.getProperty(Configuration.GEODIR_KAFKA_DATA_BROKERLIST));
    if (null != properties.getProperty(Configuration.GEODIR_KAFKA_DATA_PRODUCER_CLIENTID)) {
      plasmaProps.setProperty("client.id", properties.getProperty(Configuration.GEODIR_KAFKA_DATA_PRODUCER_CLIENTID));
    }
    plasmaProps.setProperty("request.required.acks", "-1");
    plasmaProps.setProperty("producer.type","sync");
    plasmaProps.setProperty("request.required.acks", "-1");
    plasmaProps.setProperty("producer.type","sync");
    plasmaProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    plasmaProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());

    ProducerConfig plasmaConfig = new ProducerConfig(plasmaProps);
    this.plasmaProducer = new Producer<byte[], byte[]>(plasmaConfig);
    
    //
    // Extract keys
    //
    
    this.AES_KAFKA_SUBS = keystore.decodeKey(properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_AES));      
    this.AES_KAFKA_DATA = keystore.decodeKey(properties.getProperty(Configuration.GEODIR_KAFKA_DATA_AES));
    this.AES_ZK_SUBS = keystore.decodeKey(properties.getProperty(Configuration.GEODIR_ZK_SUBS_AES));
    this.SIPHASH_KAFKA_SUBS = SipHashInline.getKey(keystore.decodeKey(properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_MAC)));
    this.SIPHASH_KAFKA_DATA = SipHashInline.getKey(keystore.decodeKey(properties.getProperty(Configuration.GEODIR_KAFKA_DATA_MAC)));
    this.SIPHASH_FETCH_PSK = SipHashInline.getKey(keystore.decodeKey(properties.getProperty(Configuration.GEODIR_FETCH_PSK)));
    
    keystore.setKey(KeyStore.SIPHASH_DIRECTORY_PSK, keystore.decodeKey(properties.getProperty(Configuration.GEODIR_DIRECTORY_PSK)));
    
    //
    // Forget master key if it was set
    //
    
    keystore.forget();
    
    //
    // Start the various curator frameworks
    //
    
    this.plasmaCurator = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(properties.getProperty(io.warp10.continuum.Configuration.GEODIR_ZK_PLASMA_QUORUM))
        .build();
    plasmaCurator.start();

    this.subsCurator = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(properties.getProperty(io.warp10.continuum.Configuration.GEODIR_ZK_SUBS_QUORUM))
        .build();
    this.subsCurator.start();

    this.serviceCurator = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(properties.getProperty(io.warp10.continuum.Configuration.GEODIR_ZK_SERVICE_QUORUM))
        .build();
    this.serviceCurator.start();

    //
    // Create ThreadDirectoryClient
    //
    
    try {
      //
      // Copy configuration from GeoDir to Directory.
      // We can do that as we were passed a clone of 'properties'
      //
      
      properties.setProperty(Configuration.DIRECTORY_PSK, properties.getProperty(Configuration.GEODIR_DIRECTORY_PSK));
      properties.setProperty(Configuration.DIRECTORY_ZK_ZNODE, properties.getProperty(Configuration.GEODIR_ZK_DIRECTORY_ZNODE));
      this.directoryClient = new ThriftDirectoryClient(keystore, properties);
    } catch (Exception e) {
      throw new RuntimeException("Unable to start GeoDirectory", e);
    }
    
    //
    // Load the known subscriptions for this GeoDir
    //
    
    zkLoad();
    
    //
    // Initialize Kafka Consumer Pools
    //

    //
    // GeoDir subscriptions
    //

    ConsumerFactory subsConsumerFactory = new SubsConsumerFactory(this);
    
    KafkaSynchronizedConsumerPool pool = new KafkaSynchronizedConsumerPool(properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_ZKCONNECT),
        properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_TOPIC),
        properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_CONSUMER_CLIENTID),
        properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_GROUPID),
        Integer.parseInt(properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_NTHREADS)),
        Long.parseLong(properties.getProperty(Configuration.GEODIR_KAFKA_SUBS_COMMITPERIOD)), subsConsumerFactory);
    
    pool.setAbortHook(new Hook() {      
      @Override
      public void call() {
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_SUBS_ABORTS, Sensision.EMPTY_LABELS, 1);
      }
    });

    pool.setPreCommitOffsetHook(new Hook() {      
      @Override
      public void call() {
        //
        // Store updated selectors to ZooKeeper
        //
        zkStore();
      }
    });
    
    pool.setCommitOffsetHook(new Hook() {      
      @Override
      public void call() {
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_SUBS_KAFKA_COMMITS, Sensision.EMPTY_LABELS, 1);
      }
    });
    
    pool.setSyncHook(new Hook() {      
      @Override
      public void call() {
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_SUBS_SYNCS, Sensision.EMPTY_LABELS, 1);
      }
    });
  
    //
    // Plasma data feed
    //

    ConsumerFactory dataConsumerFactory = new DataConsumerFactory(this);
    
    KafkaSynchronizedConsumerPool datapool = new KafkaSynchronizedConsumerPool(properties.getProperty(Configuration.GEODIR_KAFKA_DATA_ZKCONNECT),
        properties.getProperty(Configuration.GEODIR_KAFKA_DATA_TOPIC),
        properties.getProperty(Configuration.GEODIR_KAFKA_DATA_CONSUMER_CLIENTID),
        properties.getProperty(Configuration.GEODIR_KAFKA_DATA_GROUPID),
        Integer.parseInt(properties.getProperty(Configuration.GEODIR_KAFKA_DATA_NTHREADS)),
        Long.parseLong(properties.getProperty(Configuration.GEODIR_KAFKA_DATA_COMMITPERIOD)), subsConsumerFactory);
    
    pool.setAbortHook(new Hook() {      
      @Override
      public void call() {
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_ABORTS, Sensision.EMPTY_LABELS, 1);
      }
    });

    pool.setCommitOffsetHook(new Hook() {      
      @Override
      public void call() {
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_KAFKA_COMMITS, Sensision.EMPTY_LABELS, 1);
      }
    });
    
    pool.setSyncHook(new Hook() {      
      @Override
      public void call() {
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_SYNCS, Sensision.EMPTY_LABELS, 1);
      }
    });

    //
    // Start Thread, this will trigger an initial subscription update
    //

    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("Warp GeoDirectory");
    t.start();
        
    startThrift(properties);
    
    //
    // Start Jetty server
    //
  
    Server server = new Server();
  
    ServerConnector connector = new ServerConnector(server, Integer.parseInt(properties.getProperty(Configuration.GEODIR_ACCEPTORS)), Integer.parseInt(properties.getProperty(Configuration.GEODIR_SELECTORS)));
    connector.setIdleTimeout(Long.parseLong(properties.getProperty(Configuration.GEODIR_IDLE_TIMEOUT)));
    connector.setPort(Integer.parseInt(properties.getProperty(Configuration.GEODIR_HTTP_PORT)));
    connector.setHost(properties.getProperty(Configuration.GEODIR_HTTP_HOST));
    connector.setName("Warp GeoDir");
  
    server.setConnectors(new Connector[] { connector });

    server.setHandler(this);
  
    JettyUtil.setSendServerVersion(server, false);
  
    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    //
    // Start Thrift Server
    //
    
    
    //
    // Register service in ZK
    //
  }

  private void startThrift(Properties properties) {
    //
    // Start the Thrift Service
    //
        
    GeoDirectoryService.Processor processor = new GeoDirectoryService.Processor(this);
    
    ServiceInstance<Map> instance = null;

    ServiceDiscovery<Map> sd = ServiceDiscoveryBuilder.builder(Map.class)
        .basePath(properties.getProperty(Configuration.GEODIR_ZK_SERVICE_ZNODE))
        .client(this.serviceCurator)
        .build();

    try {
      int port = Integer.parseInt(properties.getProperty(Configuration.GEODIR_THRIFT_PORT));
      String host = properties.getProperty(Configuration.GEODIR_THRIFT_HOST);
      InetSocketAddress bindAddress = new InetSocketAddress(host, port);
      TServerTransport transport = new TServerSocket(bindAddress);
      TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
      args.processor(processor);
      
      int maxThreads = Integer.parseInt(properties.getProperty(Configuration.GEODIR_THRIFT_MAXTHREADS));
      int maxThriftFrameLength = Integer.parseInt(properties.getProperty(Configuration.GEODIR_THRIFT_MAXFRAMELEN));
      
      args.maxWorkerThreads(maxThreads);
      args.minWorkerThreads(Math.max(1, maxThreads >> 2));
      
      if (0 != maxThriftFrameLength) {
        args.inputTransportFactory(new io.warp10.thrift.TFramedTransport.Factory(maxThriftFrameLength));
        args.outputTransportFactory(new io.warp10.thrift.TFramedTransport.Factory(maxThriftFrameLength));        
      } else {
        args.inputTransportFactory(new io.warp10.thrift.TFramedTransport.Factory());
        args.outputTransportFactory(new io.warp10.thrift.TFramedTransport.Factory());
      }
      
      args.inputProtocolFactory(new TCompactProtocol.Factory());
      args.outputProtocolFactory(new TCompactProtocol.Factory());
      TServer server = new TThreadPoolServer(args);
      
      ServiceInstanceBuilder<Map> builder = ServiceInstance.builder();
      builder.port(((TServerSocket) transport).getServerSocket().getLocalPort());
      builder.address(((TServerSocket) transport).getServerSocket().getInetAddress().getHostAddress());
      builder.id(UUID.randomUUID().toString());
      builder.name(GEODIR_SERVICE);
      builder.serviceType(ServiceType.DYNAMIC);
      Map<String,String> payload = new HashMap<String,String>();
      payload.put(INSTANCE_PAYLOAD_MODULUS, Long.toString(this.modulus));
      payload.put(INSTANCE_PAYLOAD_REMAINDER, Long.toString(this.remainder));
      payload.put("thrift.protocol", "org.apache.thrift.protocol.TCompactProtocol");
      payload.put("thrift.transport", "org.apache.thrift.transport.TFramedTransport");
      if (0 != maxThriftFrameLength) {
        payload.put(INSTANCE_PAYLOAD_THRIFT_MAXFRAMELEN, Integer.toString(maxThriftFrameLength));
      }
      payload.put(INSTANCE_PAYLOAD_GEODIR, this.name);
      
      builder.payload(payload);

      instance = builder.build();

      sd.start();
      sd.registerService(instance);
      
      server.serve();
    } catch (TTransportException tte) {
      LOG.error("Thrift transport error", tte);
    } catch (Exception e) {
      LOG.error("Caught exception while starting Thrift service", e);
    } finally {
      if (null != instance) {
        try {
          sd.unregisterService(instance);
        } catch (Exception e) {
        }
      }
    }    
  }
  
  /**
   * Add content of an encoder to our index, checking for 
   */
  private void index(GTSEncoder encoder) {
    //
    // Determine the set of 'tokens' for which we should index 'encoder'
    //
    
    Set<String> billed = new HashSet<String>();
    
    String id = GTSHelper.gtsIdToString(encoder.getClassId(), encoder.getLabelsId());
    
    for(Entry<String,Set<String>> entry: this.subscriptions.entrySet()) {
      if (entry.getValue().contains(id)) {
        //
        // Check Throttling
        //
        
        String producer = billedId.get(entry.getKey());        
        try {
          ThrottlingManager.checkDDP(null, producer, null, null, (int) encoder.getCount(), 0L);
          // We can add 'token' since throttling did not trigger
          billed.add(producer);
        } catch (WarpException we) {
          // Ignore exception
        }
      }
    }
    
    //
    // Do nothing if we don't need to index 'encoder'
    //
    
    if (billed.isEmpty()) {
      return;
    }
    
    long indexed = this.index.index(encoder);
    
    //
    // Now bill the various tokens
    //
    
    Map<String,String> labels = new HashMap<String, String>();
    
    for (String consumer: billed) {
      if (null == consumer) {
        continue;
      }
      labels.clear();      
      labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, consumer);
      labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, this.name);
      Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_INDEXED_PERCONSUMER, labels, indexed);
    }
    
    labels.clear();      
    labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, this.name);
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_INDEXED, labels, indexed);
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    //
    // Only support GEO endpoint calls for our GeoDirectory
    //
    
    if (!target.startsWith(Constants.API_ENDPOINT_GEO + "/" + this.name + "/")) {
      return;
    }
    
    if ((Constants.API_ENDPOINT_GEO + "/" + this.name + Constants.API_ENDPOINT_GEO_LIST).equals(target)) {
      doList(baseRequest, request, response);
    } else if ((Constants.API_ENDPOINT_GEO + "/" + this.name + Constants.API_ENDPOINT_GEO_ADD).equals(target)) {
      doAdd(baseRequest, request, response);
    } else if ((Constants.API_ENDPOINT_GEO + "/" + this.name + Constants.API_ENDPOINT_GEO_REMOVE).equals(target)) {
      doRemove(baseRequest, request, response);
    } else if ((Constants.API_ENDPOINT_GEO + "/" + this.name + Constants.API_ENDPOINT_GEO_INDEX).equals(target)) {
      doIndex(baseRequest, request, response);
    }
  }
  
  private void doList(Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    baseRequest.setHandled(true);
    
    String token = request.getParameter(Constants.HTTP_PARAM_TOKEN);

    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("text/plain");
    
    PrintWriter pw = response.getWriter();
    
    Set<String> selectors = this.selectors.get(token);
   
    if (null == selectors) {
      throw new IOException("Unknown token.");
    }

    for (String selector: selectors) {
      pw.print("SELECTOR ");
      pw.println(selector);
    }    
    
    pw.print("SUBSCRIPTIONS ");
    pw.println(this.subscriptions.containsKey(token) ? this.subscriptions.get(token).size() : 0);
  }
  
  private void doAdd(Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    doAddRemove(true, baseRequest, request, response);
  }
  
  private void doRemove(Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    doAddRemove(false, baseRequest, request, response);
  }

  private void doAddRemove(boolean add, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    baseRequest.setHandled(true);
    
    //
    // Extract parameters
    //
    
    String token = request.getParameter(Constants.HTTP_PARAM_TOKEN);
    String[] selectors = request.getParameterValues(Constants.HTTP_PARAM_SELECTOR);
    
    if (null == token || null == selectors) {
      throw new IOException("Missing '" + Constants.HTTP_PARAM_TOKEN + "' or '" + Constants.HTTP_PARAM_SELECTOR + "' parameter.");
    }
    
    //
    // Validate the token
    //
    
    try {
      ReadToken rt = Tokens.extractReadToken(token);
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }
    
    //
    // Create subscription message
    //
    
    GeoDirectorySubscriptions sub = new GeoDirectorySubscriptions();
    sub.setTimestamp(System.currentTimeMillis());
    sub.setRemoval(!add);
    sub.putToSubscriptions(token, new HashSet<String>());
    for (String selector: selectors) {
      sub.getSubscriptions().get(token).add(selector);
    }
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    byte[] data = null;
    
    try {
      data = serializer.serialize(sub);
    } catch (TException te) {
      throw new IOException("Unable to forward subscription request.");
    }
    
    //
    // Encrypt
    //
    
    if (null != AES_KAFKA_SUBS) {
      data = CryptoUtils.wrap(AES_KAFKA_SUBS, data);
    }
    
    //
    // Add MAC
    //
    
    if (null != SIPHASH_KAFKA_SUBS) {
      data = CryptoUtils.addMAC(SIPHASH_KAFKA_SUBS, data);
    }
    
    //
    // Send the message to Kafka
    //
    
    KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.subsTopic, data);
    this.subsProducer.send(message);
    
    response.setStatus(HttpServletResponse.SC_OK);
  }

  /**
   * Force indexing of some data by fetching them and forwarding them onto the data topic
   * This enables someone with a ReadToken to re-index historical data
   */
  private void doIndex(Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    
    baseRequest.setHandled(true);
    
    //
    // Extract parameters
    //
    
    String token = request.getParameter(Constants.HTTP_PARAM_TOKEN);
    String[] selectors = request.getParameterValues(Constants.HTTP_PARAM_SELECTOR);
    
    if (null == selectors) {
      throw new IOException("Missing selector.");
    }
    
    if (selectors.length != 1) {
      throw new IOException("Can only specify a single selector per request.");
    }
    
    if (null == token) {
      throw new IOException("Missing token.");
    }
    
    //
    // A token can only be used if it has subscribed to GTS in this index    
    //
    
    if (!this.subscriptions.containsKey(token)) {
      throw new IOException("The provided token does not have any current subscriptions in this index.");
    }
    
    //
    // INFO(hbs): this will trigger billing for every one subscribing to GTS in this index, we consider this a marginal case
    //
    
    //
    // TODO(hbs): Issue a signed fetch request which will retrieve GTSWrappers (to be implemented in Fetch)
    //
    
    URL url = new URL(this.fetchEndpoint);
    
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    
    conn.setDoOutput(false);
    conn.setDoInput(true);
    conn.setChunkedStreamingMode(8192);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    
    long now = System.currentTimeMillis();
    StringBuilder sb = new StringBuilder(Long.toHexString(now));
    sb.append(":");
    byte[] content = (Long.toString(now) + ":" + token).getBytes(Charsets.ISO_8859_1);
    long hash = SipHashInline.hash24(this.SIPHASH_FETCH_PSK[0], this.SIPHASH_FETCH_PSK[1], content, 0, content.length);
    sb.append(Long.toHexString(hash));
    conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_FETCH_SIGNATURE), sb.toString());      

    //
    // Build query string
    //
    // token
    // selector
    // format=wrapper
    // now + timespan
    // or start/stop
    //
    
    sb = new StringBuilder();
    sb.append(URLEncoder.encode(Constants.HTTP_PARAM_TOKEN, "UTF-8"));
    sb.append("=");
    sb.append(URLEncoder.encode(token, "UTF-8"));
    sb.append("&");
    sb.append(URLEncoder.encode(Constants.HTTP_PARAM_SELECTOR, "UTF-8"));
    sb.append("=");
    sb.append(URLEncoder.encode(selectors[0], "UTF-8"));
    sb.append("&");
    sb.append(URLEncoder.encode(Constants.HTTP_PARAM_FORMAT, "UTF-8"));
    sb.append("=");
    sb.append(URLEncoder.encode("wrapper", "UTF-8"));
    
    if (null != request.getParameter(Constants.HTTP_PARAM_NOW) && null != request.getParameter(Constants.HTTP_PARAM_TIMESPAN)) {
      sb.append("&");
      sb.append(URLEncoder.encode(Constants.HTTP_PARAM_NOW, "UTF-8"));
      sb.append("=");
      sb.append(URLEncoder.encode(request.getParameter(Constants.HTTP_PARAM_NOW), "UTF-8"));
      sb.append("&");
      sb.append(URLEncoder.encode(Constants.HTTP_PARAM_TIMESPAN, "UTF-8"));
      sb.append("=");
      sb.append(URLEncoder.encode(request.getParameter(Constants.HTTP_PARAM_TIMESPAN), "UTF-8"));
    } else if (null != request.getParameter(Constants.HTTP_PARAM_START) && null != request.getParameter(Constants.HTTP_PARAM_STOP)) {
      sb.append("&");
      sb.append(URLEncoder.encode(Constants.HTTP_PARAM_START, "UTF-8"));
      sb.append("=");
      sb.append(URLEncoder.encode(request.getParameter(Constants.HTTP_PARAM_START), "UTF-8"));
      sb.append("&");
      sb.append(URLEncoder.encode(Constants.HTTP_PARAM_STOP, "UTF-8"));
      sb.append("=");
      sb.append(URLEncoder.encode(request.getParameter(Constants.HTTP_PARAM_STOP), "UTF-8"));
    } else {
      throw new IOException("Missing parameters " + Constants.HTTP_PARAM_START + "/" + Constants.HTTP_PARAM_STOP + " or " + Constants.HTTP_PARAM_NOW + "/" + Constants.HTTP_PARAM_TIMESPAN);
    }
    
    byte[] postDataBytes = sb.toString().getBytes(Charsets.UTF_8);
    
    conn.setRequestProperty("Content-Length", Long.toString(postDataBytes.length));
    conn.getOutputStream().write(postDataBytes);

    InputStream in = conn.getInputStream();
    
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    
    long total = 0L;
    
    while (true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      //
      // Extract MAC
      //
      
      byte[] data = line.getBytes(Charsets.US_ASCII);

      long mac = Longs.fromByteArray(Hex.decode(new String(data, 0, 16, Charsets.US_ASCII)));

      //
      // Extract content and decode it
      //
      
      data = OrderPreservingBase64.decode(data, 16, data.length - 16);
      
      //
      // Compute hash
      //
      
      hash = SipHashInline.hash24(this.SIPHASH_FETCH_PSK[0], this.SIPHASH_FETCH_PSK[1], data, 0, data.length);
      
      //
      // Ignore invalid GTSWrapper
      //
      
      if (hash != mac) {
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, this.name);
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_FETCH_INVALIDMACS, labels, 1);
        continue;
      }
      
      //
      // Extract GTSWrapper
      //
      
      GTSWrapper wrapper = new GTSWrapper();
    
      try {
        deserializer.deserialize(wrapper, Arrays.copyOfRange(data, 16, data.length));
        total += wrapper.getCount();
      } catch (TException te) {
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, this.name);
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_FETCH_FAILEDDESER, labels, 1);
        continue;
      }
      
      //
      // Check encoder base
      //
      
      if (0L != wrapper.getBase()) {
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, this.name);
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_FETCH_INVALIDBASE, labels, 1);
        continue;
      }
      
      //
      // Now push the encoder to Kafka
      //
      
      pushData(wrapper);
    }
    
    br.close();
    
    conn.disconnect();
    
    //
    // Flush Kafka
    //
    
    pushData(null);
    
    response.setContentType("text/plain");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println(total);
  }
  
  
  private void pushData(GTSWrapper wrapper) {
        
    KafkaDataMessage msg = new KafkaDataMessage();
    msg.setClassId(wrapper.getMetadata().getClassId());
    msg.setLabelsId(wrapper.getMetadata().getLabelsId());
    msg.setData(wrapper.bufferForEncoded());
    msg.setType(KafkaDataMessageType.STORE);
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    byte[] value = null;
    
    try {
      value = serializer.serialize(msg);
    } catch (TException te) {
      // Ignore the error
      return;
    }
    
    //
    // Encrypt value if the AES key is defined
    //
      
    if (null != this.AES_KAFKA_DATA) {
      value = CryptoUtils.wrap(this.AES_KAFKA_DATA, value);               
    }
      
    //
    // Compute MAC if the SipHash key is defined
    //
      
    if (null != this.SIPHASH_KAFKA_DATA) {
      value = CryptoUtils.addMAC(this.SIPHASH_KAFKA_DATA, value);;
    }

    byte[] key = null;
    
    KeyedMessage<byte[], byte[]> outmsg = new KeyedMessage<byte[], byte[]>(this.plasmaTopic, key, value);

    send(outmsg);
  }
  
  private void send(KeyedMessage<byte[], byte[]> outmsg) {
    long thismsg = 0L;

    if (null != outmsg) {
      thismsg = outmsg.key().length + outmsg.message().length;
    }

    // FIXME(hbs): we check if the size would outgrow the maximum, if so we flush the message before.
    // in Ingress we add the message first, which could lead to a msg too big for Kafka, we will need
    // to fix Ingress at some point.
    
    synchronized(msglist) {
      if (msglist.size() > 0 && (null == outmsg || msgsize.get() + thismsg > KAFKA_OUT_MAXSIZE)) {
        this.plasmaProducer.send(msglist);
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, this.name);
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_KAFKA_OUT_SENT, labels, 1);
        msglist.clear();
        msgsize.set(0L);
      }
      
      if (null != outmsg) {
        msglist.add(outmsg);
        msgsize.addAndGet(thismsg);
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, this.name);
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_KAFKA_OUT_MESSAGES, labels, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_KAFKA_OUT_BYTES, labels, thismsg);
      }
    }

  }    
  
  
  
  
  
  
  
  
  
  private void updateSubscriptions(String token) {
    //
    // Retrieve the Metadata from Directory
    //

    Set<String> tokens = new HashSet<String>();
    
    if (null == token) {
      tokens.addAll(this.selectors.keySet());
    } else {
      tokens.add(token);
    }
    
    for (String t: tokens) {
      
      Set<String> ids = new HashSet<String>();
      
      ReadToken rtoken = null;
      try {
        rtoken = Tokens.extractReadToken(t);        
      } catch (WarpScriptException ee) {
        // FIXME(hbs): we should 
        continue;
      }
      
      Map<String,String> tokenLabelSelectors = Tokens.labelSelectorsFromReadToken(rtoken);
      
      Set<String> sel = new HashSet<String>();
      
      sel.addAll(this.selectors.get(t));
      
      for (String selector: sel) {
        //
        // Parse selector
        //

        String classSelector = null;
        Map<String,String> labelSelectors = null;
        
        try {
          Object[] result = PARSESELECTOR.parse(sel.toString());
          classSelector = (String) result[0];
          labelSelectors = (Map<String,String>) result[1];
        } catch (WarpScriptException ee) {
        }

        if (null == classSelector || null == labelSelectors) {
          //
          // Remove selector from set as it is bogus
          //
          
          this.selectors.get(t).remove(sel);
          continue;
        }

        //
        // Add labels from token
        //
        
        labelSelectors.putAll(tokenLabelSelectors);
        
        List<String> classSel = new ArrayList<String>();
        List<Map<String,String>> labelsSel = new ArrayList<Map<String,String>>();
        
        classSel.add(classSelector);
        labelsSel.add(labelSelectors);
        
        Iterator<Metadata> iter = null;

        try {
          iter = directoryClient.iterator(classSel, labelsSel);
          
          //
          // Compute the GTS Id for each GTS
          //
          
          while(iter.hasNext()) {
            Metadata metadata = iter.next();
            long labelsid = metadata.getLabelsId();
            
            // Ignore GTS if remainder is not the one we expect
            if (labelsid % this.modulus != this.remainder) {
              continue;
            }
            
            String gtsid = GTSHelper.gtsIdToString(metadata.getClassId(), metadata.getLabelsId());
            
            ids.add(gtsid);
          }
        } catch (IOException ioe) {
          // Ignore error
          continue;
        } finally {
          if (null != iter && iter instanceof MetadataIterator) {
            try { ((MetadataIterator) iter).close(); } catch (Exception e) {}
          }
        }
      }
      
      //
      // Store ids for token 't'
      //
      
      this.subscriptions.put(t, ids);    
    }
    
    //
    // Store subscriptions in ZK
    //
    
    zkStore();
    
    //
    // Store Plasma subscription
    //
  
    plasmaStore();
    
    //
    // Update alarm
    //
    
    this.wakeup.set(System.currentTimeMillis() + this.period);
  }
  
  /**
   * Store the subscription so Plasma BackEnds can send us data
   * This code is almost verbatim that of PlasmaFrontEnd.java
   */
  private void plasmaStore() {
    // Extract current subscriptions
    Set<BigInteger> subs = new HashSet<BigInteger>();
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream(subscriptions.size() * 16);
    
    for (Set<String> s: subscriptions.values()) {
      for (String ss: s) {
        try {
          baos.write(GTSHelper.unpackGTSId(ss));
        } catch (IOException ioe) {          
        }
      }
    }
    
    // Delete current znodes
    
    for (String znode: currentPlasmaZnodes) {
      try {
        this.subsCurator.delete().guaranteed().forPath(znode);
      } catch (Exception e) {
        LOG.error("Error while deleting subscription znodes", e);
      }
    }
    
    currentPlasmaZnodes.clear();
    
    // Store new ones
  
    // Retrieve bytes
    byte[] bytes = baos.toByteArray();
        
    //
    // We now create znodes, limiting the size of each one to maxZnodeSize
    //
    
    int idx = 0;
    
    while(idx < bytes.length) {
      int chunksize = Math.min(this.maxPlasmaZnodeSize, bytes.length - idx);
      
      // Ensure chunksize is a multiple of 16
      chunksize = chunksize - (chunksize % 16);
      
      byte[] data = new byte[chunksize];
      
      System.arraycopy(bytes, idx, data, 0, data.length);
      
      UUID uuid = UUID.randomUUID();
      
      long sip = SipHashInline.hash24(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), data, 0, data.length);
      
      String path = this.plasmaZnodeRoot + "/0." + uuid.toString() + "." + this.plasmaTopic + "." + idx + "." + Long.toHexString(sip);
      
      try {
        this.subsCurator.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);
        currentPlasmaZnodes.add(path);
      } catch (Exception e) {
        LOG.error("Error while creating ZK subscription znode " + path, e);
      }
      
      idx += data.length;
    }
    
    //
    // To notify the backend, update the subscription's zone data
    //
    
    byte[] randomData = (this.plasmaTopic + "." + System.currentTimeMillis()).getBytes(Charsets.UTF_8);
    
    try {
      this.subsCurator.setData().forPath(this.plasmaZnodeRoot, randomData);
    } catch (Exception e) {
      LOG.error("Error while storing data for " + this.plasmaZnodeRoot, e);
    }
  }
  
  /**
   * Store subscriptions in ZooKeeper so the other nodes
   * serving this GeoDirectory can pick them up when they
   * start.
   */
  private void zkStore() {
    //
    // We only store something if there is a change
    //
    
    boolean hasChanged = this.selectorsChanged.getAndSet(false);
    
    if (!hasChanged) {
      return;      
    }
    
    //
    // We remove all znodes with prefix 'id'
    //
    
    for (String znode: currentSubsZnodes) {
      try {
        this.subsCurator.delete().guaranteed().forPath(znode);
      } catch (Exception e) {
        LOG.error("Error while deleting " + znode, e);
      }
    }
    
    currentSubsZnodes.clear();
    
    //
    // Create znodes with our current snapshot of subscriptions
    //
    
    // We synchronize on 'selectors'
    
    synchronized(this.selectors) {
      // Loop on the tokens
      
      int chunkid = -1;
      long chunksize = 0L;
      
      
      GeoDirectorySubscriptions thriftSubs = null;
      
      for (String token: this.selectors.keySet()) {
        
        for (String selector: this.selectors.get(token)) {
          
          if (null == thriftSubs) {
            thriftSubs = new GeoDirectorySubscriptions();
            chunksize =  0L;
            chunkid++;
          } else {
            // If we would be over the maxSubZnodeSize limit, store the znode now
            if (chunksize + selector.length() + token.length() > this.maxSubsZnodeSize) {

              storeChunk(chunkid, thriftSubs);
              
              //
              // Reset 'thriftSubs'
              //
              
              thriftSubs = new GeoDirectorySubscriptions();
              chunksize = 0L;
              chunkid++;
            }
          }
          
          if (0 == thriftSubs.getSubscriptionsSize() || !thriftSubs.getSubscriptions().containsKey(token)) {
            thriftSubs.putToSubscriptions(token, new HashSet<String>());
          }
        }
        
      }
      
      //
      // Store the last chunk
      //
      
      if (null != thriftSubs && thriftSubs.getSubscriptionsSize() > 0) {
        storeChunk(chunkid, thriftSubs);
      }
    }
  }
  
  /**
   * Load initial subscriptions from ZooKeeper
   */
  private void zkLoad() {
    List<String> znodes = null;
    
    try {
      znodes = this.subsCurator.getChildren().forPath(this.subsZnodeRoot);

      for (String znode: znodes) {
        byte[] data = this.subsCurator.getData().forPath(znode);
        
        //
        // Unwrap
        //
        
        if (null != AES_ZK_SUBS) {
          data = CryptoUtils.unwrap(AES_ZK_SUBS, data);
        }
        
        if (null == data) {
          LOG.error("Ignoring invalid znode data for " + znode);
          continue;
        }
        
        TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
        GeoDirectorySubscriptions gds = new GeoDirectorySubscriptions();
        deser.deserialize(gds, data);
        
        //
        // Only consider the subscription if it is for our GeoDirectory
        //
        
        if (this.name.equals(gds.getName())) {
          continue;
        }
        
        if (0 == gds.getSubscriptionsSize()) {
          continue;
        }
        
        for (Entry<String,Set<String>> entry: gds.getSubscriptions().entrySet()) {
          if (!this.selectors.containsKey(entry.getKey())) {
            this.selectors.put(entry.getKey(), entry.getValue());
          } else {
            this.selectors.get(entry.getKey()).addAll(entry.getValue());
          }
        }

        //
        // Record the znode if it was produced by us (at least by a process using the same id)
        //
        
        if (znode.contains(this.name + "-" + this.id + "-")) {
          this.currentSubsZnodes.add(znode);
        }
      }
      
    } catch (Exception e) {
      LOG.error("Error while loading subscriptions", e);
      throw new RuntimeException(e);
    }
    
  }
  
  private void storeChunk(long chunkid, GeoDirectorySubscriptions thriftSubs) {    
    //
    // Complete thriftSubs
    //
    
    thriftSubs.setTimestamp(System.currentTimeMillis());
    thriftSubs.setName(this.name);
    thriftSubs.setRemoval(false);
    
    //
    // Serialize 'thriftSubs'
    //
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    byte[] data = null;
    
    try {
      data = serializer.serialize(thriftSubs);
    } catch (TException te) {
      LOG.error("Error while serializing subscriptions", te);
      return;
    }
    
    //
    // Encrypt the serialized content
    //
    
    if (null != AES_ZK_SUBS) {
      data = CryptoUtils.wrap(AES_ZK_SUBS, data);
    }
    
    //
    // Store in ZK
    //
    
    String path = null;
    try {
      path = this.subsZnodeRoot + "/" + this.name + "-" + this.id + "-" + Long.toHexString(0x100000000L + chunkid);
      this.subsCurator.create().withMode(CreateMode.PERSISTENT).forPath(path, data);
      currentSubsZnodes.add(path);
    } catch (Exception e) {
      LOG.error("Error while creating subscription znode " + path, e);
    }    
  }
  
  @Override
  public void run() {
    while(true) {
      //
      // Update the subscriptions, we need to do that unconditionnaly as Directory may have new GTS for a given selector
      //
      
      updateSubscriptions(null);

      //
      // Sleep for the configured delay
      //
      
      this.wakeup.set(System.currentTimeMillis() + this.period);
      
      do {
        long delay = this.wakeup.get() - System.currentTimeMillis();
        if (delay < 0) {
          delay = 0;
        }
        try { Thread.sleep(delay); } catch (InterruptedException ie) {}
      } while(System.currentTimeMillis() < this.wakeup.get());      
    }
  }

  /**
   * Do the actual filtering of GTS ids according to our GeoIndex
   */
  @Override
  public GeoDirectoryResponse filter(GeoDirectoryRequest request) throws TException {
    GeoDirectoryResponse response = new GeoDirectoryResponse();
    
    if (0 == request.getGtsSize() || 0 == request.getShapeSize()) {
      return response;
    }
    
    long nano = System.nanoTime();
    
    //
    // Extract shape
    //
        
    GeoXPShape shape = GeoXPLib.fromCells(request.getShape());
    
    //
    // Limit shape to the maximum number of allowed cells
    //
    
    shape = GeoXPLib.limit(shape, this.maxcells);
    
    Set<String> unfiltered = new HashSet<String>();
    
    //
    // Convert GTS ids to strings
    //
    
    for (Entry<Long,Set<Long>> entry: request.getGts().entrySet()) {
      long classid = entry.getKey();
      
      for (long labelsid: entry.getValue()) {
        String id = GTSHelper.gtsIdToString(classid, labelsid);
        unfiltered.add(id);
      }
    }
    
    //
    // Do the Geo filtering
    //
    
    Set<String> filtered = this.index.find(unfiltered, shape, request.isInside(), request.getStartTimestamp(), request.getEndTimestamp());
    
    //
    // Update sensision metrics
    //
    
    Map<String,String> labels = new HashMap<String, String>();
    labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, this.name);
    labels.put(SensisionConstants.SENSISION_LABEL_TYPE, request.isInside() ? "in" : "out");    
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_FILTERED, labels, filtered.size());
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_UNFILTERED, labels, unfiltered.size());
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_REQUESTS, labels, 1);
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_CELLS, labels, request.getShapeSize());
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_TIME_US, labels, (System.nanoTime() - nano) / 1000);
    
    //
    // Convert string GTS ids back to numeric classid/labelsid
    //
    
    for (String id: filtered) {
      long[] clslbls = GTSHelper.unpackGTSIdLongs(id);
      
      if (0 == response.getGtsSize() || !response.getGts().containsKey(clslbls[0])) {
        response.putToGts(clslbls[0], new HashSet<Long>());
      }
      
      response.getGts().get(clslbls[0]).add(clslbls[1]);
    }
    
    return response;
  }
  
  private static class SubsConsumerFactory implements ConsumerFactory {
    
    private final GeoDirectory directory;
    
    public SubsConsumerFactory(GeoDirectory directory) {
      this.directory = directory;
    }
    
    @Override
    public Runnable getConsumer(final KafkaSynchronizedConsumerPool pool, final KafkaStream<byte[], byte[]> stream) {
      return new Runnable() {          
        @Override
        public void run() {
          ConsumerIterator<byte[],byte[]> iter = stream.iterator();

          // Iterate on the messages
          TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

          KafkaOffsetCounters counters = pool.getCounters();
          
          // TODO(hbs): allow setting of writeBufferSize

          try {
            while (iter.hasNext()) {
              //
              // Since the call to 'next' may block, we need to first
              // check that there is a message available
              //
              
              boolean nonEmpty = iter.nonEmpty();
              
              if (nonEmpty) {
                MessageAndMetadata<byte[], byte[]> msg = iter.next();
                counters.count(msg.partition(), msg.offset());
                
                byte[] data = msg.message();

                Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_SUBS_KAFKA_MESSAGES, Sensision.EMPTY_LABELS, 1);
                Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_SUBS_KAFKA_BYTES, Sensision.EMPTY_LABELS, data.length);
                
                if (null != directory.SIPHASH_KAFKA_SUBS) {
                  data = CryptoUtils.removeMAC(directory.SIPHASH_KAFKA_SUBS, data);
                }
                
                // Skip data whose MAC was not verified successfully
                if (null == data) {
                  Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_SUBS_KAFKA_INVALIDMACS, Sensision.EMPTY_LABELS, 1);
                  continue;
                }
                
                // Unwrap data if need be
                if (null != directory.AES_KAFKA_SUBS) {
                  data = CryptoUtils.unwrap(directory.AES_KAFKA_SUBS, data);
                }
                
                // Skip data that was not unwrapped successfuly
                if (null == data) {
                  Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_SUBS_KAFKA_INVALIDCIPHERS, Sensision.EMPTY_LABELS, 1);
                  continue;
                }
                
                //
                // Extract GeoDirectorySubscriptions
                //
                
                GeoDirectorySubscriptions subs = new GeoDirectorySubscriptions();
                deserializer.deserialize(subs, data);
                
                //
                // Ignore subscription if is not for our GeoDirectory
                //
                
                if (!directory.name.equals(subs.getName())) {
                  continue;
                }
                
                //
                // Ignore if there are no subscriptions
                //
                
                if (0 == subs.getSubscriptionsSize()) {
                  continue;
                }
                
                //
                // Handle removals
                //
                
                if (subs.isRemoval()) {
                  for (Entry<String, Set<String>> entry: subs.getSubscriptions().entrySet()) {
                    Set<String> currentSubs = directory.selectors.get(entry.getKey());
                    if (null == currentSubs) {
                      continue;
                    }
                    for (String sub: entry.getValue()) {
                      currentSubs.remove(sub);
                      directory.selectorsChanged.set(true);
                    }
                  }
                  continue;
                }
                
                //
                // Handle additions
                //
                
                for (Entry<String, Set<String>> entry: subs.getSubscriptions().entrySet()) {
                  //
                  // Check token, and extract billedId
                  //
                  
                  try {
                    ReadToken rt = Tokens.extractReadToken(entry.getKey());
                    if (null != rt.getBilledId()) {
                      directory.billedId.put(entry.getKey(), Tokens.getUUID(rt.getBilledId()));
                    }
                  } catch (Exception e) {
                    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_SUBS_INVALIDKTOKENS, Sensision.EMPTY_LABELS, 1);
                  }
                  
                  Set<String> currentSubs = null;
                  synchronized (directory.selectors) {
                    if (!directory.selectors.containsKey(entry.getKey())) {
                      directory.selectors.put(entry.getKey(), new HashSet<String>());
                    }
                    currentSubs = directory.selectors.get(entry.getKey());
                  }
                  
                  for (String sub: entry.getValue()) {
                    currentSubs.add(sub);
                    directory.selectorsChanged.set(true);
                  }
                }
              } else {
                // Sleep a tiny while
                try {
                  Thread.sleep(1L);
                } catch (InterruptedException ie) {             
                }
              }          
            }        
          } catch (Throwable t) {
            t.printStackTrace(System.err);
          } finally {
            // Set abort to true in case we exit the 'run' method
            pool.getAbort().set(true);
          }
        }
      };
    }
  }

  
  private static class DataConsumerFactory implements ConsumerFactory {
    
    private final GeoDirectory directory;
      
    public DataConsumerFactory(GeoDirectory directory) {
      this.directory = directory;
    }
      
    @Override
    public Runnable getConsumer(final KafkaSynchronizedConsumerPool pool, final KafkaStream<byte[], byte[]> stream) {
      return new Runnable() {          
        @Override
        public void run() {
          ConsumerIterator<byte[],byte[]> iter = stream.iterator();

          // Iterate on the messages
          TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

          KafkaOffsetCounters counters = pool.getCounters();
          
          try {
            while (iter.hasNext()) {
              //
              // Since the call to 'next' may block, we need to first
              // check that there is a message available
              //
              
              boolean nonEmpty = iter.nonEmpty();
              
              if (nonEmpty) {
                MessageAndMetadata<byte[], byte[]> msg = iter.next();
                counters.count(msg.partition(), msg.offset());
                
                byte[] data = msg.message();

                Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_KAFKA_MESSAGES, Sensision.EMPTY_LABELS, 1);
                Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_KAFKA_BYTES, Sensision.EMPTY_LABELS, data.length);
                
                if (null != directory.SIPHASH_KAFKA_DATA) {
                  data = CryptoUtils.removeMAC(directory.SIPHASH_KAFKA_DATA, data);
                }
                
                // Skip data whose MAC was not verified successfully
                if (null == data) {
                  Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_KAFKA_INVALIDMACS, Sensision.EMPTY_LABELS, 1);
                  continue;
                }
                
                // Unwrap data if need be
                if (null != directory.AES_KAFKA_DATA) {
                  data = CryptoUtils.unwrap(directory.AES_KAFKA_DATA, data);
                }
                
                // Skip data that was not unwrapped successfuly
                if (null == data) {
                  Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_DATA_KAFKA_INVALIDCIPHERS, Sensision.EMPTY_LABELS, 1);
                  continue;
                }

                //
                // Extract KafkaDataMessage
                //
                
                KafkaDataMessage tmsg = new KafkaDataMessage();
                deserializer.deserialize(tmsg, data);
                
                switch(tmsg.getType()) {
                  case STORE:
                    GTSEncoder encoder = new GTSEncoder(0L, null, tmsg.getData());
                    encoder.setClassId(tmsg.getClassId());
                    encoder.setLabelsId(tmsg.getLabelsId());
                    directory.index(encoder);
                    break;
                  case DELETE:
                  case ARCHIVE:
                    break;
                  default:
                    throw new RuntimeException("Invalid message type.");
                }                            
              } else {
                // Sleep a tiny while
                try {
                  Thread.sleep(1L);
                } catch (InterruptedException ie) {             
                }
              }          
            }        
          } catch (Throwable t) {
            t.printStackTrace(System.err);
          } finally {
            // Set abort to true in case we exit the 'run' method
            pool.getAbort().set(true);
          }
        }
      };
    }
  }
}
