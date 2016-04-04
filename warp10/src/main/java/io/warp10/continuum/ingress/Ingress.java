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

package io.warp10.continuum.ingress;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.KafkaProducerPool;
import io.warp10.continuum.KafkaSynchronizedConsumerPool;
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.WarpException;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.ConsumerFactory;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.egress.ThriftDirectoryClient;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.KafkaDataMessage;
import io.warp10.continuum.store.thrift.data.KafkaDataMessageType;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

// FIXME(hbs): handle archive

/**
 * This is the class which ingests metrics.
 */
public class Ingress extends AbstractHandler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Ingress.class);
  
  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    Configuration.INGRESS_HOST,
    Configuration.INGRESS_PORT,
    Configuration.INGRESS_ACCEPTORS,
    Configuration.INGRESS_SELECTORS,
    Configuration.INGRESS_IDLE_TIMEOUT,
    Configuration.INGRESS_JETTY_THREADPOOL,
    Configuration.INGRESS_ZK_QUORUM,
    Configuration.INGRESS_ZK_ZNODE,
    Configuration.INGRESS_KAFKA_META_ZKCONNECT,
    Configuration.INGRESS_KAFKA_META_BROKERLIST,
    Configuration.INGRESS_KAFKA_META_TOPIC,
    Configuration.INGRESS_KAFKA_META_GROUPID,
    Configuration.INGRESS_KAFKA_META_NTHREADS,
    Configuration.INGRESS_KAFKA_META_COMMITPERIOD,
    Configuration.INGRESS_KAFKA_DATA_ZKCONNECT,
    Configuration.INGRESS_KAFKA_DATA_BROKERLIST,
    Configuration.INGRESS_KAFKA_DATA_TOPIC,
    Configuration.INGRESS_KAFKA_ARCHIVE_ZKCONNECT,
    Configuration.INGRESS_KAFKA_ARCHIVE_BROKERLIST,
    Configuration.INGRESS_KAFKA_ARCHIVE_TOPIC,
    Configuration.INGRESS_KAFKA_DATA_POOLSIZE,
    Configuration.INGRESS_KAFKA_METADATA_POOLSIZE,
    Configuration.INGRESS_KAFKA_DATA_MAXSIZE,
    Configuration.INGRESS_KAFKA_METADATA_MAXSIZE,
    Configuration.INGRESS_VALUE_MAXSIZE,
    Configuration.DIRECTORY_PSK,
  };
  
  private final String metaTopic;

  private final String dataTopic;

  private final DirectoryClient directoryClient;

  final long maxValueSize;

  private final String cacheDumpPath;
  
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();
  
  /**
   * List of pending Kafka messages containing metadata (one per Thread)
   */
  private final ThreadLocal<List<KeyedMessage<byte[], byte[]>>> metadataMessages = new ThreadLocal<List<KeyedMessage<byte[], byte[]>>>() {
    protected java.util.List<kafka.producer.KeyedMessage<byte[],byte[]>> initialValue() {
      return new ArrayList<KeyedMessage<byte[], byte[]>>();
    };
  };
  
  /**
   * Byte size of metadataMessages
   */
  private ThreadLocal<AtomicLong> metadataMessagesSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong();
    };
  };

  /**
   * Size threshold after which we flush metadataMessages into Kafka
   */
  private final long METADATA_MESSAGES_THRESHOLD;
    
  /**
   * List of pending Kafka messages containing data
   */
  private final ThreadLocal<List<KeyedMessage<byte[], byte[]>>> dataMessages = new ThreadLocal<List<KeyedMessage<byte[], byte[]>>>() {
    protected java.util.List<kafka.producer.KeyedMessage<byte[],byte[]>> initialValue() {
      return new ArrayList<KeyedMessage<byte[], byte[]>>();
    };
  };
  
  /**
   * Byte size of dataMessages
   */
  private ThreadLocal<AtomicLong> dataMessagesSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong();
    };
  };
  
  /**
   * Size threshold after which we flush dataMessages into Kafka
   * 
   * This and METADATA_MESSAGES_THRESHOLD has to be lower than the configured Kafka max message size (max.message.size)
   */
  private final long DATA_MESSAGES_THRESHOLD;
    
  /**
   * Kafka producer for readings
   */
  //private final Producer<byte[], byte[]> dataProducer;
  
  /**
   * Pool of producers for the 'data' topic
   */
  private final Producer<byte[], byte[]>[] dataProducers;
  
  private int dataProducersCurrentPoolSize = 0;
  
  /**
   * Pool of producers for the 'metadata' topic
   */
  private final KafkaProducerPool metaProducerPool;
  
  /**
   * Number of classId/labelsId to remember (to avoid pushing their metadata to Kafka)
   * Memory footprint is that of a BigInteger whose byte representation is 16 bytes, so probably
   * around 40 bytes
   * FIXME(hbs): need to compute exactly
   */
  private static int METADATA_CACHE_SIZE = 10000000;
  
  /**
   * Cache used to determine if we should push metadata into Kafka or if it was previously seen.
   * Key is a BigInteger constructed from a byte array of classId+labelsId (we cannot use byte[] as map key)
   */
  final Map<BigInteger, Object> metadataCache = new LinkedHashMap<BigInteger, Object>(100, 0.75F, true) {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<BigInteger, Object> eldest) {
      return this.size() > METADATA_CACHE_SIZE;
    }
  };
  
  final KeyStore keystore;
  final Properties properties;
  
  final long[] classKey;
  final long[] labelsKey;
  
  final byte[] AES_KAFKA_META;
  final long[] SIPHASH_KAFKA_META;
  
  private final byte[] aesDataKey;
  private final long[] siphashDataKey;

  private final KafkaSynchronizedConsumerPool  pool;
  
  public Ingress(KeyStore keystore, Properties props) {

    //
    // Enable the ThrottlingManager
    //
    
    ThrottlingManager.enable();
    
    this.keystore = keystore;
    this.properties = props;
    
    //
    // Make sure all required configuration is present
    //
    
    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(props.getProperty(required), "Missing configuration parameter '%s'.", required);          
    }

    //
    // Extract parameters from 'props'
    //
    
    if (props.containsKey(Configuration.INGRESS_CACHE_DUMP_PATH)) {
      this.cacheDumpPath = props.getProperty(Configuration.INGRESS_CACHE_DUMP_PATH);
    } else {
      this.cacheDumpPath = null;
    }
    
    int port = Integer.valueOf(props.getProperty(Configuration.INGRESS_PORT));
    String host = props.getProperty(Configuration.INGRESS_HOST);
    int acceptors = Integer.valueOf(props.getProperty(Configuration.INGRESS_ACCEPTORS));
    int selectors = Integer.valueOf(props.getProperty(Configuration.INGRESS_SELECTORS));
    long idleTimeout = Long.parseLong(props.getProperty(Configuration.INGRESS_IDLE_TIMEOUT));
    
    if (null != props.getProperty(Configuration.INGRESS_METADATA_CACHE_SIZE)) {
      this.METADATA_CACHE_SIZE = Integer.valueOf(props.getProperty(Configuration.INGRESS_METADATA_CACHE_SIZE));
    }
    
    this.metaTopic = props.getProperty(Configuration.INGRESS_KAFKA_META_TOPIC);
    
    this.dataTopic = props.getProperty(Configuration.INGRESS_KAFKA_DATA_TOPIC);

    this.DATA_MESSAGES_THRESHOLD = Long.parseLong(props.getProperty(Configuration.INGRESS_KAFKA_DATA_MAXSIZE));
    this.METADATA_MESSAGES_THRESHOLD = Long.parseLong(props.getProperty(Configuration.INGRESS_KAFKA_METADATA_MAXSIZE));
    this.maxValueSize = Long.parseLong(props.getProperty(Configuration.INGRESS_VALUE_MAXSIZE));
    
    extractKeys(this.keystore, props);
    
    this.classKey = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_CLASS));
    this.labelsKey = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_LABELS));
    
    this.AES_KAFKA_META = this.keystore.getKey(KeyStore.AES_KAFKA_METADATA);
    this.SIPHASH_KAFKA_META = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_KAFKA_METADATA));
    
    this.aesDataKey = this.keystore.getKey(KeyStore.AES_KAFKA_DATA);
    this.siphashDataKey = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_KAFKA_DATA));    
    
    //
    // Prepare meta, data and delete producers
    //
    
    Properties metaProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    metaProps.setProperty("zookeeper.connect", props.getProperty(Configuration.INGRESS_KAFKA_META_ZKCONNECT));
    metaProps.setProperty("metadata.broker.list", props.getProperty(Configuration.INGRESS_KAFKA_META_BROKERLIST));
    metaProps.setProperty("request.required.acks", "-1");
    metaProps.setProperty("producer.type","sync");
    metaProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    metaProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    //??? metaProps.setProperty("block.on.buffer.full", "true");
    
    // FIXME(hbs): compression does not work
    //metaProps.setProperty("compression.codec", "snappy");
    //metaProps.setProperty("client.id","");

    ProducerConfig metaConfig = new ProducerConfig(metaProps);
    
    this.metaProducerPool = new KafkaProducerPool(metaConfig,
        Integer.parseInt(props.getProperty(Configuration.INGRESS_KAFKA_METADATA_POOLSIZE)),
        SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_POOL_GET,
        SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_WAIT_NANO);
    
    Properties dataProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    dataProps.setProperty("zookeeper.connect", props.getProperty(Configuration.INGRESS_KAFKA_DATA_ZKCONNECT));
    dataProps.setProperty("metadata.broker.list", props.getProperty(Configuration.INGRESS_KAFKA_DATA_BROKERLIST));
    dataProps.setProperty("request.required.acks", "-1");
    dataProps.setProperty("producer.type","sync");
    dataProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    dataProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    
    if (null != props.getProperty(Configuration.INGRESS_KAFKA_DATA_REQUEST_TIMEOUT_MS)) {
      dataProps.setProperty("request.timeout.ms", props.getProperty(Configuration.INGRESS_KAFKA_DATA_REQUEST_TIMEOUT_MS));
    }
    
    ///???? dataProps.setProperty("block.on.buffer.full", "true");
    
    // FIXME(hbs): compression does not work
    //dataProps.setProperty("compression.codec", "snappy");
    //dataProps.setProperty("client.id","");

    ProducerConfig dataConfig = new ProducerConfig(dataProps);
    //this.dataProducer = new Producer<byte[], byte[]>(dataConfig);

    //
    // Allocate producer pool
    //
    
    this.dataProducers = new Producer[Integer.parseInt(props.getProperty(Configuration.INGRESS_KAFKA_DATA_POOLSIZE))];
    
    for (int i = 0; i < dataProducers.length; i++) {
      this.dataProducers[i] = new Producer<byte[], byte[]>(dataConfig);
    }
    
    this.dataProducersCurrentPoolSize = this.dataProducers.length;
    
    //
    // Producer for the Delete topic
    //

    /*
    Properties deleteProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    deleteProps.setProperty("zookeeper.connect", props.getProperty(INGRESS_KAFKA_D_ZKCONNECT));
    deleteProps.setProperty("metadata.broker.list", props.getProperty(INGRESS_KAFKA_DELETE_BROKERLIST));
    deleteProps.setProperty("request.required.acks", "-1");
    deleteProps.setProperty("producer.type","sync");
    deleteProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    deleteProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    
    ProducerConfig deleteConfig = new ProducerConfig(deleteProps);
    this.deleteProducer = new Producer<byte[], byte[]>(deleteConfig);
*/
  
    //
    // Attempt to load the cache file (we do that prior to starting the Kafka consumer)
    //
    
    loadCache();

    //
    // Create Kafka consumer to handle Metadata deletions
    //
    
    ConsumerFactory metadataConsumerFactory = new IngressMetadataConsumerFactory(this);
    
    pool = new KafkaSynchronizedConsumerPool(props.getProperty(Configuration.INGRESS_KAFKA_META_ZKCONNECT),
        props.getProperty(Configuration.INGRESS_KAFKA_META_TOPIC),
        props.getProperty(Configuration.INGRESS_KAFKA_META_GROUPID),
        Integer.parseInt(props.getProperty(Configuration.INGRESS_KAFKA_META_NTHREADS)),
        Long.parseLong(props.getProperty(Configuration.INGRESS_KAFKA_META_COMMITPERIOD)), metadataConsumerFactory);    

    //
    // Initialize ThriftDirectoryService
    //
    
    try {
      this.directoryClient = new ThriftDirectoryClient(this.keystore, props);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    //
    // Register shutdown hook
    //
    
    final Ingress self = this;
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        //
        // Make sure the Kakfa consumers are stopped so we don't miss deletions
        // when restarting and using the cache we are about to store
        //
        
        self.pool.shutdown();
        
        LOG.info("Waiting for Ingress Kafka consumers to stop.");
        
        while(!self.pool.isStopped()) {
          LockSupport.parkNanos(250000000L);
        }
        
        LOG.info("Kafka consumers stopped, dumping GTS cache");
        
        self.dumpCache();
      }
    });
    
    //
    // Make sure ShutdownHookManager is initialized, otherwise it will try to
    // register a shutdown hook during the shutdown hook we just registered...
    //
    
    ShutdownHookManager.get();      

    //
    // Start Jetty server
    //
    
    int maxThreads = Integer.parseInt(props.getProperty(Configuration.INGRESS_JETTY_THREADPOOL));
    
    BlockingArrayQueue<Runnable> queue = null;
    
    if (props.containsKey(Configuration.INGRESS_JETTY_MAXQUEUESIZE)) {
      int queuesize = Integer.parseInt(props.getProperty(Configuration.INGRESS_JETTY_MAXQUEUESIZE));
      queue = new BlockingArrayQueue<Runnable>(queuesize);
    }
    
    Server server = new Server(new QueuedThreadPool(maxThreads,8, (int) idleTimeout, queue));
    ServerConnector connector = new ServerConnector(server, acceptors, selectors);
    connector.setIdleTimeout(idleTimeout);
    connector.setPort(port);
    connector.setHost(host);
    connector.setName("Continuum Ingress");
    
    server.setConnectors(new Connector[] { connector });

    HandlerList handlers = new HandlerList();
    handlers.addHandler(this);
    
    IngressStreamUpdateHandler suHandler = new IngressStreamUpdateHandler(this);
    handlers.addHandler(suHandler);
    
    server.setHandler(handlers);
    
    JettyUtil.setSendServerVersion(server, false);

    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("Continuum Ingress");
    t.start();
  }

  @Override
  public void run() {
    //
    // Register in ZK and watch parent znode.
    // If the Ingress count exceeds the licensed number,
    // exit if we are the first of the list.
    //
    
    while(true) {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException ie) {        
      }
    }
  }
  
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (target.equals(Constants.API_ENDPOINT_UPDATE)) {
      baseRequest.setHandled(true);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_REQUESTS, Sensision.EMPTY_LABELS, 1);
    } else if (target.equals(Constants.API_ENDPOINT_META)) {
      handleMeta(target, baseRequest, request, response);
      return;
    } else if (target.equals(Constants.API_ENDPOINT_DELETE)) {
      handleDelete(target, baseRequest, request, response);
    } else {
      return;
    }
    
    //
    // CORS header
    //
    
    response.setHeader("Access-Control-Allow-Origin", "*");

    long nano = System.nanoTime();
    
    //
    // TODO(hbs): Extract producer/owner from token
    //
    
    String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
    
    WriteToken writeToken;
    
    try {
      writeToken = Tokens.extractWriteToken(token);
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }
    
    String application = writeToken.getAppName();
    String producer = Tokens.getUUID(writeToken.getProducerId());
    String owner = Tokens.getUUID(writeToken.getOwnerId());
      
    Map<String,String> sensisionLabels = new HashMap<String,String>();
    sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

    long count = 0;
    
    try {
      if (null == producer || null == owner) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_INVALIDTOKEN, Sensision.EMPTY_LABELS, 1);
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }
      
      //
      // Build extra labels
      //
      
      Map<String,String> extraLabels = new HashMap<String,String>();
      
      // Add labels from the WriteToken if they exist
      if (writeToken.getLabelsSize() > 0) {
        extraLabels.putAll(writeToken.getLabels());
      }
      
      // Force internal labels
      extraLabels.put(Constants.PRODUCER_LABEL, producer);
      extraLabels.put(Constants.OWNER_LABEL, owner);

      // FIXME(hbs): remove me
      if (null != application) {
        extraLabels.put(Constants.APPLICATION_LABEL, application);
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }

      //
      // Determine if content is gzipped
      //

      boolean gzipped = false;
          
      if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {      
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_GZIPPED, sensisionLabels, 1);
        gzipped = true;
      }
      
      BufferedReader br = null;
          
      if (gzipped) {
        GZIPInputStream is = new GZIPInputStream(request.getInputStream());
        br = new BufferedReader(new InputStreamReader(is));
      } else {    
        br = request.getReader();
      }
      
      Long now = TimeSource.getTime();

      //
      // Check the value of the 'now' header
      //
      // The following values are supported:
      //
      // A number, which will be interpreted as an absolute time reference,
      // i.e. a number of time units since the Epoch.
      //
      // A number prefixed by '+' or '-' which will be interpreted as a
      // delta from the present time.
      //
      // A '*' which will mean to not set 'now', and to recompute its value
      // each time it's needed.
      //
      
      String nowstr = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX));
      
      if (null != nowstr) {
        if ("*".equals(nowstr)) {
          now = null;
        } else if (nowstr.startsWith("+")) {
          try {
            long delta = Long.parseLong(nowstr.substring(1));
            now = now + delta;
          } catch (Exception e) {
            throw new IOException("Invalid base timestamp.");
          }                  
        } else if (nowstr.startsWith("-")) {
          try {
            long delta = Long.parseLong(nowstr.substring(1));
            now = now - delta;
          } catch (Exception e) {
            throw new IOException("Invalid base timestamp.");
          }                            
        } else {
          try {
            now = Long.parseLong(nowstr);
          } catch (Exception e) {
            throw new IOException("Invalid base timestamp.");
          }                  
        }
      }

      //
      // Loop on all lines
      //
      
      GTSEncoder lastencoder = null;
      GTSEncoder encoder = null;
      
      byte[] bytes = new byte[16];
      
      int idx = 0;
      
      AtomicLong dms = this.dataMessagesSize.get();
      
      do {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
      
        try {
          encoder = GTSHelper.parse(lastencoder, line, extraLabels, now, maxValueSize);
          count++;
        } catch (ParseException pe) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_PARSEERRORS, sensisionLabels, 1);
          throw new IOException("Parse error at '" + line + "'", pe);
        }
                
        if (encoder != lastencoder || dms.get() + 16 + lastencoder.size() > DATA_MESSAGES_THRESHOLD) {
          //
          // Determine if we should push the metadata or not
          //
          
          encoder.setClassId(GTSHelper.classId(this.classKey, encoder.getMetadata().getName()));
          encoder.setLabelsId(GTSHelper.labelsId(this.labelsKey, encoder.getMetadata().getLabels()));

          GTSHelper.fillGTSIds(bytes, 0, encoder.getClassId(), encoder.getLabelsId());

          BigInteger metadataCacheKey = new BigInteger(bytes);

          //
          // Check throttling
          //
          
          if (null != lastencoder && lastencoder.size() > 0) {
            ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId());
            ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount());
          }
          
          if (!this.metadataCache.containsKey(metadataCacheKey)) {
            // Build metadata object to push
            Metadata metadata = new Metadata();
            // Set source to indicate we
            metadata.setSource(Configuration.INGRESS_METADATA_SOURCE);
            metadata.setName(encoder.getMetadata().getName());
            metadata.setLabels(encoder.getMetadata().getLabels());
            TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
            try {
              pushMetadataMessage(bytes, serializer.serialize(metadata));
            } catch (TException te) {
              throw new IOException("Unable to push metadata.");
            }
          }
          
          // Update metadataCache with the current key
          synchronized(metadataCache) {
            this.metadataCache.put(metadataCacheKey, null);
          }

          if (null != lastencoder) {
            Map<String,String> labels = new HashMap<String, String>();
            //labels.put(SensisionConstants.SENSISION_LABEL_OWNER, owner);
            labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);
            //if (null != application) {
            //  labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
            //}

            pushDataMessage(lastencoder);
          }
          
          if (encoder != lastencoder) {
            lastencoder = encoder;
          } else {
            //lastencoder = null;
            //
            // Allocate a new GTSEncoder and reuse Metadata so we can
            // correctly handle a continuation line if this is what occurs next
            //
            Metadata metadata = lastencoder.getMetadata();
            lastencoder = new GTSEncoder(0L);
            lastencoder.setMetadata(metadata);
          }
        }

        if (0 == count % 1000) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_GLOBAL, Sensision.EMPTY_LABELS, count);
          count = 0;
        }
      } while (true); 
      
      if (null != lastencoder && lastencoder.size() > 0) {
        Map<String,String> labels = new HashMap<String, String>();
        //labels.put(SensisionConstants.SENSISION_LABEL_OWNER, owner);
        labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);
        //if (null != application) {
        //  labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
        //}
        
        ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId());
        ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount());

        pushDataMessage(lastencoder);
      }
    } catch (WarpException we) {
      throw new IOException(we);      
    } finally {
      //
      // Flush message buffers into Kafka
      //
      
      pushMetadataMessage(null, null);
      pushDataMessage(null);

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_GLOBAL, Sensision.EMPTY_LABELS, count);
      
      long micros = (System.nanoTime() - nano) / 1000L;
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_TIME_US, sensisionLabels, micros);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_TIME_US_GLOBAL, Sensision.EMPTY_LABELS, micros);      
    }
    
    response.setStatus(HttpServletResponse.SC_OK);
  }
  
  /**
   * Handle Metadata updating
   */
  public void handleMeta(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    
    //FIXME(hbs) !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // NEED TO REWRITE THIS FUNCTION
    
    if (target.equals(Constants.API_ENDPOINT_META)) {
      baseRequest.setHandled(true);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_REQUESTS, Sensision.EMPTY_LABELS, 1);
    } else {
      return;
    }
    
    //
    // CORS header
    //
    
    response.setHeader("Access-Control-Allow-Origin", "*");

    //
    // Loop over the input lines.
    // Each has the following format:
    //
    // class{labels}{attributes}
    //
    
    //
    // TODO(hbs): Extract producer/owner from token
    //
    
    String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
        
    WriteToken writeToken;
    
    try {
      try {
        writeToken = Tokens.extractWriteToken(token);
      } catch (WarpScriptException ee) {
        throw new IOException(ee);
      }
      
      String application = writeToken.getAppName();
      String producer = Tokens.getUUID(writeToken.getProducerId());
      String owner = Tokens.getUUID(writeToken.getOwnerId());
        
      if (null == producer || null == owner) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALIDTOKEN, Sensision.EMPTY_LABELS, 1);      
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }

      Map<String,String> sensisionLabels = new HashMap<String,String>();
      sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

      if (null != application) {
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }
      
      long count = 0;
           
      //
      // Determine if content if gzipped
      //

      boolean gzipped = false;
          
      if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {       Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_REQUESTS, Sensision.EMPTY_LABELS, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_GZIPPED, sensisionLabels, 1);     
        gzipped = true;
      }
      
      BufferedReader br = null;
          
      if (gzipped) {
        GZIPInputStream is = new GZIPInputStream(request.getInputStream());
        br = new BufferedReader(new InputStreamReader(is));
      } else {    
        br = request.getReader();
      }
      
      //
      // Loop on all lines
      //

      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        Metadata metadata = MetadataUtils.parseMetadata(line);
        
        //
        // Force owner/producer
        //
        
        metadata.getLabels().put(Constants.PRODUCER_LABEL, producer);
        metadata.getLabels().put(Constants.OWNER_LABEL, owner);
      
        if (null != application) {
          metadata.getLabels().put(Constants.APPLICATION_LABEL, application);
        }

        if (!MetadataUtils.validateMetadata(metadata)) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALID, sensisionLabels, 1);
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid metadata " + line);
          return;
        }
        
        count++;

        metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
        
        try {
          pushMetadataMessage(metadata);
        } catch (Exception e) {
          throw new IOException("Unable to push metadata");
        }
      }

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_RECORDS, sensisionLabels, count);
    } finally {
      //
      // Flush message buffers into Kafka
      //
    
      pushMetadataMessage(null, null);
    }
  
    response.setStatus(HttpServletResponse.SC_OK);
  }

  public void handleDelete(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (target.equals(Constants.API_ENDPOINT_DELETE)) {
      baseRequest.setHandled(true);
    } else {
      return;
    }    
    
    //
    // CORS header
    //
    
    response.setHeader("Access-Control-Allow-Origin", "*");

    long nano = System.nanoTime();
    
    //
    // Extract token infos
    //
    
    String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
            
    WriteToken writeToken;
    
    try {
      writeToken = Tokens.extractWriteToken(token);
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }
    
    String application = writeToken.getAppName();
    String producer = Tokens.getUUID(writeToken.getProducerId());
    String owner = Tokens.getUUID(writeToken.getOwnerId());
      
    //
    // For delete operations, producer and owner MUST be equal
    //
    
    if (!producer.equals(owner)) {
      throw new IOException("Invalid write token for deletion.");
    }
    
    Map<String,String> sensisionLabels = new HashMap<String,String>();
    sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

    long count = 0;
    long gts = 0;
    
    boolean completeDeletion = false;
    
    try {      
      if (null == producer || null == owner) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }
      
      //
      // Build extra labels
      //
      
      Map<String,String> extraLabels = new HashMap<String,String>();
      //
      // Only set owner and potentially app, producer may vary
      //      
      extraLabels.put(Constants.OWNER_LABEL, owner);
      // FIXME(hbs): remove me
      if (null != application) {
        extraLabels.put(Constants.APPLICATION_LABEL, application);
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }

      boolean hasRange = false;
      
      //
      // Extract start/end
      //
      
      String startstr = request.getParameter(Constants.HTTP_PARAM_START);
      String endstr = request.getParameter(Constants.HTTP_PARAM_END);
      
      String minagestr = request.getParameter(Constants.HTTP_PARAM_MINAGE);
      
      long start = Long.MIN_VALUE;
      long end = Long.MAX_VALUE;
      
      long minage = 0L;

      if (null != minagestr) {
        minage = Long.parseLong(minagestr);
        
        if (minage < 0) {
          throw new IOException("Invalid value for '" + Constants.HTTP_PARAM_MINAGE + "', expected a number of ms >= 0");
        }
      }
      
      if (null != startstr) {
        if (null == endstr) {
          throw new IOException("Both " + Constants.HTTP_PARAM_START + " and " + Constants.HTTP_PARAM_END + " should be defined.");
        }
        if (startstr.contains("T")) {
          start = fmt.parseDateTime(startstr).getMillis() * Constants.TIME_UNITS_PER_MS;
        } else {
          start = Long.valueOf(startstr);
        }
      }
      
      if (null != endstr) {
        if (null == startstr) {
          throw new IOException("Both " + Constants.HTTP_PARAM_START + " and " + Constants.HTTP_PARAM_END + " should be defined.");
        }
        if (endstr.contains("T")) {
          end = fmt.parseDateTime(endstr).getMillis() * Constants.TIME_UNITS_PER_MS;          
        } else {
          end = Long.valueOf(endstr);
        }
      }

      if (Long.MIN_VALUE == start && Long.MAX_VALUE == end && null == request.getParameter(Constants.HTTP_PARAM_DELETEALL)) {
        throw new IOException("Parameter " + Constants.HTTP_PARAM_DELETEALL + " should be set when deleting a full range.");
      }
      
      if (start > end) {
        throw new IOException("Invalid time range specification.");
      }
      
      //
      // Extract selector
      //
      
      String selector = request.getParameter(Constants.HTTP_PARAM_SELECTOR);
      
      //
      // Extract the class and labels selectors
      // The class selector and label selectors are supposed to have
      // values which use percent encoding, i.e. explicit percent encoding which
      // might have been re-encoded using percent encoding when passed as parameter
      //
      //
      
      Matcher m = EgressFetchHandler.SELECTOR_RE.matcher(selector);
      
      if (!m.matches()) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }
      
      String classSelector = URLDecoder.decode(m.group(1), "UTF-8");
      String labelsSelection = m.group(2);
      
      Map<String,String> labelsSelectors;

      try {
        labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
      } catch (ParseException pe) {
        throw new IOException(pe);
      }
      
      //
      // Force 'owner'/'app' from token
      //
      
      labelsSelectors.putAll(extraLabels);

      List<Metadata> metadatas = null;
      
      List<String> clsSels = new ArrayList<String>();
      List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
      
      clsSels.add(classSelector);
      lblsSels.add(labelsSelectors);
      
      
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("text/plain");

      PrintWriter pw = response.getWriter();
      StringBuilder sb = new StringBuilder();

      try (MetadataIterator iterator = directoryClient.iterator(clsSels, lblsSels)) {
        while(iterator.hasNext()) {
          Metadata metadata = iterator.next();
        
          pushDeleteMessage(start, end, minage, metadata, writeToken);
          
          if (Long.MAX_VALUE == end && Long.MIN_VALUE == start && 0 == minage) {
            completeDeletion = true;
            // We must also push the metadata deletion and remove the metadata from the cache
            Metadata meta = new Metadata(metadata);
            meta.setSource(Configuration.INGRESS_METADATA_DELETE_SOURCE);
            pushMetadataMessage(meta);          
            byte[] bytes = new byte[16];
            // We know class/labels Id were computed in pushMetadataMessage
            GTSHelper.fillGTSIds(bytes, 0, meta.getClassId(), meta.getLabelsId());
            BigInteger key = new BigInteger(bytes);
            synchronized(this.metadataCache) {
              this.metadataCache.remove(key);
            }
          }
          sb.setLength(0);
          GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels());
          
          if (metadata.getAttributesSize() > 0) {
            GTSHelper.labelsToString(sb, metadata.getAttributes());
          } else {
            sb.append("{}");
          }

          pw.write(sb.toString());
          pw.write("\r\n");
          gts++;
        }      
      } catch (Exception e) {        
      }
    } finally {
      // Flush delete messages
      pushDeleteMessage(0L,0L,0L,null, writeToken);
      if (completeDeletion) {
        pushMetadataMessage(null, null);
      }
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_DELETE_REQUESTS, sensisionLabels, 1);
    }

    response.setStatus(HttpServletResponse.SC_OK);
  }
  
  /**
   * Extract Ingress related keys and populate the KeyStore with them.
   * 
   * @param props Properties from which to extract the key specs
   */
  private void extractKeys(KeyStore keystore, Properties props) {
    String keyspec = props.getProperty(Configuration.INGRESS_KAFKA_META_MAC);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.INGRESS_KAFKA_META_MAC + " MUST be 128 bits long.");
      keystore.setKey(KeyStore.SIPHASH_KAFKA_METADATA, key);
    }
    
    keyspec = props.getProperty(Configuration.INGRESS_KAFKA_DATA_MAC);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.INGRESS_KAFKA_DATA_MAC + " MUST be 128 bits long.");
      keystore.setKey(KeyStore.SIPHASH_KAFKA_DATA, key);
    }

    keyspec = props.getProperty(Configuration.INGRESS_KAFKA_META_AES);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.INGRESS_KAFKA_META_AES + " MUST be 128, 192 or 256 bits long.");
      keystore.setKey(KeyStore.AES_KAFKA_METADATA, key);
    }

    keyspec = props.getProperty(Configuration.INGRESS_KAFKA_DATA_AES);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.INGRESS_KAFKA_DATA_AES + " MUST be 128, 192 or 256 bits long.");
      keystore.setKey(KeyStore.AES_KAFKA_DATA, key);
    }
    
    keyspec = props.getProperty(Configuration.DIRECTORY_PSK);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.DIRECTORY_PSK + " MUST be 128 bits long.");
      this.keystore.setKey(KeyStore.SIPHASH_DIRECTORY_PSK, key);
    }    
    
    this.keystore.forget();
  }
  
  void pushMetadataMessage(Metadata metadata) throws IOException {
    
    if (null == metadata) {
      pushMetadataMessage(null, null);
      return;
    }
    
    //
    // Compute class/labels Id
    //
    
    metadata.setClassId(GTSHelper.classId(this.classKey, metadata.getName()));
    metadata.setLabelsId(GTSHelper.labelsId(this.labelsKey, metadata.getLabels()));
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    try {
      byte[] bytes = new byte[16];
      GTSHelper.fillGTSIds(bytes, 0, metadata.getClassId(), metadata.getLabelsId());
      pushMetadataMessage(bytes, serializer.serialize(metadata));
    } catch (TException te) {
      throw new IOException("Unable to push metadata.");
    }
  }
  
  /**
   * Push a metadata message onto the buffered list of Kafka messages
   * and flush the list to Kafka if it has reached a threshold.
   * 
   * @param key Key of the message to queue
   * @param value Value of the message to queue
   */
  private void pushMetadataMessage(byte[] key, byte[] value) throws IOException {
    
    AtomicLong mms = this.metadataMessagesSize.get();
    List<KeyedMessage<byte[], byte[]>> msglist = this.metadataMessages.get();
    
    if (null != key && null != value) {
      
      //
      // Add key as a prefix of value
      //
      
      byte[] kv = Arrays.copyOf(key, key.length + value.length);
      System.arraycopy(value, 0, kv, key.length, value.length);
      value = kv;
      
      //
      // Encrypt value if the AES key is defined
      //
      
      if (null != this.AES_KAFKA_META) {
        value = CryptoUtils.wrap(this.AES_KAFKA_META, value);               
      }
      
      //
      // Compute MAC if the SipHash key is defined
      //
      
      if (null != this.SIPHASH_KAFKA_META) {
        value = CryptoUtils.addMAC(this.SIPHASH_KAFKA_META, value);
      }
      
      KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.metaTopic, Arrays.copyOf(key, key.length), value);
      msglist.add(message);
      mms.addAndGet(key.length + value.length);
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_META_MESSAGES, Sensision.EMPTY_LABELS, 1);
    }
    
    if (msglist.size() > 0 && (null == key || null == value || mms.get() > METADATA_MESSAGES_THRESHOLD)) {
      Producer<byte[],byte[]> producer = this.metaProducerPool.getProducer();
      try {
        producer.send(msglist);
      } catch (Throwable t) {
        //
        // We need to remove the IDs of Metadata in 'msglist' from the cache so they get a chance to be
        // pushed later
        //

        for (KeyedMessage<byte[],byte[]> msg: msglist) {
          synchronized(this.metadataCache) {
            this.metadataCache.remove(new BigInteger(msg.key()));
          }          
        }

        throw t;
      } finally {
        this.metaProducerPool.recycleProducer(producer);
      }
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_META_SEND, Sensision.EMPTY_LABELS, 1);
      msglist.clear();
      mms.set(0L);
      // Update sensision metric with size of metadata cache
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_METADATA_CACHED, Sensision.EMPTY_LABELS, this.metadataCache.size());
    }      
  }

  /**
   * Push a metadata message onto the buffered list of Kafka messages
   * and flush the list to Kafka if it has reached a threshold.
   * 
   * @param encoder GTSEncoder to push to Kafka. It MUST have classId/labelsId set.
   */
  void pushDataMessage(GTSEncoder encoder) throws IOException {    
    if (null != encoder) {
      KafkaDataMessage msg = new KafkaDataMessage();
      msg.setType(KafkaDataMessageType.STORE);
      msg.setData(encoder.getBytes());
      msg.setClassId(encoder.getClassId());
      msg.setLabelsId(encoder.getLabelsId());

      sendDataMessage(msg);
    } else {
      sendDataMessage(null);
    }
  }

  private void sendDataMessage(KafkaDataMessage msg) throws IOException {    
    AtomicLong dms = this.dataMessagesSize.get();
    List<KeyedMessage<byte[], byte[]>> msglist = this.dataMessages.get();
    
    if (null != msg) {
      //
      // Build key
      //
      
      byte[] bytes = new byte[16];
      
      GTSHelper.fillGTSIds(bytes, 0, msg.getClassId(), msg.getLabelsId());

      //ByteBuffer bb = ByteBuffer.wrap(new byte[16]).order(ByteOrder.BIG_ENDIAN);
      //bb.putLong(encoder.getClassId());
      //bb.putLong(encoder.getLabelsId());

      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
           
      byte[] msgbytes = null;
      
      try {
        msgbytes = serializer.serialize(msg);
      } catch (TException te) {
        throw new IOException(te);
      }
      
      //
      // Encrypt value if the AES key is defined
      //
        
      if (null != this.aesDataKey) {
        msgbytes = CryptoUtils.wrap(this.aesDataKey, msgbytes);               
      }
        
      //
      // Compute MAC if the SipHash key is defined
      //
        
      if (null != this.siphashDataKey) {
        msgbytes = CryptoUtils.addMAC(this.siphashDataKey, msgbytes);
      }
        
      //KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.dataTopic, bb.array(), msgbytes);
      KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.dataTopic, bytes, msgbytes);
      msglist.add(message);
      //this.dataMessagesSize.get().addAndGet(bb.array().length + msgbytes.length);      
      dms.addAndGet(bytes.length + msgbytes.length);      
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_MESSAGES, Sensision.EMPTY_LABELS, 1);
    }

    if (msglist.size() > 0 && (null == msg || dms.get() > DATA_MESSAGES_THRESHOLD)) {
      Producer<byte[],byte[]> producer = getDataProducer();
      //this.dataProducer.send(msglist);
      try {
        producer.send(msglist);
      } catch (Throwable t) {
        throw t;
      } finally {
        recycleDataProducer(producer);
      }
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_SEND, Sensision.EMPTY_LABELS, 1);
      msglist.clear();
      dms.set(0L);          
    }        
  }
  
  private Producer<byte[],byte[]> getDataProducer() {
    
    //
    // We will count how long we wait for a producer
    //
    
    long nano = System.nanoTime();
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_POOL_GET, Sensision.EMPTY_LABELS, 1);
    
    while(true) {
      synchronized (this.dataProducers) {
        if (this.dataProducersCurrentPoolSize > 0) {
          //
          // hand out the producer at index 0
          //
          
          Producer<byte[],byte[]> producer = this.dataProducers[0];
          
          //
          // Decrement current pool size
          //
          
          this.dataProducersCurrentPoolSize--;
          
          //
          // Move the last element of the array at index 0
          //
          
          this.dataProducers[0] = this.dataProducers[this.dataProducersCurrentPoolSize];
          this.dataProducers[this.dataProducersCurrentPoolSize] = null;

          //
          // Log waiting time
          //
          
          nano = System.nanoTime() - nano;          
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_WAIT_NANO, Sensision.EMPTY_LABELS, nano);

          return producer;
        }
      }
      
      LockSupport.parkNanos(500000L);
    }    
  }
  
  private void recycleDataProducer(Producer<byte[],byte[]> producer) {
    
    if (this.dataProducersCurrentPoolSize == this.dataProducers.length) {
      throw new RuntimeException("Invalid call to recycleProducer, pool already full!");
    }
    
    synchronized (this.dataProducers) {
      //
      // Add the recycled producer at the end of the pool
      //

      this.dataProducers[this.dataProducersCurrentPoolSize++] = producer;
    }
  }
  
  
  /**
   * Push a deletion message onto the buffered list of Kafka messages
   * and flush the list to Kafka if it has reached a threshold.
   * 
   * Deletion messages MUST be pushed onto the data topic, otherwise the
   * ordering won't be respected and you risk deleting a GTS which has been
   * since repopulated with data.
   * 
   * @param start Start timestamp for deletion
   * @param end End timestamp for deletion
   * @param metadata Metadata of the GTS to delete
   * @param writeToken Write token making deletion request
   */
  private void pushDeleteMessage(long start, long end, long minage, Metadata metadata, WriteToken writeToken) throws IOException {
    if (null != metadata) {
      KafkaDataMessage msg = new KafkaDataMessage();
      msg.setType(KafkaDataMessageType.DELETE);
      msg.setDeletionStartTimestamp(start);
      msg.setDeletionEndTimestamp(end);
      msg.setDeletionMinAge(minage);
      msg.setClassId(metadata.getClassId());
      msg.setLabelsId(metadata.getLabelsId());
      msg.setWriteToken(writeToken);

      sendDataMessage(msg);
    } else {
      sendDataMessage(null);
    }
  }

  private void dumpCache() {
    if (null == this.cacheDumpPath) {
      return;
    }

    OutputStream out = null;
    
    long nano = System.nanoTime();
    long count = 0;
    
    try {
      out = new GZIPOutputStream(new FileOutputStream(this.cacheDumpPath));
      
      Set<BigInteger> bis = new HashSet<BigInteger>();

      synchronized(this.metadataCache) {
        bis.addAll(this.metadataCache.keySet());
      }
      
      Iterator<BigInteger> iter = bis.iterator();
      
      //
      // 128bits
      //
      
      byte[] allzeroes = new byte[16];
      Arrays.fill(allzeroes, (byte) 0); 
      byte[] allones = new byte[16];
      Arrays.fill(allones, (byte) 0xff);
      
      while (true) {
        try {
          if (!iter.hasNext()) {
            break;
          }
          BigInteger bi = iter.next();
          
          byte[] raw = bi.toByteArray();

          //
          // 128bits
          //
          
          if (raw.length != 16) {
            if (bi.signum() < 0) {
              out.write(allones, 0, 16 - raw.length);
            } else {
              out.write(allzeroes, 0, 16 - raw.length);              
            }
          }
          
          out.write(raw);
          count++;
        } catch (ConcurrentModificationException cme) {          
        }
      }          
    } catch (IOException ioe) {      
    } finally {
      if (null != out) {
        try { out.close(); } catch (Exception e) {}
      }
    }
    
    nano = System.nanoTime() - nano;
    
    LOG.info("Dumped " + count + " cache entries in " + (nano / 1000000.0D) + " ms.");    
  }
  
  private void loadCache() {
    if (null == this.cacheDumpPath) {
      return;
    }
    
    InputStream in = null;
    
    byte[] buf = new byte[8192];
    
    long nano = System.nanoTime();
    long count = 0;
    
    try {
      in = new GZIPInputStream(new FileInputStream(this.cacheDumpPath));
      
      int offset = 0;
      
      byte[] raw = new byte[16];
      
      while(true) {
        int len = in.read(buf, offset, buf.length - offset);
                
        offset += len;

        int idx = 0;
        
        while(idx < offset && offset - idx >= 16) {
          System.arraycopy(buf, idx, raw, 0, 16);
          BigInteger id = new BigInteger(raw);
          synchronized(this.metadataCache) {
            this.metadataCache.put(id, null);
          }
          count++;
          idx += 16;
        }
        
        if (idx < offset) {
          for (int i = idx; i < offset; i++) {
            buf[i - idx] = buf[i];
          }
          offset = offset - idx;
        } else {
          offset = 0;
        }
        
        if (len < 0) {
          break;
        }
      }      
    } catch (IOException ioe) {      
    } finally {
      if (null != in) {
        try { in.close(); } catch (Exception e) {}
      }
    }
    
    nano = System.nanoTime() - nano;
    
    LOG.info("Loaded " + count + " cache entries in " + (nano / 1000000.0D) + " ms.");
  }
}
