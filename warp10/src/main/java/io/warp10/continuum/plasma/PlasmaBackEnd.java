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

package io.warp10.continuum.plasma;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.warp10.BytesUtils;
import io.warp10.ThriftUtils;
import io.warp10.continuum.KafkaOffsetCounters;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.KafkaDataMessage;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.sensision.Sensision;

/**
 * Reads GTS updates in Kafka and pushes them to PlasmaFrontEnd instances via Kafka
 */
public class PlasmaBackEnd extends Thread implements NodeCacheListener {

  private static final Logger LOG = LoggerFactory.getLogger(PlasmaBackEnd.class);

  private final KeyStore keystore;

  //
  // Flag indicating whether or not we should update the subscriptions
  //

  private final AtomicBoolean updateSubscriptions = new AtomicBoolean(true);

  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_CONSUMER_BOOTSTRAP_SERVERS,
    io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_TOPIC,
    io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_GROUPID,
    io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_COMMITPERIOD,
    io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_NTHREADS,
    io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_OUT_PRODUCER_BOOTSTRAP_SERVERS,
    io.warp10.continuum.Configuration.PLASMA_BACKEND_SUBSCRIPTIONS_ZK_QUORUM,
    io.warp10.continuum.Configuration.PLASMA_BACKEND_SUBSCRIPTIONS_ZK_ZNODE,
  };

  /**
   * Flag for signaling abortion of consuming process
   */
  private final AtomicBoolean abort = new AtomicBoolean(false);

  private final Properties properties;

  private NodeCache cache;

  private Map<String,Set<BigInteger>> subscriptions = null;

  private boolean identicalSipHashKeys = false;
  private boolean identicalAESKeys = false;

  private final KafkaProducer<byte[],byte[]> kafkaProducer;

  private final List<ProducerRecord<byte[], byte[]>> msglist = new ArrayList<ProducerRecord<byte[],byte[]>>();

  private final AtomicLong msgsize = new AtomicLong();

  private final long KAFKA_OUT_MAXSIZE;

  private byte[] lastKnownData = null;

  private CuratorFramework curatorFramework = null;

  private String rootznode = null;

  /**
   * Barrier to synchronize the polling threads and the commit of offsets
   */
  private CyclicBarrier barrier = null;

  private AtomicBoolean mustSync = new AtomicBoolean(false);

  public PlasmaBackEnd(KeyStore keystore, final Properties props) {
    this.keystore = keystore;

    this.properties = (Properties) props.clone();

    //
    // Check mandatory parameters
    //

    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(required), "Missing configuration parameter '%s'.", required);
    }

    //
    // Extract parameters
    //

    this.KAFKA_OUT_MAXSIZE = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_OUT_MAXSIZE, "900000"));
    final int nthreads = Integer.valueOf(properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_NTHREADS));

    //
    // Extract keys
    //

    extractKeys(properties);

    //
    // Create the outbound producer
    //

    Properties dataProps = new Properties();

    properties.putAll(io.warp10.continuum.Configuration.extractPrefixed(props, props.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_OUT_PRODUCER_CONF_PREFIX)));

    // @see <a href="http://kafka.apache.org/documentation.html#producerconfigs">http://kafka.apache.org/documentation.html#producerconfigs</a>
    dataProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_OUT_PRODUCER_BOOTSTRAP_SERVERS));
    if (null != props.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_OUT_PRODUCER_CLIENTID)) {
      dataProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, props.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_OUT_PRODUCER_CLIENTID));
    }
    dataProps.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    dataProps.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, io.warp10.continuum.KafkaPartitioner.class.getName());
    dataProps.setProperty(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    dataProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
    dataProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");

    // FIXME(hbs): compression does not work
    //dataProps.setProperty("compression.codec", "snappy");

    this.kafkaProducer = new KafkaProducer<byte[], byte[]>(dataProps);

    //
    // Launch a Thread which will populate the metadata cache
    // We don't do that in the constructor otherwise it might take too long to return
    //

    final PlasmaBackEnd self = this;

    final String groupid = properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_GROUPID);
    final String topic = properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_TOPIC);
    final String strategy = properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY);
    final String clientid = properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_CONSUMER_CLIENTID);

    final long commitPeriod = 1000L;
    final KafkaOffsetCounters counters = new KafkaOffsetCounters(topic, groupid, commitPeriod * 2);

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {

        //
        // Enter an endless loop which will spawn 'nthreads' threads
        // each time the Kafka consumer is shut down.
        //

        while (true) {
          try {
            Map<String,Integer> topicCountMap = new HashMap<String, Integer>();

            topicCountMap.put(properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_TOPIC), nthreads);

            Properties props = new Properties();

            // Load explicit configuration
            props.putAll(io.warp10.continuum.Configuration.extractPrefixed(properties, properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_CONF_PREFIX)));

            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_CONSUMER_BOOTSTRAP_SERVERS));
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
            // Do not commit offsets in ZK
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            // Reset offset to largest
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            if (null != clientid) {
              props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientid);
            }
            if (null != strategy) {
              props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, strategy);
            }

            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");

            props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

            org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>[] consumers = null;

            // Reset the counters
            counters.reset();

            ExecutorService executor = Executors.newFixedThreadPool(nthreads);

            //
            // Barrier and flag for synchronizing during offset commits
            //

            barrier = new CyclicBarrier(nthreads + 1);
            mustSync.set(false);

            //
            // now create runnables which will consume messages
            //

            consumers = new org.apache.kafka.clients.consumer.KafkaConsumer[nthreads];

            Collection<String> topics = Collections.singletonList(topic);

            for (int i = 0; i < nthreads; i++) {
              consumers[i] = new org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>(props);
              executor.submit(new KafkaConsumer(self, consumers[i], counters, topics));
            }

            long lastsync = 0L;

            while(!abort.get() && !Thread.currentThread().isInterrupted()) {
              LockSupport.parkNanos(100000000L);

              //
              // Publish offsets every second to Sensision, after flushing the offsets
              //

              if (System.currentTimeMillis() - lastsync > commitPeriod) {
                mustSync.set(true);
                while(nthreads != barrier.getNumberWaiting()) {
                  LockSupport.parkNanos(10000L);
                }
                try {
                  for (org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer: consumers) {
                    consumer.commitSync();
                  }
                  counters.sensisionPublish();
                  lastsync = System.currentTimeMillis();
                } catch (Exception e) {
                  abort.set(true);
                } finally {
                  mustSync.set(false);
                  barrier.await();
                }
              }
            }

            //
            // We exited the loop, this means one of the threads triggered an abort,
            // we will shut down the executor and shut down the connector to start over.
            //

            executor.shutdownNow();

            for (org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer: consumers) {
              try {
                consumer.close();
              } catch (Exception e) {
              }
            }
            abort.set(false);
          } catch (Throwable t) {
            LOG.error("Error while spawning KafkaConsumers.", t);
          } finally {
            LockSupport.parkNanos(1000000L);
          }
        }
      }
    });

    t.setName("Plasma Backend Spawner");
    t.setDaemon(true);
    t.start();

    this.setName("Plasma Backend");
    this.setDaemon(true);
    this.start();
  }

  @Override
  public void run() {
    //
    // Initialize Curator
    //

    this.curatorFramework = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(5000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_SUBSCRIPTIONS_ZK_QUORUM))
        .build();
    this.curatorFramework.start();

    //
    // Create the subscription path if it does not exist
    //

    try {
      Stat stat = this.curatorFramework.checkExists().forPath(properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_SUBSCRIPTIONS_ZK_ZNODE));
      if (null == stat) {
        this.curatorFramework.create().creatingParentsIfNeeded().forPath(properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_SUBSCRIPTIONS_ZK_ZNODE));
      }
    } catch (Exception e) {
      LOG.error("Error while creating subscription znode", e);
    }

    //
    // Create a Path Cache
    //

    this.cache = new NodeCache(curatorFramework, properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_SUBSCRIPTIONS_ZK_ZNODE));
    cache.getListenable().addListener(this);

    try {
      cache.start(true);
    } catch (Exception e) {
      LOG.error("Error while starting subscription cache", e);
    }

    //
    // Endlessly update the subscriptions, at most once every second
    //

    while(true) {
      LockSupport.parkNanos(1000000L);

      boolean update = this.updateSubscriptions.getAndSet(false);

      if (!update) {
        continue;
      }

      subscriptionUpdate();
    }
  }

  /**
   * Update the subscriptions
   */
  private void subscriptionUpdate() {
    //
    // Update number of subscription updates
    //

    Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_SUBSCRIPTIONS_UPDATES, Sensision.EMPTY_LABELS, 1);

    //
    // Reread current data
    // FIXME(hbs): this should be improved when running many PlasmaFE
    //

    List<String> entries = null;

    try {
      entries = this.curatorFramework.getChildren().forPath(properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_SUBSCRIPTIONS_ZK_ZNODE));
    } catch (Exception e) {
      LOG.error("Error while retrieving subscriptions", e);
    }

    if (null == entries) {
      return;
    }

    // Subscriptions by topic
    Map<String, Set<BigInteger>> subsbytopic = new HashMap<String, Set<BigInteger>>();

    for (String entry: entries) {
      try {
        // Strip subscriptions znode
        //path = path.substring(properties.getProperty(PLASMA_BACKEND_SUBSCRIPTIONS_ZNODE).length() + 1);
        // Extract path components (VERSION.UUID.TOPIC.CHUNK.HASH)
        String[] tokens = entry.split("\\.");

        UUID uuid = UUID.fromString(tokens[1]);
        String topic = tokens[2];

        long hash = new BigInteger(tokens[4], 16).longValue();

        String path = properties.getProperty(io.warp10.continuum.Configuration.PLASMA_BACKEND_SUBSCRIPTIONS_ZK_ZNODE) + "/" + entry;

        // Extract content
        byte[] content = this.curatorFramework.getData().forPath(path);

        // Compute SipHash of content
        long sip = SipHashInline.hash24(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), content, 0, content.length);

        if (hash != sip) {
          Map<String,String> labels = new HashMap<String, String>();
          labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, topic);
          Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_SUBSCRIPTIONS_INVALID_HASHES, labels, 1);
          continue;
        }

        //
        // Get subscription map for topic
        //

        Set<BigInteger> subs = subsbytopic.get(topic);

        if (null == subs) {
          subs = new HashSet<BigInteger>();
          subsbytopic.put(topic, subs);
        }

        byte[] bytes = new byte[16];

        for (int i = 0; i < content.length; i += 16) {
          System.arraycopy(content, i, bytes, 0, 16);
          BigInteger id = new BigInteger(bytes);
          subs.add(id);
        }
      } catch (Exception e) {
        LOG.error("Error while scanning subscriptions", e);
      }
    }

    //
    // Update number of subscriptions per topic
    //

    for (Map.Entry<String, Set<BigInteger>> topicAndSubs: subsbytopic.entrySet()) {
      Map<String,String> labels = new HashMap<String, String>();
      labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, topicAndSubs.getKey());
      Sensision.set(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_SUBSCRIPTIONS, labels, topicAndSubs.getValue().size());
    }

    // Reset number of subscriptions per topic absent from 'subsbytopic'

    if (null != this.subscriptions) {
      for (String topic: this.subscriptions.keySet()) {
        if (!subsbytopic.containsKey(topic)) {
          Map<String,String> labels = new HashMap<String, String>();
          labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, topic);
          Sensision.set(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_SUBSCRIPTIONS, labels, 0);
        }
      }
    }

    //
    // Replace subscriptions
    //

    this.subscriptions = subsbytopic.isEmpty() ? null : subsbytopic;
  }

  @Override
  public void nodeChanged() throws Exception {
    //
    // Do something quick, simply update a flag when the content of the node changed (not its children)
    //

    byte[] data = this.cache.getCurrentData().getData();

    if (null == lastKnownData || null == data || 0 != BytesUtils.compareTo(lastKnownData, data)) {
      lastKnownData = null == data ? null : Arrays.copyOf(data, data.length);
      this.updateSubscriptions.set(true);
    }
  }

  /**
   * Extract Directory related keys and populate the KeyStore with them.
   *
   * @param props Properties from which to extract the key specs
   */
  private void extractKeys(Properties props) {
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_IN, props, io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_MAC, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_KAFKA_PLASMA_BACKEND_IN, props, io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_IN_AES, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_OUT, props, io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_OUT_MAC, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_KAFKA_PLASMA_BACKEND_OUT, props, io.warp10.continuum.Configuration.PLASMA_BACKEND_KAFKA_OUT_AES, 128, 192, 256);


    //
    // Check if the inbound and outbound SipHash/AES keys are identical this is to speed up dispatching
    //

    if (null != this.keystore.getKey(KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_IN) && null != this.keystore.getKey(KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_OUT)) {
      if (0 == BytesUtils.compareTo(this.keystore.getKey(KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_IN), this.keystore.getKey(KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_OUT))) {
        this.identicalSipHashKeys = true;
      }
    } else if (null == this.keystore.getKey(KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_IN) && null == this.keystore.getKey(KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_OUT)) {
      this.identicalSipHashKeys = true;
    } else {
      this.identicalSipHashKeys = false;
    }

    if (null != this.keystore.getKey(KeyStore.AES_KAFKA_PLASMA_BACKEND_IN) && null != this.keystore.getKey(KeyStore.AES_KAFKA_PLASMA_BACKEND_OUT)) {
      if (0 == BytesUtils.compareTo(this.keystore.getKey(KeyStore.AES_KAFKA_PLASMA_BACKEND_IN), this.keystore.getKey(KeyStore.AES_KAFKA_PLASMA_BACKEND_OUT))) {
        this.identicalAESKeys = true;
      }
    } else if (null == this.keystore.getKey(KeyStore.AES_KAFKA_PLASMA_BACKEND_IN) && null == this.keystore.getKey(KeyStore.AES_KAFKA_PLASMA_BACKEND_OUT)) {
      this.identicalAESKeys = true;
    } else {
      this.identicalAESKeys = false;
    }

  }

  private static class KafkaConsumer implements Runnable {

    private final PlasmaBackEnd backend;
    private final org.apache.kafka.clients.consumer.KafkaConsumer<byte[],byte[]> consumer;
    private final Collection<String> topics;
    private final KafkaOffsetCounters counters;

    public KafkaConsumer(PlasmaBackEnd backend, org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer, KafkaOffsetCounters counters, Collection<String> topics) {
      this.backend = backend;
      this.consumer = consumer;
      this.topics = topics;
      this.counters = counters;
    }

    @Override
    public void run() {
      long count = 0L;

      byte[] clslbls = new byte[16];

      try {
        this.consumer.subscribe(topics);

        byte[] inSipHashKey = backend.keystore.getKey(KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_IN);
        byte[] inAESKey = backend.keystore.getKey(KeyStore.AES_KAFKA_PLASMA_BACKEND_IN);

        byte[] outSipHashKey = backend.keystore.getKey(KeyStore.SIPHASH_KAFKA_PLASMA_BACKEND_OUT);
        byte[] outAESKey = backend.keystore.getKey(KeyStore.AES_KAFKA_PLASMA_BACKEND_OUT);

        // Iterate on the messages
        TDeserializer deserializer = ThriftUtils.getTDeserializer(new TCompactProtocol.Factory());

        // TODO(hbs): allow setting of writeBufferSize

        //Kafka 2.x Duration delay = Duration.of(500L, ChronoUnit.MILLIS);
        long delay = 500L;

        boolean polling = false;

        while (!backend.abort.get()) {
          //
          // If synchronization should happen, wait on the synchronization barrier
          //
          if (backend.mustSync.get()) {
            backend.barrier.await();
          }

          ConsumerRecords<byte[], byte[]> records = consumer.poll(delay);

          boolean first = true;

          for (ConsumerRecord<byte[], byte[]> record : records) {

            if (!first) {
              throw new RuntimeException("Invalid input, expected a single record, got " + records.count());
            }

            first = false;

            count++;
            counters.count(record.partition(), record.offset());

            // Do nothing if there are no subscriptions
            if (null == backend.subscriptions || backend.subscriptions.isEmpty()) {
              continue;
            }

            byte[] data = record.value();

            Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_KAFKA_IN_MESSAGES, Sensision.EMPTY_LABELS, 1);
            Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_KAFKA_IN_BYTES, Sensision.EMPTY_LABELS, data.length);

            if (null != inSipHashKey) {
              data = CryptoUtils.removeMAC(inSipHashKey, data);
            }

            // Skip data whose MAC was not verified successfully
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_KAFKA_IN_INVALIDMACS, Sensision.EMPTY_LABELS, 1);
              continue;
            }

            // Unwrap data if need be
            if (null != inAESKey) {
              data = CryptoUtils.unwrap(inAESKey, data);
            }

            // Skip data that was not unwrapped successfully
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_KAFKA_IN_INVALIDCIPHERS, Sensision.EMPTY_LABELS, 1);
              continue;
            }

            //
            // Extract KafkaDataMessage
            //

            KafkaDataMessage tmsg = new KafkaDataMessage();
            deserializer.deserialize(tmsg, data);

            switch(tmsg.getType()) {
              case STORE:
                backend.dispatch(clslbls, record, tmsg, outSipHashKey, outAESKey);
                break;
              case DELETE:
                break;
              default:
                throw new RuntimeException("Invalid message type.");
            }
          }
        }
      } catch (Throwable t) {
        LOG.error("Error processing subscription messages from Kafka.", t);
      } finally {
        // Set abort to true in case we exit the 'run' method
        backend.abort.set(true);
      }
    }
  }

  /**
   * Dispatch the message to the various topics
   *
   * @param clslbls stable array to extract classId/labelsId
   * @param record Original record, its key/value will be re-used if SipHash/AES keys match
   * @param msg payload of the original message, in case we need to re-hash/re-encrypt it
   */
  private void dispatch(byte[] clslbls, ConsumerRecord<byte[], byte[]> record, KafkaDataMessage msg, byte[] outSipHashKey, byte[] outAESKey) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("DISPATCHING " + record);
    }

    if (null == this.subscriptions || this.subscriptions.isEmpty()) {
      return;
    }

    long classid = msg.getClassId();
    long labelsid = msg.getLabelsId();

    for (int i = 0; i < 8; i++) {
      // 128bits
      clslbls[7 - i] = (byte) (classid & 0xff);
      clslbls[15 - i] = (byte) (labelsid & 0xff);
      classid >>>= 8;
      labelsid >>>= 8;
    }

    BigInteger id = new BigInteger(clslbls);

    Map<String,Set<BigInteger>> subs = this.subscriptions;

    // Is the record ready to be sent?
    boolean msgReady = this.identicalAESKeys && this.identicalSipHashKeys;

    byte[] key = record.key();
    byte[] value = null;

    Map<String,String> labels = new HashMap<String, String>();

    for (Map.Entry<String, Set<BigInteger>> topicAndSubs: subs.entrySet()) {
      String topic = topicAndSubs.getKey();

      if (!topicAndSubs.getValue().contains(id)) {
        continue;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("DISPATCHING TO TOPIC " + topic);
      }

      //
      // Repackage the record
      //

      ProducerRecord<byte[],byte[]> outmsg = null;

      if (msgReady) {
        value = record.value();
        outmsg = new ProducerRecord<byte[], byte[]>(topic, key, value);
      } else {
        if (null == value) {
          TSerializer serializer = ThriftUtils.getTSerializer(new TCompactProtocol.Factory());

          try {
            value = serializer.serialize(msg);
          } catch (TException te) {
            // Ignore the error
            continue;
          }

          //
          // Encrypt value if the AES key is defined
          //

          if (null != outAESKey) {
            value = CryptoUtils.wrap(outAESKey, value);
          }

          //
          // Compute MAC if the SipHash key is defined
          //

          if (null != outSipHashKey) {
            value = CryptoUtils.addMAC(outSipHashKey, value);
          }
        }
        outmsg = new ProducerRecord<byte[], byte[]>(topic, key, value);
      }

      try {
        sendDataMessage(outmsg);
      } catch (IOException ioe) {
      }
    }

    // Force flushing of KafkaMessages, this will trigger the actual push to Kafka
    // so we send all outgoing messages at once
    try {
      sendDataMessage(null);
    } catch (IOException ioe) {
    }
  }

  private synchronized void sendDataMessage(ProducerRecord<byte[], byte[]> msg) throws IOException {

    long thismsg = 0L;
    if (null != msg) {
      thismsg = msg.key().length + msg.value().length;
    }

    // FIXME(hbs): we check if the size would outgrow the maximum, if so we flush the message before.
    // in Ingress we add the message first, which could lead to a msg too big for Kafka, we will need
    // to fix Ingress at some point.

    if (msglist.size() > 0 && (null == msg || msgsize.get() + thismsg > KAFKA_OUT_MAXSIZE)) {
      Future[] futures = new Future[msglist.size()];

      int idx = 0;
      for (ProducerRecord record: msglist) {
        futures[idx++] = this.kafkaProducer.send(record);
      }
      this.kafkaProducer.flush();

      for (Future future: futures) {
        try {
          future.get();
        } catch (Exception e) {
          throw new IOException(e);
        }
      }

      Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_KAFKA_OUT_SENT, Sensision.EMPTY_LABELS, 1);
      msglist.clear();
      msgsize.set(0L);
    }

    if (null != msg) {
      msglist.add(msg);
      msgsize.addAndGet(thismsg);
      Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_KAFKA_OUT_MESSAGES, Sensision.EMPTY_LABELS, 1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_BACKEND_KAFKA_OUT_BYTES, Sensision.EMPTY_LABELS, thismsg);
    }
  }
 }
