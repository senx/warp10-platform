//
//   Copyright 2018-2022  SenX S.A.S.
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

package io.warp10.continuum;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.script.thrift.data.WebCallRequest;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandaloneWebCallService;

/**
 * Class which implements pulling data from Kafka and making WebCall calls
 */
public class KafkaWebCallBroker extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaWebCallBroker.class);

  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    Configuration.WEBCALL_NTHREADS,
    Configuration.WEBCALL_KAFKA_CONSUMER_BOOTSTRAP_SERVERS,
    Configuration.WEBCALL_KAFKA_PRODUCER_BOOTSTRAP_SERVERS,
    Configuration.WEBCALL_KAFKA_TOPIC,
    Configuration.WEBCALL_KAFKA_GROUPID,
    Configuration.WEBCALL_KAFKA_COMMITPERIOD,
  };

  /**
   * Keystore
   */
  private final KeyStore keystore;

  /**
   * Flag indicating an abort
   */
  private final AtomicBoolean abort = new AtomicBoolean(false);

  /**
   * CyclicBarrier for synchronizing producers prior to committing the offsets
   */
  private CyclicBarrier barrier;

  /**
   * Number of milliseconds between offsets commits
   */
  private final long commitPeriod;

  public KafkaWebCallBroker(KeyStore keystore, final Properties properties) throws IOException {
    this.keystore = keystore;

    //
    // Check mandatory parameters
    //

    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(required), "Missing configuration parameter '%s'.", required);
    }

    //
    // Extract parameters
    //

    final String topic = properties.getProperty(Configuration.WEBCALL_KAFKA_TOPIC);
    final int nthreads = Integer.valueOf(properties.getProperty(Configuration.WEBCALL_NTHREADS));

    //
    // Extract keys
    //

    KafkaWebCallService.initKeys(keystore, properties);

    keystore.forget();

    final KafkaWebCallBroker self = this;

    commitPeriod = Long.parseLong(properties.getProperty(Configuration.WEBCALL_KAFKA_COMMITPERIOD));

    final String groupid = properties.getProperty(Configuration.WEBCALL_KAFKA_GROUPID);

    final KafkaOffsetCounters counters = new KafkaOffsetCounters(topic, groupid, commitPeriod * 2);

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {

        ExecutorService executor = null;
        KafkaConsumer[] consumers = null;

        while(true) {
          try {
            //
            // Enter an endless loop which will spawn 'nthreads' threads
            // each time the Kafka consumer is shut down
            //

            Map<String,Integer> topicCountMap = new HashMap<String, Integer>();

            topicCountMap.put(topic, nthreads);

            Properties props = new Properties();

            // Load explicit configuration
            props.putAll(Configuration.extractPrefixed(properties, properties.getProperty(Configuration.WEBCALL_KAFKA_CONSUMER_CONF_PREFIX)));

            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(Configuration.WEBCALL_KAFKA_CONSUMER_BOOTSTRAP_SERVERS));
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
            if (null != properties.getProperty(Configuration.WEBCALL_KAFKA_CONSUMER_CLIENTID)) {
              props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, properties.getProperty(Configuration.WEBCALL_KAFKA_CONSUMER_CLIENTID));
            }
            if (null != properties.getProperty(Configuration.WEBCALL_KAFKA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY)) {
              props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, properties.getProperty(Configuration.WEBCALL_KAFKA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY));
            }
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

            // Force number of records returned by poll to be at most 1
            props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

            self.barrier = new CyclicBarrier(nthreads + 1);

            executor = Executors.newFixedThreadPool(nthreads);

            //
            // now create runnables which will consume messages
            //

            counters.reset();

            consumers = new KafkaConsumer[nthreads];

            for (int i = 0; i < nthreads; i++) {
              consumers[i] = new KafkaConsumer<>(props);
              consumers[i].subscribe(Collections.singletonList(topic));
              executor.submit(new WebCallConsumer(self, consumers[i], counters));
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
                  for (KafkaConsumer consumer: consumers) {
                    consumer.commitSync();
                  }

                  Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_KAFKA_IN_COMMITS, Sensision.EMPTY_LABELS, 1);

                  // Release the waiting threads
                  try {
                    barrier.await();
                  } catch (Exception e) {
                    break;
                  }
                }
              } catch (Throwable t) {
                t.printStackTrace(System.out);
                LOG.error("",t);
                abort.set(true);
              }

              LockSupport.parkNanos(100000000L);
            }
          } catch (Throwable t) {
            LOG.error("", t);
          } finally {
            //
            // We exited the loop, this means one of the threads triggered an abort,
            // we will shut down the executor and shut down the connector to start over.
            //

            if (null != executor) {
              try {
                executor.shutdownNow();
              } catch (Exception e) {
              }
            }
            if (null != consumers) {
              for (KafkaConsumer consumer: consumers) {
                try {
                  consumer.close();
                } catch (Exception e) {
                }
              }
            }

            Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_IN_ABORTS, Sensision.EMPTY_LABELS, 1);

            abort.set(false);

            LockSupport.parkNanos(1000000000L);
          }
        }
      }
    });
    t.setName("[WebCall Broker Spawner]");
    t.setDaemon(true);
    t.start();

    this.setName("[WebCall Broker]");
    this.setDaemon(true);
    this.start();
  }

  @Override
  public void run() {
    while (true){
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException ie) {
      }
    }
  }

  private static class WebCallConsumer implements Runnable {
    private final KafkaWebCallBroker broker;
    private final KafkaConsumer<byte[],byte[]> consumer;
    private final KafkaOffsetCounters counters;

    public WebCallConsumer(KafkaWebCallBroker broker, KafkaConsumer<byte[], byte[]> consumer, KafkaOffsetCounters counters) {
      this.broker = broker;
      this.consumer = consumer;
      this.counters = counters;
    }

    @Override
    public void run() {

      Thread.currentThread().setName("[WebCall Broker Consumer]");

      long count = 0L;

      final ReentrantLock lock = new ReentrantLock(true);

      try {
        byte[] siphashKey = broker.keystore.getKey(KeyStore.SIPHASH_KAFKA_WEBCALL);
        byte[] aesKey = broker.keystore.getKey(KeyStore.AES_KAFKA_WEBCALL);

        //
        // Start the synchronization Thread
        //

        Thread synchronizer = new Thread(new Runnable() {
          @Override
          public void run() {
            long lastsync = System.currentTimeMillis();

            //
            // Check for how long we've been storing readings, if we've reached the commitperiod,
            // flush any pending commits and synchronize with the other threads so offsets can be committed
            //

            while(true) {
              long now = System.currentTimeMillis();

              if (now - lastsync > broker.commitPeriod) {
                //
                // Now join the cyclic barrier which will trigger the
                // commit of offsets
                // We lock on 'lock' so KafkaConsumer is not accessed while we wait
                // for the commit of offsets (as KafkaConsumer is not thread safe)
                //
                try {
                  lock.lockInterruptibly();
                  broker.barrier.await();
                  Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_BARRIER_SYNCS, Sensision.EMPTY_LABELS, 1);
                } catch (Exception e) {
                  broker.abort.set(true);
                  return;
                } finally {
                  lastsync = System.currentTimeMillis();
                  if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                  }
                }
              }

              LockSupport.parkNanos(100000000L);
            }
          }
        });

        synchronizer.setName("[WebCall Broker Synchronizer]");
        synchronizer.setDaemon(true);
        synchronizer.start();

        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

        //Kafka 2.x Duration delay = Duration.of(500, ChronoUnit.MILLIS);
        long delay = 500L;
        while (!broker.abort.get()) {
          ConsumerRecords<byte[], byte[]> records = null;

          try {
            lock.lockInterruptibly();
            records = consumer.poll(delay);
          } finally {
            if (lock.isHeldByCurrentThread()) {
              lock.unlock();
            }
          }

          boolean first = true;

          for (ConsumerRecord<byte[], byte[]> record: records) {
            if (!first) {
              throw new RuntimeException("Invalid input, expected a single record, got " + records.count());
            }

            first = false;

            count++;
            counters.count(record.partition(), record.offset());

            byte[] data = record.value();

            Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_KAFKA_IN_COUNT, Sensision.EMPTY_LABELS, 1);
            Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_KAFKA_IN_BYTES, Sensision.EMPTY_LABELS, data.length);

            if (null != siphashKey) {
              data = CryptoUtils.removeMAC(siphashKey, data);
            }

            // Skip data whose MAC was not verified successfully
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_KAFKA_IN_FAILEDMACS, Sensision.EMPTY_LABELS, 1);
              continue;
            }

            // Unwrap data if need be
            if (null != aesKey) {
              data = CryptoUtils.unwrap(aesKey, data);
            }

            // Skip data that was not unwrapped successfuly
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_KAFKA_IN_FAILEDDECRYPTS, Sensision.EMPTY_LABELS, 1);
              // TODO(hbs): increment Sensision metric
              continue;
            }

            //
            // Extract KafkaDataMessage
            //

            WebCallRequest request = new WebCallRequest();
            deserializer.deserialize(request, data);

            //
            // Update latency
            //

            Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_LATENCY_MS, Sensision.EMPTY_LABELS, System.currentTimeMillis() - request.getTimestamp());


            //
            // Issue the WebCall
            //

            StandaloneWebCallService.doCall(request);
          }
        }
      } catch (Throwable t) {
        LOG.warn("WebCall Broker failed.", t);
        // FIXME(hbs): update Sensision metrics
      } finally {
        // Set abort to true in case we exit the 'run' method
        broker.abort.set(true);
      }
    }
  }
}
