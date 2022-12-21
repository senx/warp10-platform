//
//   Copyright 2019-2022  SenX S.A.S.
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

import java.util.Collection;
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic class handling Kafka topic consumption and handling
 * by multiple threads.
 *
 * One thread called the spawner will create consuming threads
 * at launch time and when an abort situation is encountered.
 *
 * One thread called the synchronizer will periodically synchronize
 * the consuming threads via a cyclic barrier and commit the offsets.
 *
 * The consuming threads consume the Kafka messages and act upon them.
 */
public class KafkaSynchronizedConsumerPool {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSynchronizedConsumerPool.class);

  private Properties initialConfig;
  private CyclicBarrier barrier;
  private final AtomicBoolean abort;
  private final AtomicBoolean initialized;

  private final Synchronizer synchronizer;
  private final Spawner spawner;

  private final KafkaOffsetCounters counters;

  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private Map<KafkaConsumer,ReentrantLock> consumerLocks = null;
  private Map<KafkaConsumer,AtomicBoolean> pending = null;

  private AtomicBoolean mustCommit = new AtomicBoolean(false);

  public static interface ConsumerFactory {
    public Runnable getConsumer(KafkaSynchronizedConsumerPool pool, KafkaConsumer<byte[], byte[]> stream, Collection<String> topics);
  }

  public static interface Hook {
    public void call();
  }

  private static class Synchronizer extends Thread {

    private final KafkaSynchronizedConsumerPool pool;
    private final long commitPeriod;
    private Hook syncHook = null;

    public Synchronizer(KafkaSynchronizedConsumerPool pool, long commitPeriod) {
      this.pool = pool;
      this.commitPeriod = commitPeriod;
    }

    @Override
    public void run() {
      long lastsync = System.currentTimeMillis();

      //
      // Check for how long we've been storing readings, if we've reached the commitperiod,
      // flush any pending commits and synchronize with the other threads so offsets can be committed
      //

      while(true) {
        long now = System.currentTimeMillis();

        //
        // Only do something is the pool has been initialized (or re-initialized)
        //

        if (pool.getInitialized().get() && !pool.getAbort().get() && (now - lastsync > commitPeriod)) {
          //
          // Now join the cyclic barrier which will trigger the
          // commit of offsets
          //
          try {
            pool.mustCommit.set(true);
            pool.getBarrier().await();
            if (null != syncHook) {
              syncHook.call();
            }
            // MOVE into sync hook - Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_BARRIER_SYNCS, Sensision.EMPTY_LABELS, 1);
          } catch (Exception e) {
            pool.getAbort().set(true);
          } finally {
            lastsync = System.currentTimeMillis();
          }
        }

        LockSupport.parkNanos(100000000L);
      }
    }
  }

  private static class Spawner extends Thread {

    private final KafkaSynchronizedConsumerPool pool;
    private final String zkconnect;
    private final String topic;
    private final String groupid;
    private final String clientid;
    private final String strategy;
    private final String autoOffsetReset;
    private final int nthreads;
    private final ConsumerFactory factory;

    private Hook abortHook = null;
    private Hook preCommitOffsetHook = null;
    private Hook commitOffsetHook = null;

    public Spawner(KafkaSynchronizedConsumerPool pool, String zkconnect, String topic, String clientid, String groupid, String strategy, String autoOffsetReset, int nthreads, ConsumerFactory factory) {
      this.pool = pool;
      this.zkconnect = zkconnect;
      this.topic = topic;
      this.groupid = groupid;
      this.strategy = strategy;
      this.autoOffsetReset = autoOffsetReset;
      this.clientid = clientid;
      this.nthreads = nthreads;
      this.factory = factory;
    }

    public void run() {

      ExecutorService executor = null;

      while(!pool.shutdown.get()) {

        KafkaConsumer[] consumers = null;

        try {
          //
          // Enter an endless loop which will spawn 'nthreads' threads
          // each time the Kafka consumer is shut down
          //

          Properties props = new Properties();

          // Load explicit configuration
          props.putAll(this.pool.initialConfig);

          props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.zkconnect);
          props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,this.groupid);
          if (null != this.clientid) {
            props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, this.clientid);
          }
          if (null != this.strategy) {
            props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, this.strategy);
          }
          props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

          if (null != this.autoOffsetReset) {
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.autoOffsetReset);
          }

          props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
          props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

          // Only retrieve a single message per call to poll
          props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

          // Reset counters so we only export metrics for partitions we really consume
          pool.getCounters().reset();

          // 1 for the Synchronizer, 1 for the Spawner

          pool.setBarrier(new CyclicBarrier(1 + 1));

          executor = Executors.newFixedThreadPool(nthreads);

          //
          // now create runnables which will consume messages
          //

          consumers = new KafkaConsumer[nthreads];
          pool.consumerLocks = new HashMap<KafkaConsumer,ReentrantLock>(nthreads);
          pool.pending = new HashMap<KafkaConsumer,AtomicBoolean>(nthreads);

          Collection<String> topics = Collections.singletonList(topic);

          for (int i = 0; i < nthreads; i++) {
            consumers[i] = new KafkaConsumer<>(props);
            pool.consumerLocks.put(consumers[i], new ReentrantLock());
            pool.pending.put(consumers[i], new AtomicBoolean(false));
            executor.submit(factory.getConsumer(pool,consumers[i], topics));
          }

          pool.getInitialized().set(true);

          while(!pool.getAbort().get() && !Thread.currentThread().isInterrupted()) {
            try {
              if (1 == pool.getBarrier().getNumberWaiting()) {
                //
                // Check if we should abort, which could happen when
                // an exception was thrown when flushing the commits just before
                // entering the barrier
                //

                if (pool.getAbort().get()) {
                  break;
                }

                //
                // All processing threads are waiting on the barrier, this means we can flush the offsets because
                // they have all processed data successfully for the given activity period
                //


                if (null != preCommitOffsetHook) {
                  preCommitOffsetHook.call();
                }

                // Commit offsets
                try {
                  for (KafkaConsumer consumer: consumers) {
                    ReentrantLock lock = pool.consumerLocks.get(consumer);
                    AtomicBoolean pending = pool.pending.get(consumer);

                    while(true) {
                      try {
                        lock.lockInterruptibly();
                        // Check if the consumer has pending records, if so,
                        // continue the look
                        if (pending.get()) {
                          continue;
                        }
                        // No pending records, commit the offsets and exit the loop
                        consumer.commitSync();
                        break;
                      } finally {
                        if (lock.isHeldByCurrentThread()) {
                          lock.unlock();
                        }
                      }
                    }
                  }
                } finally {
                  pool.mustCommit.set(false);
                }

                pool.getCounters().commit();
                pool.getCounters().sensisionPublish();

                if (null != commitOffsetHook) {
                  commitOffsetHook.call();
                }
                // MOVE in COMMIT HOOK - Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_KAFKA_IN_COMMITS, Sensision.EMPTY_LABELS, 1);

                // Release the waiting threads
                try {
                  pool.getBarrier().await();
                } catch (Exception e) {
                  break;
                }
              }
            } catch (Throwable t) {
              pool.getAbort().set(true);
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

          if (null != abortHook) {
            abortHook.call();
          }
          // MOVE in ABORT HOOK -           Sensision.update(SensisionConstants.SENSISION_CLASS_WEBCALL_IN_ABORTS, Sensision.EMPTY_LABELS, 1);

          // Set initialized to false first as this is what is checked by the Synchronizer
          pool.getInitialized().set(false);
          pool.getAbort().set(false);

          LockSupport.parkNanos(1000000000L);
        }
      }

      pool.stopped.set(true);
    }
  }

  public KafkaSynchronizedConsumerPool(Properties initialConfig, String zkconnect, String topic, String clientid, String groupid, String strategy, int nthreads, long commitPeriod, ConsumerFactory factory) {
    this(initialConfig, zkconnect, topic, clientid, groupid, strategy, null, nthreads, commitPeriod, factory);
  }

  public KafkaSynchronizedConsumerPool(Properties initialConfig, String zkconnect, String topic, String clientid, String groupid, String strategy, String autoOffsetReset, int nthreads, long commitPeriod, ConsumerFactory factory) {

    this.initialConfig = initialConfig;
    this.abort = new AtomicBoolean(false);
    this.initialized = new AtomicBoolean(false);

    this.synchronizer = new Synchronizer(this, commitPeriod);
    this.spawner = new Spawner(this, zkconnect, topic, clientid, groupid, strategy, autoOffsetReset, nthreads, factory);

    this.counters = new KafkaOffsetCounters(topic, groupid, commitPeriod * 2);

    synchronizer.setName("[Synchronizer '" + topic + "' (" + groupid + ") nthr=" + nthreads + " every " + commitPeriod + " ms]");
    synchronizer.setDaemon(true);
    synchronizer.start();

    spawner.setName("[Spawner '" + topic + "' (" + groupid + ") nthr=" + nthreads + " every " + commitPeriod + " ms]");
    spawner.setDaemon(true);
    spawner.start();
  }

  private void setBarrier(CyclicBarrier barrier) {
    this.barrier = barrier;
  }

  private CyclicBarrier getBarrier() {
    return this.barrier;
  }

  public AtomicBoolean getAbort() {
    return this.abort;
  }

  public AtomicBoolean getInitialized() {
    return this.initialized;
  }

  public void setAbortHook(Hook hook) {
    this.spawner.abortHook = hook;
  }

  public void setPreCommitOffsetHook(Hook hook) {
    this.spawner.preCommitOffsetHook = hook;
  }

  public void setCommitOffsetHook(Hook hook) {
    this.spawner.commitOffsetHook = hook;
  }

  public void setSyncHook(Hook hook) {
    this.synchronizer.syncHook = hook;
  }

  public KafkaOffsetCounters getCounters() {
    return this.counters;
  }

  public void shutdown() {
    // Set shutdown to true first
    this.shutdown.set(true);
    this.abort.set(true);
  }

  public boolean isStopped() {
    return this.stopped.get();
  }

  //Kafka 2.xpublic ConsumerRecords poll(KafkaConsumer consumer, Duration delay) throws InterruptedException {
  public ConsumerRecords poll(KafkaConsumer consumer, long delay) throws InterruptedException {
    ReentrantLock lock = consumerLocks.get(consumer);

    if (null == lock) {
      throw new RuntimeException("Unknown consumer.");
    }

    try {
      this.pending.get(consumer).set(false);
      lock.lockInterruptibly();
      // If the pool must commit, return an empty set of records
      if (this.mustCommit.get()) {
        return new ConsumerRecords(new HashMap());
      }
      ConsumerRecords records = consumer.poll(delay);
      this.pending.get(consumer).set(records.count() > 0);
      return records;
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
    }
  }
}
