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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.KafkaOffsetCounters;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.KafkaDataMessage;
import io.warp10.continuum.store.thrift.data.KafkaDataMessageType;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.fdb.FDBClearRange;
import io.warp10.fdb.FDBContext;
import io.warp10.fdb.FDBMutation;
import io.warp10.fdb.FDBSet;
import io.warp10.fdb.FDBUtils;
import io.warp10.sensision.Sensision;

/**
 * Class which implements pulling data from Kafka and storing it in
 * FoundationDB
 */
public class Store extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(Store.class);

  public static final String DEFAULT_FDB_RETRYLIMIT = Long.toString(4);

  private static RateLimiter rateLimit = null;

  static {
    //
    // If instructed to do so, launch a thread which will read the throttling file periodically
    //

    if (null != WarpConfig.getProperty(Configuration.STORE_THROTTLING_FILE)) {

      final File throttlingFile = new File(WarpConfig.getProperty(Configuration.STORE_THROTTLING_FILE));
      final long period = Long.parseLong(WarpConfig.getProperty(Configuration.STORE_THROTTLING_PERIOD, "60000"));

      Thread t = new Thread() {
        @Override
        public void run() {
          while (true) {
            BufferedReader br = null;
            try {
              if (throttlingFile.exists()) {
                br = new BufferedReader(new FileReader(throttlingFile));

                String line = br.readLine();

                if (null == line) {
                  Store.rateLimit = null;
                } else {
                  double rate = Double.parseDouble(line);
                  if (null == Store.rateLimit && 0.0D != rate) {
                    Store.rateLimit = RateLimiter.create(rate);
                  } else if (0.0D != rate) {
                    Store.rateLimit.setRate(rate);
                  } else {
                    Store.rateLimit = null;
                  }
                }
              } else {
                Store.rateLimit = null;
                Sensision.clear(SensisionConstants.CLASS_WARP_STORE_THROTTLING_RATE, Sensision.EMPTY_LABELS);
              }
            } catch (Throwable t) {
              // Clear current throttling rate
              Store.rateLimit = null;
              Sensision.clear(SensisionConstants.CLASS_WARP_STORE_THROTTLING_RATE, Sensision.EMPTY_LABELS);
            } finally {
              if (null != br) {
                try { br.close(); } catch (IOException ioe) {}
              }
              if (null != Store.rateLimit) {
                Sensision.set(SensisionConstants.CLASS_WARP_STORE_THROTTLING_RATE, Sensision.EMPTY_LABELS, Store.rateLimit.getRate());
              }
            }

            LockSupport.parkNanos(period * 1000000L);
          }
        }
      };

      t.setName("[Store Throttling Reader]");
      t.setDaemon(true);
      t.start();
    }
  }

  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    Configuration.STORE_NTHREADS,
    Configuration.STORE_KAFKA_DATA_CONSUMER_BOOTSTRAP_SERVERS,
    Configuration.STORE_KAFKA_DATA_TOPIC,
    Configuration.STORE_KAFKA_DATA_GROUPID,
    Configuration.STORE_KAFKA_DATA_COMMITPERIOD,
    Configuration.STORE_KAFKA_DATA_INTERCOMMITS_MAXTIME,
  };

  /**
   * Keystore
   */
  private final KeyStore keystore;

  private final Properties properties;

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

  /**
   * Maximum number of milliseconds between offsets commits. This limit is there so we detect hang calls to htable.batch
   */
  private final long maxTimeBetweenCommits;

  /**
   * Maximum size we allow the pending mutations list to grow
   */
  private final long maxPendingMutationsSize;

  private final boolean SKIP_WRITE;

  private int generation = 0;

  private UUID uuid = UUID.randomUUID();

  //
  // FoundationDB
  //

  private final FDBContext fdbContext;

  private final long fdbRetryLimit;

  private final boolean FDBUseTenantPrefix;

  public Store(KeyStore keystore, final Properties properties, Integer nthr) throws IOException {
    this.keystore = keystore;
    this.properties = properties;

    if ("true".equals(properties.getProperty(Configuration.STORE_SKIP_WRITE))) {
      this.SKIP_WRITE = true;
    } else {
      this.SKIP_WRITE = false;
    }

    //
    // Check mandatory parameters
    //

    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(required), "Missing configuration parameter '%s'.", required);
    }

    //
    // Extract parameters
    //

    final String topic = properties.getProperty(Configuration.STORE_KAFKA_DATA_TOPIC);
    final int nthreads = null != nthr ? nthr.intValue() : Integer.valueOf(properties.getProperty(Configuration.STORE_NTHREADS_KAFKA, "1"));

    this.FDBUseTenantPrefix = "true".equals(properties.getProperty(Configuration.FDB_USE_TENANT_PREFIX));

    if (this.FDBUseTenantPrefix && null != properties.getProperty(Configuration.STORE_FDB_TENANT)) {
      throw new RuntimeException("Cannot set '" + Configuration.STORE_FDB_TENANT + "' when '" + Configuration.FDB_USE_TENANT_PREFIX + "' is true.");
    }

    this.fdbContext = new FDBContext(properties.getProperty(Configuration.STORE_FDB_CLUSTERFILE), properties.getProperty(Configuration.STORE_FDB_TENANT));
    this.fdbRetryLimit = Long.parseLong(properties.getProperty(Configuration.STORE_FDB_RETRYLIMIT, DEFAULT_FDB_RETRYLIMIT));

    //
    // Extract keys
    //

    extractKeys(properties);

    final Store self = this;

    commitPeriod = Long.parseLong(properties.getProperty(Configuration.STORE_KAFKA_DATA_COMMITPERIOD));
    maxTimeBetweenCommits = Long.parseLong(properties.getProperty(Configuration.STORE_KAFKA_DATA_INTERCOMMITS_MAXTIME));

    if (maxTimeBetweenCommits <= commitPeriod) {
      throw new RuntimeException(Configuration.STORE_KAFKA_DATA_INTERCOMMITS_MAXTIME + " MUST be set to a value above that of " + Configuration.STORE_KAFKA_DATA_COMMITPERIOD);
    }

    this.maxPendingMutationsSize = (long) Math.min(FDBUtils.MAX_TXN_SIZE * 0.95, Long.parseLong(properties.getProperty(Configuration.STORE_FDB_DATA_PENDINGMUTATIONS_MAXSIZE, Constants.DEFAULT_FDB_DATA_PENDINGMUTATIONS_MAXSIZE)));

    final String groupid = properties.getProperty(Configuration.STORE_KAFKA_DATA_GROUPID);

    final KafkaOffsetCounters counters = new KafkaOffsetCounters(topic, groupid, commitPeriod * 2);

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {

        ExecutorService executor = null;

        KafkaConsumer<byte[], byte[]>[] kconsumers = null;

        StoreConsumer[] consumers = new StoreConsumer[nthreads];
        Database[] databases = new Database[nthreads];

        for (int i = 0; i < nthreads; i++) {
          databases[i] = fdbContext.getDatabase();
        }

        while(true) {
          try {
            //
            // Enter an endless loop which will spawn 'nthreads' threads
            // each time the Kafka consumer is shut down (which will happen if an error
            // happens while talking to FDB, to get a chance to re-read data from the
            // previous snapshot).
            //

            Properties props = new Properties();

            // Load explicit configuration
            props.putAll(Configuration.extractPrefixed(properties, properties.getProperty(Configuration.STORE_KAFKA_DATA_CONSUMER_CONF_PREFIX)));

            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(Configuration.STORE_KAFKA_DATA_CONSUMER_BOOTSTRAP_SERVERS));
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
            if (null != properties.getProperty(Configuration.STORE_KAFKA_DATA_CONSUMER_CLIENTID)) {
              props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, properties.getProperty(Configuration.STORE_KAFKA_DATA_CONSUMER_CLIENTID));
            }
            if (null != properties.getProperty(Configuration.STORE_KAFKA_DATA_CONSUMERID_PREFIX)) {
              // If a consumerId prefix is provided, the consumerId is built the same way than inside the Kafka ConsumerConnector with the prefix part prepended to the hostname
              UUID uuid = UUID.randomUUID();
              String consumerUuid = String.format("%s_%s_%d-%s",
                      properties.getProperty(Configuration.STORE_KAFKA_DATA_CONSUMERID_PREFIX),
                      InetAddress.getLocalHost().getHostName(),
                      System.currentTimeMillis(),
                      Long.toHexString(uuid.getMostSignificantBits()).substring(0,8));
              props.setProperty("consumer.id", consumerUuid);
            }
            if (null != properties.getProperty(Configuration.STORE_KAFKA_DATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY)) {
              props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, properties.getProperty(Configuration.STORE_KAFKA_DATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY));
            }
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            //
            // This is VERY important, offset MUST be reset to 'earliest' so we get a chance to store as many datapoints
            // as we can when the lag gets beyond the history Kafka maintains.
            //

            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

            kconsumers = new KafkaConsumer[nthreads];

            self.barrier = new CyclicBarrier(nthreads + 1);

            executor = Executors.newFixedThreadPool(nthreads);

            //
            // now create runnables which will consume messages
            // We reset the counters when we re-allocate the consumers
            //

            counters.reset();

            int idx = 0;

            for (int i = 0; i < nthreads; i++) {
              if (null != consumers[idx] && consumers[idx].getShouldReset()) {
                try {
                  databases[idx].close();
                } catch (Throwable t) {
                  LOG.error("Caught throwable while closing FoundationDB database.", t);
                }
                try {
                  databases[idx] = fdbContext.getDatabase();
                } catch (Throwable t) {
                  LOG.error("Caught throwable while getting new FoundationDB database.", t);
                  // Force connection reset
                  throw new RuntimeException(t);
                }
              }
              kconsumers[i] = new KafkaConsumer<byte[],byte[]>(props);
              consumers[idx] = new StoreConsumer(databases[idx], self, kconsumers[i], counters, Collections.singletonList(topic));
              executor.submit(consumers[idx]);
              idx++;
            }

            long lastBarrierSync = System.currentTimeMillis();

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
                  // they have all processed data successfully for the given activity period and none is currently calling
                  // KafkaConsumer.poll
                  //

                  // Commit offsets

                  for (KafkaConsumer kconsumer: kconsumers) {
                    kconsumer.commitSync();
                  }

                  counters.sensisionPublish();
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_KAFKA_COMMITS, Sensision.EMPTY_LABELS, 1);

                  // Release the waiting threads
                  try {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("Releasing waiting threads.");
                    }
                    barrier.await();
                    lastBarrierSync = System.currentTimeMillis();
                  } catch (Exception e) {
                    break;
                  }
                } else if (System.currentTimeMillis() - lastBarrierSync > maxTimeBetweenCommits) {
                  //
                  // If the last barrier synchronization was more than 'maxTimeBetweenCommits' ms ago
                  // we abort, because it probably means the call to htable.batch has hang and
                  // we need to respawn the consuming threads.
                  //
                  // FIXME(hbs): We might have to fix this to be able to tolerate long deletes.
                  //
                  Sensision.update(SensisionConstants.CLASS_WARP_STORE_KAFKA_COMMITS_OVERDUE, Sensision.EMPTY_LABELS, 1);
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Last Kafka commit was more than " + maxTimeBetweenCommits + " ms ago, aborting.");
                  }
                  for (int i = 0; i < consumers.length; i++) {
                    consumers[i].setShouldReset(true);
                  }
                  abort.set(true);
                }
              } catch (Throwable t) {
                LOG.error("", t);
                abort.set(true);
              }

              LockSupport.parkNanos(1000000L);
            }

          } catch (Throwable t) {
            LOG.error("Caught Throwable while synchronizing", t);
          } finally {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Spawner reinitializing.");
            }

            //
            // We exited the loop, this means one of the threads triggered an abort,
            // we will shut down the executor and shut down the connector to start over.
            //

            if (null != kconsumers) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Closing Kafka consumers.");
              }
              for (KafkaConsumer kconsumer: kconsumers) {
                try {
                  if (null != kconsumer) {
                    kconsumer.close();
                  }
                } catch (Exception e) {
                  LOG.error("Error while closing Kafka consumer", e);
                }
              }
            }

            if (null != executor) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Closing executor.");
              }
              try {
                for (StoreConsumer consumer: consumers) {
                  consumer.localabort.set(true);
                  Thread sync = consumer.getSynchronizer();
                  while(null != sync && sync.isAlive()) {
                    sync.interrupt();
                    LockSupport.parkNanos(100000000L);
                  }
                  //
                  // We need to shut down the executor prior to waiting for the
                  // consumer threads to die, otherwise they will never die!
                  //
                  executor.shutdownNow();
                  while(!consumer.isDone()) {
                    Thread consumerThread = consumer.getConsumerThread();
                    if (null != consumerThread && consumerThread.isAlive()) {
                      consumer.getConsumerThread().interrupt();
                    } else {
                      // We consider the consumer done if it does not have a current thread or if its current thread is no longer alive
                      break;
                    }
                    LockSupport.parkNanos(100000000L);
                  }
                }
              } catch (Exception e) {
                LOG.error("Error while closing executor", e);
              }
            }

            //
            // Reset barrier
            //

            if (null != barrier) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Resetting barrier.");
              }
              barrier.reset();
            }

            if (LOG.isDebugEnabled()) {
              LOG.debug("Increasing generation.");
            }
            generation++;

            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_ABORTS, Sensision.EMPTY_LABELS, 1);

            abort.set(false);

            LockSupport.parkNanos(100000000L);
          }
        }
      }
    });

    t.setName("[Continuum Store Spawner " + this.uuid + "]");
    t.setDaemon(true);
    t.start();

    this.setName("[Continuum Store " + this.uuid + "]");
    this.setDaemon(true);
    this.start();
  }

  @Override
  public void run() { // Store#run
    while (true){
      LockSupport.parkNanos(Long.MAX_VALUE);
    }
  }

  private static class StoreConsumer implements Runnable {

    private boolean shouldReset = false;
    private boolean done = false;
    // Is the synchronizer done
    private final AtomicBoolean syncDone = new AtomicBoolean(false);
    private final Store store;
    private final KafkaConsumer<byte[],byte[]> consumer;
    private final Collection<String> topics;
    private final byte[] fdbAESKey;
    private final AtomicLong lastMutation = new AtomicLong(0L);
    private final List<FDBMutation> mutations;

    /**
     * Lock for protecting the access to the 'puts' list.
     * This lock is also used to mutex the synchronization and the processing of
     * a message from Kafka so we do not commit and offset for an inflight message.
     * On machines with a high number of cores, starvation has been observed when the
     * lock is not created 'fair', leading to chaotic Store behavior since the wait limit for
     * Kafka offset commit is reached thus triggering a reset.
     * Creating the lock with fairness to true solves this issue to the expense of slightly
     * lesser performance.
     */
    private final ReentrantLock mutationslock = new ReentrantLock(true);

    private final AtomicLong mutationsSize = new AtomicLong(0L);
    private final AtomicBoolean localabort = new AtomicBoolean(false);
    private final KafkaOffsetCounters counters;

    private Thread synchronizer = null;

    // Indicates there is a message currently being processed
    final AtomicBoolean inflightMessage = new AtomicBoolean(false);

    // Indicates that an offset commit should take place, if this is true then
    // retrieving a message will be delayed until after the offsets have been committed
    final AtomicBoolean needToSync = new AtomicBoolean(false);

    private final Database db;

    private Thread currentThread = null;

    public StoreConsumer(Database db, Store store, KafkaConsumer<byte[], byte[]> consumer, KafkaOffsetCounters counters, Collection<String> topics) {
      this.store = store;
      this.consumer = consumer;
      this.topics = topics;
      this.mutations = new ArrayList<FDBMutation>();
      this.counters = counters;
      this.fdbAESKey = store.keystore.getKey(KeyStore.AES_FDB_DATA);
      this.db = db;
    }

    private Thread getSynchronizer() {
      return this.synchronizer;
    }

    private Thread getConsumerThread() {
      return currentThread;
    }

    private boolean isDone() {
      // We are done if we are marked as done or if our synchronizer is done. This gives us more chances to detect we are done since
      // interruptions tend to make us miss our own end....
      return this.done || syncDone.get();
    }

    private boolean getShouldReset() {
      return this.shouldReset;
    }

    private void setShouldReset(boolean reset) {
      this.shouldReset = reset;
    }

    @Override
    public void run() {

      this.currentThread = Thread.currentThread();
      Thread.currentThread().setName("[Store Consumer - gen " + this.store.uuid + "/" + store.generation + "]");

      // We need to call subscribe here otherwise the thread associated with 'consumer' will be the one which created the consumer
      // and any call to poll or close will lead to an exception stating that the consumer is not MT safe
      this.consumer.subscribe(this.topics);

      long count = 0L;

      try {
        byte[] siphashKey = store.keystore.getKey(KeyStore.SIPHASH_KAFKA_DATA);
        byte[] aesKey = store.keystore.getKey(KeyStore.AES_KAFKA_DATA);

        //
        // AtomicLong with the timestamp of the last Put or 0 if
        // none were added since the last flush
        //

        final StoreConsumer self = this;

        //
        // Start the synchronization Thread. There is one such thread per Store instance
        //

        final CyclicBarrier ourbarrier = store.barrier;

        synchronizer = new Thread(new Runnable() {

          @Override
          public void run() {
            try {
              // Last time we synced the Kafka offsets
              long lastKafkaOffsetCommit = System.currentTimeMillis();

              //
              // Check for how long we've been storing readings, if we've reached the commitperiod,
              // flush any pending commits and synchronize with the other threads so offsets can be committed
              //

              boolean doPause = false;

              while(!localabort.get() && !synchronizer.isInterrupted()) {
                long now = System.currentTimeMillis();
                if (now - lastKafkaOffsetCommit > store.commitPeriod // We've committed too long ago
                    && (!inflightMessage.get() // There is no message currently being processed
                        || !needToSync.get()   // We do not need to sync offsets
                        || !((0L != lastMutation.get() // there is at least one mutation to persist
                               && (now - lastMutation.get() > 500L) // and it was processed more than 500ms ago
                               || mutationsSize.get() > store.maxPendingMutationsSize) // or the size of all mutations to persist is above a threshold
                            )
                        )
                    ) {
                  try {
                    mutationslock.lockInterruptibly();

                    //
                    // If a message processing is ongoing we need to delay synchronization
                    //

                    if (inflightMessage.get()) {
                      needToSync.set(true);
                      // Indicate we should pause after we've relinquished the lock
                      doPause = true;
                      continue;
                    }

                    needToSync.set(false);

                    //
                    // Attempt to flush to FoundationDB
                    //

                    try {
                      long nanos = System.nanoTime();
                      if (!store.SKIP_WRITE) {
                        if (LOG.isDebugEnabled()) {
                          LOG.debug("FoundationDB commit of " + mutations.size() + " mutations (" + mutationsSize.get() + " bytes).");
                        }
                        if (mutations.size() > 0) {
                          flushMutations();
                        }
                      }

                      mutations.clear();
                      mutationsSize.set(0L);
                      nanos = System.nanoTime() - nanos;
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("FoundationDB commit took " + nanos + " ns.");
                      }
                      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_COMMITS, Sensision.EMPTY_LABELS, 1);
                      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_TIME_NANOS, Sensision.EMPTY_LABELS, nanos);
                    } catch (Throwable t) {
                      mutations.clear();
                      mutationsSize.set(0L);
                      // If an exception is thrown, abort
                      store.abort.set(true);
                      LOG.error("Received Throwable while writing to FoundationDB - forcing reset", t);
                      return;
                    }

                    //
                    // Now join the cyclic barrier which will trigger the
                    // commit of offsets
                    //

                    try {
                      // We wait at most maxTimeBetweenCommits so we can abort in case the synchronization was too long ago
                      ourbarrier.await(store.maxTimeBetweenCommits, TimeUnit.MILLISECONDS);
                      needToSync.set(false);
                      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_BARRIER_SYNCS, Sensision.EMPTY_LABELS, 1);
                    } catch (Exception e) {
                      store.abort.set(true);
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("Received Exception while await barrier", e);
                      }
                      return;
                    } finally {
                      lastKafkaOffsetCommit = System.currentTimeMillis();
                    }
                  } catch (InterruptedException ie) {
                    store.abort.set(true);
                    return;
                  } finally {
                    if (mutationslock.isHeldByCurrentThread()) {
                      mutationslock.unlock();
                    }
                    if (doPause) {
                      doPause = false;
                      LockSupport.parkNanos(100000L);
                    }
                  }
                } else if ((0L != lastMutation.get() && (now - lastMutation.get() > 500L) || mutationsSize.get() > store.maxPendingMutationsSize)) {
                  //
                  // If the last mutation was added to 'puts' more than 500ms ago, force a flush
                  //

                  try {
                    mutationslock.lockInterruptibly();

                    if (!mutations.isEmpty()) {
                      try {
                        long nanos = System.nanoTime();
                        if (!store.SKIP_WRITE) {
                          if (LOG.isDebugEnabled()) {
                            LOG.debug("Forcing FoundationDB batch of " + mutations.size() + " mutations (" + mutationsSize.get() + " bytes).");
                          }
                          flushMutations();
                        }

                        mutations.clear();
                        mutationsSize.set(0L);
                        nanos = System.nanoTime() - nanos;
                        if (LOG.isDebugEnabled()) {
                          LOG.debug("Forced FoundationDB flush took " + nanos + " ns");
                        }
                        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_COMMITS, Sensision.EMPTY_LABELS, 1);
                        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_TIME_NANOS, Sensision.EMPTY_LABELS, nanos);
                        // Reset lastPut to 0
                        lastMutation.set(0L);
                      } catch (Throwable t) {
                        // Clear list of Puts
                        int nmutations = mutations.size();
                        long msize = mutationsSize.get();
                        mutations.clear();
                        mutationsSize.set(0L);
                        // If an exception is thrown, abort
                        store.abort.set(true);
                        LOG.error("Received Throwable while forced writing of " + nmutations + " mutations (" + msize + " bytes) to FoundationDB - forcing reset", t);
                        return;
                      }
                    }
                  } catch (InterruptedException ie) {
                    store.abort.set(true);
                    return;
                  } finally {
                    if (mutationslock.isHeldByCurrentThread()) {
                      mutationslock.unlock();
                    }
                  }
                }

                LockSupport.parkNanos(1000000L);
              }
            } finally {
              syncDone.set(true);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Synchronizer done.");
              }
              localabort.set(true);
              done = true;
            }
          }

          private void flushMutations() throws IOException {
            Transaction txn = null;

            boolean retry = false;
            long retries = store.fdbRetryLimit;

            do {
              try {
                retry = false;
                txn = db.createTransaction();
                // Allow RAW access because we may manually force a tenant key prefix without actually setting a tenant
                txn.options().setRawAccess();
                int sets = 0;
                int clearranges = 0;

                for (FDBMutation mutation: mutations) {
                  if (mutation instanceof FDBSet) {
                    sets++;
                  } else if (mutation instanceof FDBClearRange) {
                    clearranges++;
                  }
                  mutation.apply(txn);
                }

                txn.commit().get();
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_SETS_COMMITTED, Sensision.EMPTY_LABELS, sets);
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_CLEARRANGES_COMMITTED, Sensision.EMPTY_LABELS, clearranges);
              } catch (Throwable t) {
                FDBUtils.errorMetrics("store", t.getCause());
                if (t.getCause() instanceof FDBException && ((FDBException) t.getCause()).isRetryable() && retries-- > 0) {
                  retry = true;
                } else {
                  throw new IOException("Error while commiting to FoundationDB.", t);
                }
              } finally {
                if (null != txn) {
                  try { txn.close(); } catch (Throwable t) {}
                }
              }
            } while(retry);
          }
        }); // Synchronizer

        synchronizer.setName("[Continuum Store Synchronizer - gen " + this.store.uuid + "/" + store.generation + "]");
        synchronizer.setDaemon(true);
        synchronizer.start();

        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

        // The call to resetInflight is a hack, we need to reset inflightMessage BUT iter.hasNext() may block
        // so we add an artificial call to resetInflight which always returns true but has the side effect
        // of resetting inflightMessage, this makes the code cleaner as we don't have to add calls to inflightMessage.set(false)
        // throughout the code

        // Kafka 2.x
        Duration delay = Duration.of(500L, ChronoUnit.MILLIS);

        while (resetInflight() && !store.abort.get() && !Thread.currentThread().isInterrupted()) {

          ConsumerRecords<byte[], byte[]> consumerRecords = null;

          boolean doPause = false;

          try {
            mutationslock.lockInterruptibly();
            // Clear the 'inflight' status
            inflightMessage.set(false);

            // If we already have plenty of mutations to flush, wait until we sync
            if (mutationsSize.get() >= store.maxPendingMutationsSize) {
              doPause = true;
              continue;
            }

            // Do not call poll if we must commit the offsets
            if (needToSync.get()) {
              doPause = true;
              continue;
            }

            consumerRecords = consumer.poll(delay);
            inflightMessage.set(consumerRecords.count() > 0);
          } finally {
            if (mutationslock.isHeldByCurrentThread()) {
              mutationslock.unlock();
            }
            // Wait a little if we need to sync
            if (doPause) {
              LockSupport.parkNanos(10000L);
            }
          }

          boolean first = true;

          Iterator<ConsumerRecord<byte[],byte[]>> iter = consumerRecords.iterator();

          while(resetInflight() && iter.hasNext()) {

            ConsumerRecord<byte[],byte[]> record = null;

            //
            // Indicate we have a message currently being processed.
            // We change the value of the flag while holding mutationsLock so we know
            // we are not currently synchronizing.
            //

            try {
              mutationslock.lockInterruptibly();
              // Continue the loop if we need to synchronize
              if (needToSync.get()) {
                mutationslock.unlock();
                LockSupport.parkNanos(100000L);
                continue;
              }

              if (!first) {
                throw new RuntimeException("Invalid input, expected a single record, got " + consumerRecords.count());
              }

              first = false;

              record = iter.next();
              inflightMessage.set(true);
            } finally {
              if (mutationslock.isHeldByCurrentThread()) {
                mutationslock.unlock();
              }
            }

            //
            // Synchronize with the synchronizer so we know the committed offsets will only include the processed messages
            //

            count++;
            self.counters.count(record.partition(), record.offset());

            byte[] data = record.value();

            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_KAFKA_COUNT, Sensision.EMPTY_LABELS, 1);
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_KAFKA_BYTES, Sensision.EMPTY_LABELS, data.length);

            if (null != siphashKey) {
              data = CryptoUtils.removeMAC(siphashKey, data);
            }

            // Skip data whose MAC was not verified successfully
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_KAFKA_FAILEDMACS, Sensision.EMPTY_LABELS, 1);
              // TODO(hbs): increment Sensision metric
              continue;
            }

            // Unwrap data if need be
            if (null != aesKey) {
              data = CryptoUtils.unwrap(aesKey, data);
            }

            // Skip data that was not unwrapped successfuly
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_KAFKA_FAILEDDECRYPTS, Sensision.EMPTY_LABELS, 1);
              // TODO(hbs): increment Sensision metric
              continue;
            }

            //
            // Extract KafkaDataMessage
            //

            KafkaDataMessage tmsg = new KafkaDataMessage();
            deserializer.deserialize(tmsg, data);

            switch(tmsg.getType()) {
              case STORE:
                handleStore(tmsg);
                break;
              case DELETE:
                handleDelete(tmsg);
                break;
              default:
                throw new RuntimeException("Invalid message type.");
            }
          }
        }
      } catch (Throwable t) {
        LOG.error("Received Throwable while processing Kafka message.", t);
      } finally {
        //
        // Attempt to close the KafkaConsumer from within this thread as it can only be closed
        // from the thread which last acquired the semaphore, and since the thread could have
        // been interrupted before the semaphore was released it could then be impossible to
        // call close from another thread.
        try {
          this.consumer.close();
        } catch (Exception e) {
          LOG.error("Error closing consumer. The synchronizer thread might have a chance to do so.", e);
        }

        // Interrupt the synchronizer thread
        this.localabort.set(true);
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Consumer exiting, killing synchronizer.");
          }
          while(synchronizer.isAlive()) {
            synchronizer.interrupt();
            LockSupport.parkNanos(100000000L);
          }
        } catch (Exception e) {}
        // Set abort to true in case we exit the 'run' method
        store.abort.set(true);
        //if (null != table) {
        //  try { table.close(); } catch (IOException ioe) { LOG.error("Error closing table ", ioe); }
        //}
      }
    }

    private boolean resetInflight() {
      inflightMessage.set(false);
      return true;
    }

    private void handleStore(KafkaDataMessage msg) throws IOException {
      if (KafkaDataMessageType.STORE != msg.getType()) {
        return;
      }

      // Skip if there are no data to decode
      if (null == msg.getData() || 0 == msg.getData().length) {
        return;
      }

      //
      // Create a GTSDecoder with the given readings (@see Ingress.java for the packed format)
      //

      GTSDecoder decoder = new GTSDecoder(0L, ByteBuffer.wrap(msg.getData()));

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_GTSDECODERS,  Sensision.EMPTY_LABELS, 1);

      byte[] tenantPrefix = store.fdbContext.getTenantPrefix();

      if (store.FDBUseTenantPrefix) {
        if (msg.getAttributesSize() > 0 && msg.getAttributes().containsKey(Constants.STORE_ATTR_FDB_TENANT_PREFIX)) {
          tenantPrefix = OrderPreservingBase64.decode(msg.getAttributes().get(Constants.STORE_ATTR_FDB_TENANT_PREFIX));
        } else {
          LOG.error("Incoherent configuration, Store mandates a tenant but the current Kafka STORE message did not have a tenant prefix set, aborting.");
          throw new RuntimeException("Incoherent configuration, Store mandates a tenant but the current Kafka STORE message did not have a tenant prefix set, aborting.");
        }
      } else {
        if (msg.getAttributesSize() > 0 && msg.getAttributes().containsKey(Constants.STORE_ATTR_FDB_TENANT_PREFIX)) {
          LOG.error("Incoherent configuration, Store has no tenant support but the current Kafka STORE message has a tenant prefix set, aborting.");
          throw new RuntimeException("Incoherent configuration, Store has no tenant support and the current Kafka STORE message has a tenant prefix set.");
        }
      }

      long datapoints = 0L;

      // We will store each reading separately, this makes readings storage idempotent
      // If BLOCK_ENCODING is enabled, prefix encoding will be used to shrink column qualifiers

      while(decoder.next()) {
        // FIXME(hbs): allow for encrypting readings
        long basets = decoder.getTimestamp();
        GTSEncoder encoder = new GTSEncoder(basets, fdbAESKey);
        encoder.addValue(basets, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());

        // Prefix + classId + labelsId + timestamp
        // 128 bits
        byte[] rowkey = new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];

        System.arraycopy(Constants.FDB_RAW_DATA_KEY_PREFIX, 0, rowkey, 0, Constants.FDB_RAW_DATA_KEY_PREFIX.length);
        // Copy classId/labelsId
        System.arraycopy(Longs.toByteArray(msg.getClassId()), 0, rowkey, Constants.FDB_RAW_DATA_KEY_PREFIX.length, 8);
        System.arraycopy(Longs.toByteArray(msg.getLabelsId()), 0, rowkey, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8, 8);
        // Copy timestamp % DEFAULT_MODULUS
        // It could be useful to have per GTS modulus BUT we don't do lookups for metadata, so we can't access a per GTS
        // modulus.... This means we will use the default.

        FDBSet set = null;

        byte[] bytes = encoder.getBytes();

        if (1 == Constants.DEFAULT_MODULUS) {
          System.arraycopy(Longs.toByteArray(Long.MAX_VALUE - basets), 0, rowkey, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 16, 8);
          set = new FDBSet(tenantPrefix, rowkey, bytes);
        } else {
          throw new RuntimeException("DEFAULT_MODULUS should have been 1!!!!!");
        }

        if (null != Store.rateLimit) {
          while(!Store.rateLimit.tryAcquire(1, TimeUnit.SECONDS)) {}
        }

        //
        // If throttling is defined, check if we should consume this message
        //

        try {
          mutationslock.lockInterruptibly();
          mutations.add(set);
          datapoints++;
          mutationsSize.addAndGet(set.size());
          lastMutation.set(System.currentTimeMillis());
        } catch (InterruptedException ie) {
          localabort.set(true);
          return;
        } finally {
          if (mutationslock.isHeldByCurrentThread()) {
            mutationslock.unlock();
          }
        }
      }
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_SETS, Sensision.EMPTY_LABELS, datapoints);
    }

    private void handleDelete(final KafkaDataMessage msg) throws Throwable {

      if (KafkaDataMessageType.DELETE != msg.getType()) {
        return;
      }

      if (1 != Constants.DEFAULT_MODULUS) {
        throw new RuntimeException("DEFAULT_MODULUS should have been 1!!!!!");
      }

      // Prefix + classId + labelsId + timestamp
      byte[] rowkey = new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];

      System.arraycopy(Constants.FDB_RAW_DATA_KEY_PREFIX, 0, rowkey, 0, Constants.FDB_RAW_DATA_KEY_PREFIX.length);
      // Copy classId/labelsId
      System.arraycopy(Longs.toByteArray(msg.getClassId()), 0, rowkey, Constants.FDB_RAW_DATA_KEY_PREFIX.length, 8);
      System.arraycopy(Longs.toByteArray(msg.getLabelsId()), 0, rowkey, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8, 8);

      long start = msg.getDeletionStartTimestamp();
      long end = msg.getDeletionEndTimestamp();

      byte[] startkey = Arrays.copyOf(rowkey, rowkey.length);
      // Endkey is one extra byte long so we include the most ancient ts in the deletion
      byte[] endkey = Arrays.copyOf(rowkey, rowkey.length + 1);

      if (Long.MAX_VALUE == end && Long.MIN_VALUE == start) {
        // Only set the end key.
        Arrays.fill(endkey, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8, endkey.length - 1, (byte) 0xff);
      } else {
        // Add reversed timestamps. The end timestamps is the start key
        System.arraycopy(Longs.toByteArray(Long.MAX_VALUE - end), 0, startkey, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8, 8);
        System.arraycopy(Longs.toByteArray(Long.MAX_VALUE - start), 0, endkey, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8, 8);
      }

      byte[] tenantPrefix = store.fdbContext.getTenantPrefix();

      if (store.FDBUseTenantPrefix) {
        if (msg.getAttributesSize() > 0 && msg.getAttributes().containsKey(Constants.STORE_ATTR_FDB_TENANT_PREFIX)) {
          tenantPrefix = OrderPreservingBase64.decode(msg.getAttributes().get(Constants.STORE_ATTR_FDB_TENANT_PREFIX));
        } else {
          LOG.error("Incoherent configuration, Store mandates a tenant but the current Kafka DELETE message did not have a tenant prefix set, aborting.");
          throw new RuntimeException("Incoherent configuration, Store mandates a tenant but the current Kafka DELETE message did not have a tenant prefix set, aborting.");
        }
      } else {
        if (msg.getAttributesSize() > 0 && msg.getAttributes().containsKey(Constants.STORE_ATTR_FDB_TENANT_PREFIX)) {
          LOG.error("Incoherent configuration, Store has no tenant support but the current Kafka DELETE message has a tenant prefix set, aborting.");
          throw new RuntimeException("Incoherent configuration, Store has no tenant support and the current Kafka DELETE message has a tenant prefix set.");
        }
      }

      FDBClearRange clearRange = new FDBClearRange(tenantPrefix, startkey, endkey);

      if (null != Store.rateLimit) {
        while(!Store.rateLimit.tryAcquire(1, TimeUnit.SECONDS)) {}
      }

      try {
        mutationslock.lockInterruptibly();
        mutations.add(clearRange);
        mutationsSize.addAndGet(clearRange.size());
        lastMutation.set(System.currentTimeMillis());
      } catch (InterruptedException ie) {
        localabort.set(true);
        return;
      } finally {
        if (mutationslock.isHeldByCurrentThread()) {
          mutationslock.unlock();
        }
      }

      //
      // Update Sensision metrics for deletion
      //

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_DELETE_OPS, Sensision.EMPTY_LABELS, 1);
    }
  }


  /**
   * Extract Store related keys and populate the KeyStore with them.
   *
   * @param props Properties from which to extract the key specs
   */
  private void extractKeys(Properties props) {
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_KAFKA_DATA, props, Configuration.STORE_KAFKA_DATA_MAC, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_KAFKA_DATA, props, Configuration.STORE_KAFKA_DATA_AES, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_FDB_DATA, props, Configuration.STORE_FDB_DATA_AES, 128, 192, 256);

    this.keystore.forget();
  }
}
