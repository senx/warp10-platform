//
//   Copyright 2019  SenX S.A.S.
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

package io.warp10.script;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geoxp.oss.CryptoHelper;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.leader.LeaderLatch;
import com.netflix.curator.retry.RetryNTimes;

import io.warp10.WarpDist;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.KafkaProducerPool;
import io.warp10.continuum.KafkaSynchronizedConsumerPool;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.thrift.data.RunRequest;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.DummyKeyStore;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.sensision.Sensision;

/**
 * Periodically submit WarpScript scripts residing in subdirectories of the given root.
 * Greatly inspired by Sensision's own ScriptRunner
 */
public class ScriptRunner extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(ScriptRunner.class);
  
  static class NamedThreadFactory implements ThreadFactory {
    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(1);

    NamedThreadFactory() {
      SecurityManager s = System.getSecurityManager();
      if (null == s) {
        group = Thread.currentThread().getThreadGroup();
      } else {
        group = s.getThreadGroup();
      }
    }

    public Thread newThread(Runnable r) {
      Thread t = new Thread(group, r, "[Warp ScriptRunner Thread #" + threadNumber.getAndIncrement() + "]", 0);

      if (t.isDaemon()) {
        t.setDaemon(false);
      }

      if (Thread.NORM_PRIORITY != t.getPriority()) {
        t.setPriority(Thread.NORM_PRIORITY);
      }

      return t;
    }
  }

  /*
   * CLEAR footer, used to remove all symbols and clear the stack so no output is returned
   * even if EXPORT was called
   */
  protected static final byte[] CLEAR = ("\n" + WarpScriptLib.CLEARSYMBOLS + " " + WarpScriptLib.CLEAR + "\n").getBytes(Charsets.UTF_8);

  protected ExecutorService executor;

  protected long scanperiod;

  private String root;

  protected String endpoint;

  private long minperiod;

  private LeaderLatch leaderLatch;

  private KafkaProducerPool kafkaProducerPool;

  private String topic = null;

  private final String id;

  protected byte[] KAFKA_AES = null;

  protected long[] KAFKA_MAC = null;

  private KafkaSynchronizedConsumerPool consumerPool = null;

  //## Add LeaderLatch configuration
  //## Add Kafka configuration for outgoing RunRequests + MAC + AES

  /**
   * Required properties for the standalone version of ScriptRunner
   */
  public static final String[] REQUIRED_PROPERTIES_STANDALONE = {
      Configuration.RUNNER_ENDPOINT,
      Configuration.RUNNER_NTHREADS,
      Configuration.RUNNER_ROOT,
      Configuration.RUNNER_SCANPERIOD,
      Configuration.RUNNER_MINPERIOD,
      Configuration.RUNNER_ID,
  };

  /**
   * Required properties for the distributed 'worker' role of ScriptRunner
   */
  public static final String[] REQUIRED_PROPERTIES_WORKER = {
      Configuration.RUNNER_KAFKA_ZKCONNECT,
      Configuration.RUNNER_KAFKA_TOPIC,
      Configuration.RUNNER_KAFKA_GROUPID,
      Configuration.RUNNER_KAFKA_COMMITPERIOD,
      Configuration.RUNNER_KAFKA_NTHREADS,
      Configuration.RUNNER_NTHREADS,
      Configuration.RUNNER_ENDPOINT,
      Configuration.RUNNER_ID,
  };

  /**
   * Required properties for the distributed 'scheduler' role of ScriptRunner
   */
  public static final String[] REQUIRED_PROPERTIES_SCHEDULER = {
      Configuration.RUNNER_KAFKA_BROKERLIST,
      Configuration.RUNNER_KAFKA_TOPIC,
      Configuration.RUNNER_KAFKA_POOLSIZE,
      Configuration.RUNNER_ROOT,
      Configuration.RUNNER_SCANPERIOD,
      Configuration.RUNNER_MINPERIOD,
      Configuration.RUNNER_ID,
      Configuration.RUNNER_ZK_QUORUM,
      Configuration.RUNNER_ZK_ZNODE,
  };

  private final boolean isScheduler;
  private final boolean isStandalone;
  private final boolean isWorker;
  private final KeyStore keystore;

  private final byte[] runnerPSK;

  private final boolean runAtStartup;

  public ScriptRunner(KeyStore keystore, Properties config) throws IOException {
    //
    // Extract our roles
    //

    Preconditions.checkNotNull(config.getProperty(Configuration.RUNNER_ROLES), "Property '" + Configuration.RUNNER_ROLES + "' MUST be set.");

    String[] roles = config.getProperty(Configuration.RUNNER_ROLES).split(",");

    if (roles.length > 2) {
      throw new IOException("Role can only be 'standalone' or either or both 'scheduler' and 'worker'.");
    }

    Set<String> configuredRoles = new HashSet<String>();
    configuredRoles.addAll(Arrays.asList(roles));

    isStandalone = configuredRoles.contains("standalone");

    isScheduler = configuredRoles.contains("scheduler");

    isWorker = configuredRoles.contains("worker");

    if (isStandalone || isScheduler) {
      this.runAtStartup = "true".equals(config.getProperty(Configuration.RUNNER_RUNATSTARTUP, "true"));
    } else {
      this.runAtStartup = true;
    }

    if (isStandalone && (isWorker || isScheduler)) {
      throw new IOException("Role is either 'standalone' or either or both 'scheduler' and 'worker'.");
    }

    this.keystore = keystore;

    this.runnerPSK = keystore.getKey(KeyStore.AES_RUNNER_PSK);

    //
    // Check the required properties and configure the various roles
    //

    if (isStandalone) {
      for (String required: REQUIRED_PROPERTIES_STANDALONE) {
        Preconditions.checkNotNull(config.getProperty(required), "Missing configuration parameter '%s'.", required);
      }

      this.root = config.getProperty(Configuration.RUNNER_ROOT);
      int nthreads = Integer.parseInt(config.getProperty(Configuration.RUNNER_NTHREADS));
      this.scanperiod = Long.parseLong(config.getProperty(Configuration.RUNNER_SCANPERIOD));
      this.minperiod = Long.parseLong(config.getProperty(Configuration.RUNNER_MINPERIOD));
      this.endpoint = config.getProperty(Configuration.RUNNER_ENDPOINT);

      ThreadPoolExecutor runnersExecutor = new ThreadPoolExecutor(nthreads, nthreads, 30000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nthreads * 256), new NamedThreadFactory());
      runnersExecutor.allowCoreThreadTimeOut(true);

      this.executor = runnersExecutor;

      this.leaderLatch = null;

      this.setDaemon(true);
      this.setName("[Warp ScriptRunner]");
      this.start();
    }

    if (isWorker || isScheduler) {
      extractKeys(config);

      byte[] k = this.keystore.getKey(KeyStore.SIPHASH_KAFKA_RUNNER);

      if (null != k) {
        this.KAFKA_MAC = SipHashInline.getKey(k);
      }
    }

    if (isWorker) {
      for (String required: REQUIRED_PROPERTIES_WORKER) {
        Preconditions.checkNotNull(config.getProperty(required), "Missing configuration parameter '%s'.", required);
      }

      this.endpoint = config.getProperty(Configuration.RUNNER_ENDPOINT);

      String zkconnect = config.getProperty(Configuration.RUNNER_KAFKA_ZKCONNECT);
      Properties initialConfig = Configuration.extractPrefixed(config, config.getProperty(Configuration.RUNNER_KAFKA_CONSUMER_CONF_PREFIX));
      this.topic = config.getProperty(Configuration.RUNNER_KAFKA_TOPIC);
      String groupid = config.getProperty(Configuration.RUNNER_KAFKA_GROUPID);
      String clientid = config.getProperty(Configuration.RUNNER_KAFKA_CONSUMER_CLIENTID);
      String strategy = config.getProperty(Configuration.RUNNER_KAFKA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY);
      int nthreads = Integer.parseInt(config.getProperty(Configuration.RUNNER_KAFKA_NTHREADS));
      long commitPeriod = Long.parseLong(config.getProperty(Configuration.RUNNER_KAFKA_COMMITPERIOD));

      ThreadPoolExecutor runnersExecutor = new ThreadPoolExecutor(nthreads, nthreads, 30000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nthreads * 256), new NamedThreadFactory());
      runnersExecutor.allowCoreThreadTimeOut(true);

      this.executor = runnersExecutor;

      this.consumerPool = new KafkaSynchronizedConsumerPool(initialConfig, zkconnect, topic, clientid, groupid, strategy, nthreads, commitPeriod, new ScriptRunnerConsumerFactory(this));
    }

    if (isScheduler) {
      for (String required: REQUIRED_PROPERTIES_SCHEDULER) {
        Preconditions.checkNotNull(config.getProperty(required), "Missing configuration parameter '%s'.", required);
      }

      this.root = config.getProperty(Configuration.RUNNER_ROOT);
      this.scanperiod = Long.parseLong(config.getProperty(Configuration.RUNNER_SCANPERIOD));
      this.minperiod = Long.parseLong(config.getProperty(Configuration.RUNNER_MINPERIOD));
      this.topic = config.getProperty(Configuration.RUNNER_KAFKA_TOPIC);

      Properties props = new Properties();
      
      props.putAll(Configuration.extractPrefixed(config, config.getProperty(Configuration.RUNNER_KAFKA_PRODUCER_CONF_PREFIX)));

      // @see http://kafka.apache.org/documentation.html#producerconfigs
      props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty(Configuration.RUNNER_KAFKA_BROKERLIST));
      if (null != config.getProperty(Configuration.RUNNER_KAFKA_PRODUCER_CLIENTID)) {
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, config.getProperty(Configuration.RUNNER_KAFKA_PRODUCER_CLIENTID));
      }
      props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
      props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
      props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
      // Only a single in flight request to remove risk of message reordering during retries
      props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

      this.kafkaProducerPool = new KafkaProducerPool(props,
          Integer.parseInt(config.getProperty(Configuration.RUNNER_KAFKA_POOLSIZE)),
          SensisionConstants.SENSISION_CLASS_CONTINUUM_RUNNER_KAFKA_PRODUCER_POOL_GET,
          SensisionConstants.SENSISION_CLASS_CONTINUUM_RUNNER_KAFKA_PRODUCER_WAIT_NANOS);

      //
      // Create LeaderLatch
      //

      CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
          .connectionTimeoutMs(1000)
          .retryPolicy(new RetryNTimes(10, 500))
          .connectString(config.getProperty(Configuration.RUNNER_ZK_QUORUM))
          .build();
      curatorFramework.start();

      this.leaderLatch = new LeaderLatch(curatorFramework, config.getProperty(Configuration.RUNNER_ZK_ZNODE));

      try {
        this.leaderLatch.start();
      } catch (Exception e) {
        LOG.error("Error starting leader latch", e);
        throw new IOException(e);
      }
      
      this.setDaemon(true);
      this.setName("[Warp ScriptRunner]");
      this.start();
    }

    this.id = config.getProperty(Configuration.RUNNER_ID);
  }

  @Override
  public void run() {

    long lastscan = System.currentTimeMillis() - 2 * scanperiod;

    //
    // Periodicity of scripts
    //

    final Map<String, Long> scripts = new HashMap<String, Long>();

    //
    // Map of script path to next scheduled run
    //

    final Map<String, Long> nextrun = new ConcurrentHashMap<String, Long>();
    
    PriorityQueue<String> runnables = new PriorityQueue<String>(1, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        long nextrun1 = null != nextrun.get(o1) ? nextrun.get(o1) : Long.MAX_VALUE;
        long nextrun2 = null != nextrun.get(o2) ? nextrun.get(o2) : Long.MAX_VALUE;

        return Long.compare(nextrun1, nextrun2);
      }
    });

    // Wait until we are initialized so the local endpoint is up before we may attempt to use it
    while(!WarpDist.isInitialized()) {
      LockSupport.parkNanos(100000000L);
    }
    
    while (true) {
      long now = System.currentTimeMillis();

      if (now - lastscan > this.scanperiod) {
        Map<String, Long> newscripts = scanSuperRoot(this.root);

        Set<String> currentScripts = scripts.keySet();
        scripts.clear();
        scripts.putAll(newscripts);

        //
        // Clear entries from 'nextrun' which are for scripts which no longer exist
        //

        for (String prevscript: currentScripts) {
          if (!scripts.containsKey(prevscript)) {
            nextrun.remove(prevscript);
          }
        }

        lastscan = now;
      }

      //
      // Build a queue of runnable scripts
      //

      runnables.clear();

      for (String script: scripts.keySet()) {
        //
        // If script has no scheduled run yet or should run immediately, select it
        //

        Long schedule = nextrun.get(script);

        if (null == schedule) {
          if (runAtStartup) {
            runnables.add(script);
          } else {
            long period = scripts.get(script);
            long schedat = System.currentTimeMillis();

            if (0 != schedat % period) {
              schedat = schedat - (schedat % period) + period;
            }

            nextrun.put(script, schedat);
          }
        } else if (-1L != schedule && schedule <= now) {
          // Do not schedule scripts with a schedule set to -1
          runnables.add(script);
        }
      }

      boolean isLeader = isScheduler && leaderLatch.hasLeadership();

      while (runnables.size() > 0) {
        final String script = runnables.poll();
        // Set nextrun to -1 so we do not reschedule a script being scheduled
        nextrun.put(script, -1L);
        if (isStandalone) {
          schedule(nextrun, script, scripts.get(script));
        } else if (isLeader) {
          distributedSchedule(nextrun, script, scripts.get(script));
        }
      }

      LockSupport.parkNanos(50000000L);
    }
  }

  protected void schedule(final Map<String, Long> nextrun, final String script, final long periodicity) {

    if (!isStandalone) {
      return;
    }

    final ScriptRunner self = this;

    try {
      
      final long scheduledat = System.currentTimeMillis();
      
      this.executor.submit(new Runnable() {
        @Override
        public void run() {
          long nowts = System.currentTimeMillis();
          
          Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_CURRENT, Sensision.EMPTY_LABELS, 1);

          File f = new File(script);

          String path = new File(script).getAbsolutePath().substring(new File(self.root).getAbsolutePath().length() + 1);

          Map<String, String> labels = new HashMap<String, String>();
          labels.put(SensisionConstants.SENSISION_LABEL_PATH, path);

          Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_COUNT, labels, 1);

          long nano = System.nanoTime();

          HttpURLConnection conn = null;

          long ttl = Math.max(scanperiod * 2, periodicity * 2);

          InputStream in = null;
          
          try {
            in = new FileInputStream(f);

            conn = (HttpURLConnection) new URL(self.endpoint).openConnection();

            conn.setDoOutput(true);
            conn.setChunkedStreamingMode(8192);
            conn.setDoInput(true);
            conn.setRequestMethod("POST");

            conn.connect();

            OutputStream out = conn.getOutputStream();

            byte[] buf = new byte[1024];

            //
            // Push the script parameters
            //

            out.write(Long.toString(periodicity).getBytes(Charsets.UTF_8));
            out.write(' ');
            out.write('\'');
            out.write(URLEncoder.encode(Constants.RUNNER_PERIODICITY, "UTF-8").replaceAll("\\+", "%20").getBytes(Charsets.US_ASCII));
            out.write('\'');
            out.write(' ');
            out.write(WarpScriptLib.STORE.getBytes(Charsets.UTF_8));
            out.write('\n');

            out.write('\'');
            out.write(URLEncoder.encode(path, "UTF-8").replaceAll("\\+", "%20").getBytes(Charsets.US_ASCII));
            out.write('\'');
            out.write(' ');
            out.write('\'');
            out.write(URLEncoder.encode(Constants.RUNNER_PATH, "UTF-8").replaceAll("\\+", "%20").getBytes(Charsets.US_ASCII));
            out.write('\'');
            out.write(' ');
            out.write(WarpScriptLib.STORE.getBytes(Charsets.UTF_8));
            out.write('\n');

            out.write(Long.toString(scheduledat).getBytes(Charsets.UTF_8));
            out.write(' ');
            out.write('\'');
            out.write(URLEncoder.encode(Constants.RUNNER_SCHEDULEDAT, "UTF-8").replaceAll("\\+", "%20").getBytes(Charsets.US_ASCII));
            out.write('\'');
            out.write(' ');
            out.write(WarpScriptLib.STORE.getBytes(Charsets.UTF_8));
            out.write('\n');

            //
            // Generate a nonce by wrapping the current time with random 64bits
            //

            if (null != runnerPSK) {
              byte[] now = Longs.toByteArray(TimeSource.getTime());

              byte[] nonce = CryptoHelper.wrapBlob(runnerPSK, now);

              out.write('\'');
              out.write(OrderPreservingBase64.encode(nonce));
              out.write('\'');
              out.write(' ');
              out.write('\'');
              out.write(URLEncoder.encode(Constants.RUNNER_NONCE, "UTF-8").replaceAll("\\+", "%20").getBytes(Charsets.US_ASCII));
              out.write('\'');
              out.write(' ');
              out.write(WarpScriptLib.STORE.getBytes(Charsets.UTF_8));
              out.write('\n');
            }

            while (true) {
              int len = in.read(buf);

              if (len < 0) {
                break;
              }

              out.write(buf, 0, len);
            }

            // Add a 'CLEAR' at the end of the script so we don't return anything
            out.write(CLEAR);

            out.close();

            if (200 != conn.getResponseCode()) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_FAILURES, labels, ttl, 1);
            }
          } catch (Exception e) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_FAILURES, labels, ttl, 1);
          } finally {
            nextrun.put(script, nowts + periodicity);
            nano = System.nanoTime() - nano;
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_TIME_US, labels, ttl, nano / 1000L);
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_CURRENT, Sensision.EMPTY_LABELS, -1);
            if (null != conn) {
              try {
                conn.disconnect();
              } catch (Exception e) {                
              }
            }
            if (null != in) {
              try {
                in.close();
              } catch (Exception e) {                
              }
            }
          }
        }
      });
    } catch (RejectedExecutionException ree) {
      // Reschedule script immediately
      nextrun.put(script, System.currentTimeMillis());
    }
  }

  private void distributedSchedule(Map<String, Long> nextrun, final String script, final long periodicity) {

    RunRequest request = new RunRequest();

    String path = new File(script).getAbsolutePath().substring(new File(this.root).getAbsolutePath().length() + 1);

    long now = System.currentTimeMillis();
    request.setScheduledAt(now);
    request.setPeriodicity(periodicity);
    request.setPath(path);
    request.setCompressed(true);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    InputStream in = null;

    try {
      OutputStream out = new GZIPOutputStream(baos);

      in = new FileInputStream(script);

      byte[] buf = new byte[8192];

      while (true) {
        int len = in.read(buf);

        if (len <= 0) {
          break;
        }

        out.write(buf, 0, len);
        out.close();
      }
    } catch (IOException ioe) {
      // Reschedule immediately
      nextrun.put(script, System.currentTimeMillis());
      return;
    } finally {
      if (null != in) {
        try {
          in.close();
        } catch (IOException ioe) {
        }
      }
    }

    request.setContent(baos.toByteArray());

    request.setScheduler(this.id);

    byte[] content = null;

    try {
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      content = serializer.serialize(request);
    } catch (TException te) {
      // Reschedule immediately
      nextrun.put(script, System.currentTimeMillis());
      return;
    }

    //
    // Wrap content
    //

    if (null != this.KAFKA_AES) {
      content = CryptoUtils.wrap(this.KAFKA_AES, content);
    }

    //
    // Add integrity check
    //

    if (null != this.KAFKA_MAC) {
      content = CryptoUtils.addMAC(this.KAFKA_MAC, content);
    }

    KafkaProducer<byte[], byte[]> producer = null;

    // Use the script path as the scheduling key so the same script ends up in the same partition
    byte[] key = path.getBytes(Charsets.UTF_8);
    ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>(this.topic, key, content);

    try {
      producer = this.kafkaProducerPool.getProducer();
      // Call get() so we simulate a synchronous producer
      producer.send(record).get();
    } catch (Exception e) {
      // Reschedule immediately
      nextrun.put(script, System.currentTimeMillis());
      return;
    } finally {
      if (null != producer) {
        this.kafkaProducerPool.recycleProducer(producer);
      }
    }
    
    nextrun.put(script, now + periodicity);
  }

  private Map<String, Long> scanSuperRoot(String superroot) {

    Map<String, Long> scripts = new TreeMap<String, Long>();

    try (DirectoryStream<Path> roots = Files.newDirectoryStream(new File(superroot).toPath())) {
      for (Path root: roots) {
        scripts.putAll(scanRoot(root.toAbsolutePath().toString()));
      }
    } catch (IOException ioe) {
    }

    return scripts;
  }

  public String getRoot() {
    return this.root;
  }

  /**
   * Scan a directory and return a map keyed by
   * script path and whose values are run periodicities in ms.
   */

  private Map<String, Long> scanRoot(String root) {

    Map<String, Long> scripts = new TreeMap<String, Long>();

    //
    // Retrieve directory content
    //

    File dir = new File(root);

    if (!dir.exists()) {
      return scripts;
    }

    try (DirectoryStream<Path> pathes = Files.newDirectoryStream(dir.toPath())) {

      for (Path path: pathes) {

        File f = path.toFile();

        //
        // If child is a directory whose name is a valid
        // number of ms, scan its content.
        //

        if (!f.isDirectory() || !f.getParentFile().equals(dir)) {
          continue;
        }

        long period;

        try {
          period = Long.valueOf(f.getName());

          // Ignore periods below the minimum
          if (period < this.minperiod) {
            continue;
          }
        } catch (NumberFormatException nfe) {
          continue;
        }

        try (DirectoryStream<Path> subpathes = Files.newDirectoryStream(f.toPath(), "*.mc2")) {
          for (Path subpath: subpathes) {
            File script = subpath.toFile();
            scripts.put(script.getAbsolutePath(), period);
          }
        } catch (IOException ioe) {
        }
      }

    } catch (IOException ioe) {
    }


    return scripts;
  }

  public static void main(String[] args) throws Exception {
    ScriptRunner runner = new ScriptRunner(new DummyKeyStore(), System.getProperties());
    runner.start();

    while (true) {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException ie) {
      }
    }
  }

  private void extractKeys(Properties props) {
    String keyspec = props.getProperty(Configuration.RUNNER_KAFKA_AES);

    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.RUNNER_KAFKA_AES + " MUST be 128, 192 or 256 bits long.");
      this.keystore.setKey(KeyStore.AES_KAFKA_RUNNER, key);
    }

    keyspec = props.getProperty(Configuration.RUNNER_KAFKA_MAC);

    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.RUNNER_KAFKA_MAC + " MUST be 128 bits long.");
      this.keystore.setKey(KeyStore.SIPHASH_KAFKA_RUNNER, key);
    }

    this.keystore.forget();
  }
}
