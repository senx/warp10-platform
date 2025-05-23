//
//   Copyright 2018-2025  SenX S.A.S.
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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.warp10.CustomThreadFactory;
import io.warp10.ThriftUtils;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geoxp.oss.CryptoHelper;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.RetryNTimes;

import io.warp10.WarpConfig;
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
import io.warp10.script.functions.RUNNERFORCE;
import io.warp10.script.functions.RUNNERS;
import io.warp10.sensision.Sensision;

/**
 * Periodically submit WarpScript scripts residing in subdirectories of the given root.
 * Greatly inspired by Sensision's own ScriptRunner
 */
public class ScriptRunner extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(ScriptRunner.class);

  private static ScriptRunner INSTANCE = null;

  /*
   * CLEAR footer, used to remove all symbols and clear the stack so no output is returned
   * even if EXPORT was called
   */
  protected static final byte[] CLEAR = ("\n" + WarpScriptLib.CLEARSYMBOLS + " " + WarpScriptLib.CLEAR + "\n").getBytes(StandardCharsets.UTF_8);

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
      Configuration.RUNNER_KAFKA_CONSUMER_BOOTSTRAP_SERVERS,
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
      Configuration.RUNNER_KAFKA_PRODUCER_BOOTSTRAP_SERVERS,
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

  private static final Pattern VAR = Pattern.compile("\\$\\{([^}]+)\\}");

  //
  // Map of script path to next scheduled run
  //

  protected final Map<String, Long> nextrun = new ConcurrentHashMap<String, Long>();
  // Time (in ms since the epoch) of last run
  protected final Map<String, Long> lastrun = new ConcurrentHashMap<String, Long>();
  // Duration (in ms) of the last run
  protected final Map<String, Long> lastduration = new ConcurrentHashMap<String, Long>();
  // Status of last execution, either null or the error message.
  protected final Map<String, String> lasterror = new ConcurrentHashMap<String, String>();

  private static final String KEY_LASTRUN = "lastrun";
  private static final String KEY_LASTERROR = "lasterror";
  private static final String KEY_LASTDURATION = "lastduration";
  private static final String KEY_NEXTRUN = "nextrun";
  private static final String KEY_RUNNING = "running";

  static {
    //
    // Register functions which require the existence of ScriptRunner
    //

    WarpScriptLib.addNamedWarpScriptFunction(new RUNNERFORCE(WarpScriptLib.RUNNERFORCE));
    WarpScriptLib.addNamedWarpScriptFunction(new RUNNERS(WarpScriptLib.RUNNERS));

  }
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

      ThreadPoolExecutor runnersExecutor = new ThreadPoolExecutor(nthreads, nthreads, 30000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nthreads * 256), new CustomThreadFactory("Warp ScriptRunner Thread"));
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

      String zkconnect = config.getProperty(Configuration.RUNNER_KAFKA_CONSUMER_BOOTSTRAP_SERVERS);
      Properties initialConfig = Configuration.extractPrefixed(config, config.getProperty(Configuration.RUNNER_KAFKA_CONSUMER_CONF_PREFIX));
      this.topic = config.getProperty(Configuration.RUNNER_KAFKA_TOPIC);
      String groupid = config.getProperty(Configuration.RUNNER_KAFKA_GROUPID);
      String clientid = config.getProperty(Configuration.RUNNER_KAFKA_CONSUMER_CLIENTID);
      String strategy = config.getProperty(Configuration.RUNNER_KAFKA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY);
      int nthreads = Integer.parseInt(config.getProperty(Configuration.RUNNER_KAFKA_NTHREADS));
      long commitPeriod = Long.parseLong(config.getProperty(Configuration.RUNNER_KAFKA_COMMITPERIOD));

      ThreadPoolExecutor runnersExecutor = new ThreadPoolExecutor(nthreads, nthreads, 30000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nthreads * 256), new CustomThreadFactory("Warp ScriptRunner Thread"));
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

      // @see <a href="http://kafka.apache.org/documentation.html#producerconfigs">http://kafka.apache.org/documentation.html#producerconfigs</a>
      props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty(Configuration.RUNNER_KAFKA_PRODUCER_BOOTSTRAP_SERVERS));
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

  /**
   * When a script is removed from disk, call this function to remove the attached context.
   * This is not applicable for distributed scheduler, as it cannot store context for one script.
   *
   * @param scriptName
   */
  protected void removeRunnerContext(String scriptName) {
  }

  @Override
  public void run() {

    long lastscan = System.nanoTime() - 2 * scanperiod * 1000000L;

    //
    // Periodicity of scripts
    //

    final Map<String, Long> scripts = new HashMap<String, Long>();

    final AtomicLong nanoref = new AtomicLong();

    PriorityQueue<String> runnables = new PriorityQueue<String>(1, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        long nextrun1 = null != nextrun.get(o1) ? nextrun.get(o1) : Long.MAX_VALUE;
        long nextrun2 = null != nextrun.get(o2) ? nextrun.get(o2) : Long.MAX_VALUE;

        long nanos = nanoref.get();

        //
        // We order by the delta to nanos, this is an approximation of the actual order
        // but should be fine in most of the cases
        //

        return Long.compare(nextrun1 - nanos, nextrun2 - nanos);
      }
    });

    // Wait until we are initialized so the local endpoint is up before we may attempt to use it
    while(!WarpDist.isInitialized()) {
      LockSupport.parkNanos(100000000L);
    }

    while (true) {
      long now = System.nanoTime();

      if (now - lastscan > this.scanperiod * 1000000L) {
        Map<String, Long> newscripts = scanSuperRoot(this.root);

        Set<String> currentScripts = new HashSet<String>(scripts.keySet()); // copy object
        scripts.clear();
        scripts.putAll(newscripts);

        //
        // Clear entries from 'nextrun' which are for scripts which no longer exist
        //

        for (String prevscript: currentScripts) {
          if (!scripts.containsKey(prevscript)) {
            nextrun.remove(prevscript);
            lastrun.remove(prevscript);
            lastduration.remove(prevscript);
            lasterror.remove(prevscript);
            removeRunnerContext(prevscript);
          }
        }

        lastscan = now;
      }

      //
      // Build a queue of runnable scripts
      //

      runnables.clear();
      nanoref.set(now);

      for (Map.Entry<String, Long> scriptAndPeriod: scripts.entrySet()) {
        //
        // If script has no scheduled run yet or should run immediately, select it
        //
        String script = scriptAndPeriod.getKey();
        Long schedule = nextrun.get(script);

        if (null == schedule) {
          if (runAtStartup) {
            runnables.add(script);
          } else {
            Long period = 1000000L * scriptAndPeriod.getValue();
            long schedat = System.nanoTime();
            long timenanos = TimeSource.getNanoTime();

            if (0 != timenanos % period) {
              long delta = period - (timenanos % period);
              schedat = schedat + delta;
            }

            nextrun.put(script, schedat);
          }
        } else if (-1L != schedule && (schedule - now) <= 0) {
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
      final Map<String,Long> flastrun = this.lastrun;
      final Map<String,Long> flastduration = this.lastduration;
      final Map<String,String> flasterror = this.lasterror;

      this.executor.submit(new Runnable() {
        @Override
        public void run() {
          long nowns = System.nanoTime();

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

            byte[] nonce = null;

            //
            // If a pre-shared key exists, generate a nonce. This is used to determine that
            // an execution comes from a runner and to lift egress maxtime limits when calling an endpoint.
            //

            if (null != runnerPSK) {
              //
              // Generate a nonce by wrapping the current time jointly with random 64bits
              //

              byte[] noncebytes = Longs.toByteArray(TimeSource.getNanoTime());

              //
              // Add path
              //

              byte[] pathbytes = path.getBytes(StandardCharsets.UTF_8);
              noncebytes = Arrays.copyOf(noncebytes, 8 + pathbytes.length);
              System.arraycopy(pathbytes, 0, noncebytes, 8, pathbytes.length);

              nonce = OrderPreservingBase64.encode(CryptoHelper.wrapBlob(runnerPSK, noncebytes));

              conn.setRequestProperty(Constants.HTTP_HEADER_RUNNER_NONCE, new String(nonce, StandardCharsets.US_ASCII));
            }


            conn.setRequestMethod("POST");

            conn.connect();

            OutputStream out = conn.getOutputStream();


            //
            // Push the script parameters
            //

            out.write(Long.toString(periodicity).getBytes(StandardCharsets.UTF_8));
            out.write(' ');
            out.write('\'');
            out.write(URLEncoder.encode(Constants.RUNNER_PERIODICITY, StandardCharsets.UTF_8.name()).replaceAll("\\+", "%20").getBytes(StandardCharsets.US_ASCII));
            out.write('\'');
            out.write(' ');
            out.write(WarpScriptLib.STORE.getBytes(StandardCharsets.UTF_8));
            out.write('\n');

            out.write('\'');
            out.write(URLEncoder.encode(path, StandardCharsets.UTF_8.name()).replaceAll("\\+", "%20").getBytes(StandardCharsets.US_ASCII));
            out.write('\'');
            out.write(' ');
            out.write('\'');
            out.write(URLEncoder.encode(Constants.RUNNER_PATH, StandardCharsets.UTF_8.name()).replaceAll("\\+", "%20").getBytes(StandardCharsets.US_ASCII));
            out.write('\'');
            out.write(' ');
            out.write(WarpScriptLib.STORE.getBytes(StandardCharsets.UTF_8));
            out.write('\n');

            out.write(Long.toString(scheduledat).getBytes(StandardCharsets.UTF_8));
            out.write(' ');
            out.write('\'');
            out.write(URLEncoder.encode(Constants.RUNNER_SCHEDULEDAT, StandardCharsets.UTF_8.name()).replaceAll("\\+", "%20").getBytes(StandardCharsets.US_ASCII));
            out.write('\'');
            out.write(' ');
            out.write(WarpScriptLib.STORE.getBytes(StandardCharsets.UTF_8));
            out.write('\n');

            if (null != nonce) {
              out.write('\'');
              out.write(nonce);
              out.write('\'');
              out.write(' ');
              out.write('\'');
              out.write(URLEncoder.encode(Constants.RUNNER_NONCE, StandardCharsets.UTF_8.name()).replaceAll("\\+", "%20").getBytes(StandardCharsets.US_ASCII));
              out.write('\'');
              out.write(' ');
              out.write(WarpScriptLib.STORE.getBytes(StandardCharsets.UTF_8));
              out.write('\n');
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            // Strip the period out of the path and add a leading '/'
            String rawpath = "/" + path.replaceFirst("/" + Long.toString(periodicity) + "/", "/");
            // Remove the file extension
            rawpath = rawpath.substring(0, rawpath.length() - 4);

            while (true) {
              String line = br.readLine();

              if (null == line) {
                break;
              }

              // Replace ${name} and ${name:default} constructs

              Matcher m = VAR.matcher(line);

              StringBuffer mc2WithReplacement = new StringBuffer();

              while(m.find()) {
                String var = m.group(1);
                String def = m.group(0);

                int colonIndex = var.indexOf(':');
                if (colonIndex >= 0) {
                  def = var.substring(colonIndex + 1);
                  var = var.substring(0, colonIndex);
                }

                // Check in the configuration if we can find a matching key, i.e.
                // name@/path/to/script (with the period omitted) or any shorter prefix
                // of the path, i.e. name@/path/to or name@/path
                String suffix = rawpath;

                String value = null;

                while (suffix.length() > 1) {
                  value = WarpConfig.getProperty(var + "@" + suffix);
                  if (null != value) {
                    break;
                  }
                  suffix = suffix.substring(0, suffix.lastIndexOf('/'));
                }

                if (null == value) {
                  value = def;
                }

                m.appendReplacement(mc2WithReplacement, Matcher.quoteReplacement(value));
              }

              m.appendTail(mc2WithReplacement);
              out.write(mc2WithReplacement.toString().getBytes(StandardCharsets.UTF_8));
              out.write('\n');
            }

            br.close();

            // Add a 'CLEAR' at the end of the script so we don't return anything
            out.write(CLEAR);

            out.close();

            if (200 != conn.getResponseCode()) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_FAILURES, labels, ttl, 1);
              StringBuilder sb = new StringBuilder();

              String hdr = conn.getHeaderField(Constants.getHeader(Configuration.HTTP_HEADER_ERROR_LINEX));

              if (null != hdr) {
                sb.append(hdr);
              } else {
                sb.append("-");
              }

              sb.append(" ");

              hdr = conn.getHeaderField(Constants.getHeader(Configuration.HTTP_HEADER_ERROR_POSITIONX));

              if (null != hdr) {
                sb.append(hdr);
              } else {
                sb.append("-");
              }

              sb.append(" ");

              hdr = conn.getHeaderField(Constants.getHeader(Configuration.HTTP_HEADER_ERROR_MESSAGEX));

              if (null != hdr) {
                sb.append(hdr);
              } else {
                sb.append("-");
              }

              sb.append(" ");
              sb.append(conn.getResponseCode());
              sb.append(" ");
              sb.append(conn.getResponseMessage());

              flasterror.put(script, sb.toString());
            } else {
              flasterror.remove(script);
            }
          } catch (Exception e) {
            flasterror.put(script, e.getMessage());
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_FAILURES, labels, ttl, 1);
          } finally {
            flastrun.put(script, nano);
            nextrun.put(script, nowns + periodicity * 1000000L);
            nano = System.nanoTime() - nano;
            flastduration.put(script, nano);
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
      nextrun.put(script, System.nanoTime());
    }
  }

  private void distributedSchedule(Map<String, Long> nextrun, final String script, final long periodicity) {

    RunRequest request = new RunRequest();

    String path = new File(script).getAbsolutePath().substring(new File(this.root).getAbsolutePath().length() + 1);

    long now = System.currentTimeMillis();
    long nowts = System.nanoTime();

    final Map<String,Long> flastrun = this.lastrun;
    final Map<String,Long> flastduration = this.lastduration;
    final Map<String,String> flasterror = this.lasterror;

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
      flasterror.remove(script);
    } catch (IOException ioe) {
      // Reschedule immediately
      flasterror.put(script, ioe.getMessage());
      nextrun.put(script, System.nanoTime());
      return;
    } finally {
      flastrun.put(script, nowts);
      flastduration.put(script, System.nanoTime() - nowts);
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
      TSerializer serializer = ThriftUtils.getTSerializer();
      content = serializer.serialize(request);
    } catch (TException te) {
      // Reschedule immediately
      nextrun.put(script, System.nanoTime());
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
    byte[] key = path.getBytes(StandardCharsets.UTF_8);
    ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>(this.topic, key, content);

    try {
      producer = this.kafkaProducerPool.getProducer();
      // Call get() so we simulate a synchronous producer
      producer.send(record).get();
    } catch (Exception e) {
      // Reschedule immediately
      nextrun.put(script, System.nanoTime());
      return;
    } finally {
      if (null != producer) {
        this.kafkaProducerPool.recycleProducer(producer);
      }
    }

    nextrun.put(script, nowts + periodicity * 1000000L);
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

        Iterator<Path> iter = null;
        try {
          iter = Files.walk(f.toPath(), FileVisitOption.FOLLOW_LINKS)
              //.filter(path -> path.toString().endsWith(".mc2"))
              .filter(new Predicate<Path>() {
                @Override
                public boolean test(Path t) {
                  return t.toString().endsWith(".mc2");
                }
              })
              .iterator();
        } catch (IOException e) {
        }

        while (null != iter && iter.hasNext()) {
          Path p = iter.next();
          scripts.put(p.toString(), period);
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
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_KAFKA_RUNNER, props, Configuration.RUNNER_KAFKA_MAC, 128);

    this.keystore.forget();
  }

  /**
   * Return the next run for the runners matching the provided regexp.
   * The next run time is in nanoseconds
   * @param regexp Regexp to match runners path (relative to root)
   * @return
   */
  public Map<String,Object> getScheduled(String regexp) throws IOException {
    Map<String,Object> scheduled = new LinkedHashMap<String,Object>();

    Matcher m = null != regexp ? Pattern.compile(regexp).matcher("") : null;

    Path root = new File(this.root).toPath();

    for (Entry<String,Long> entry: this.nextrun.entrySet()) {
      Path p = new File(entry.getKey()).toPath();

      String name = null;

      if (p.startsWith(root)) {
        String runner = p.getName(p.getNameCount() - 1).toString();
        String periodicity = p.getName(p.getNameCount() - 2).toString();
        String group = p.getName(p.getNameCount() - 3).toString();

        name = group + "/" + periodicity + "/" + runner;

        if (null != m) {
          if (!m.reset(name).matches()) {
            continue;
          }
        }
      }

      Map<String,Object> params = new LinkedHashMap<String,Object>();
      Long lrun = lastrun.get(entry.getKey());
      Long lduration = lastduration.get(entry.getKey());
      String lerror = lasterror.get(entry.getKey());

      long nanodelta = TimeSource.getNanoTime() - System.nanoTime();
      // Convert to ms
      params.put(KEY_LASTRUN, null == lrun ? Long.MIN_VALUE : (lrun.longValue() + nanodelta) / (1_000_000_000L / Constants.TIME_UNITS_PER_S));
      params.put(KEY_LASTDURATION, null == lduration ? 0L : lduration);
      params.put(KEY_LASTERROR, lerror);
      if (null == entry.getValue() || -1L == entry.getValue()) {
        params.put(KEY_NEXTRUN, Long.MAX_VALUE);
        params.put(KEY_RUNNING, null != entry.getValue());
      } else {
        params.put(KEY_RUNNING, false);
        params.put(KEY_NEXTRUN, (entry.getValue().longValue() + nanodelta) / (1_000_000_000L / Constants.TIME_UNITS_PER_S));

      }
      scheduled.put(name, params);
    }

    return scheduled;
  }

  public void reschedule(String script, long when) throws IllegalStateException {
    long nanodelta = TimeSource.getNanoTime() - System.nanoTime();
    // Add root directory
    script = new File(this.root, script).toString();
    // If the script is known, update the entry in nextrun
    if (this.nextrun.containsKey(script)) {
      long nextrun = this.nextrun.get(script);
      if (-1L == nextrun) { // The runner is currently executing
        throw new IllegalStateException("Runner currently executing.");
      }
      this.nextrun.put(script, when - nanodelta);
    }
  }

  public static void register(ScriptRunner sr) {
    INSTANCE = sr;
  }

  public static ScriptRunner getInstance() {
    return INSTANCE;
  }
}
