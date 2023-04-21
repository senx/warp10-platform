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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.DirectoryUtil;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.Directory;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsResponse;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.HyperLogLogPlus;
import io.warp10.sensision.Sensision;

public class ThriftDirectoryClient implements ServiceCacheListener, DirectoryClient {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftDirectoryClient.class);

  private static final String STATS_GTS_ESTIMATOR = "gts.estimate";
  private static final String STATS_CLASSES_ESTIMATOR = "classes.estimate";
  private static final String STATS_LABEL_NAMES_ESTIMATOR = "labelnames.estimate";
  private static final String STATS_LABEL_VALUES_ESTIMATOR = "labelvalues.estimate";
  private static final String STATS_PER_CLASS_ESTIMATOR = "per.class.estimate";
  private static final String STATS_PER_LABEL_VALUE_ESTIMATOR = "per.label.value.estimate";
  private static final String STATS_PARTIAL = "partial.results";
  private static final String STATS_ERROR = "error.rate";

  private final CuratorFramework curatorFramework;

  private final ServiceCache<Map> serviceCache;

  private List<String> clientCache = new ArrayList<String>();

  private Map<String,Integer> modulus = new ConcurrentHashMap<String, Integer>();
  private Map<String,Integer> remainder = new ConcurrentHashMap<String, Integer>();
  private Map<String,String> hosts = new ConcurrentHashMap<String,String>();
  private Map<String,Integer> streamingPorts = new ConcurrentHashMap<String,Integer>();

  private ExecutorService executor = null;

  private final Object executorMutex = new Object();
  private final Object clientCacheMutex = new Object();

  private final long[] SIPHASH_PSK;

  final AtomicBoolean transportException = new AtomicBoolean(false);

  private final boolean noProxy;

  public ThriftDirectoryClient(KeyStore keystore, Properties props) throws Exception {

    // Extract Directory PSK

    SIPHASH_PSK = SipHashInline.getKey(keystore.getKey(KeyStore.SIPHASH_DIRECTORY_PSK));

    this.curatorFramework = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(props.getProperty(Configuration.DIRECTORY_ZK_QUORUM))
        .build();
    this.curatorFramework.start();

    if ("true".equals(props.getProperty(Configuration.DIRECTORY_STREAMING_NOPROXY))) {
      this.noProxy = true;
    } else {
      this.noProxy = false;
    }

    ServiceDiscovery<Map> discovery = ServiceDiscoveryBuilder.builder(Map.class)
        .basePath(props.getProperty(Configuration.DIRECTORY_ZK_ZNODE))
        .client(curatorFramework)
        .build();

    discovery.start();

    serviceCache = discovery.serviceCacheBuilder().name(Directory.DIRECTORY_SERVICE).build();

    serviceCache.addListener(this);
    serviceCache.start();
    cacheChanged();
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
  }

  @Override
  public void cacheChanged() {

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_CLIENT_CACHE_CHANGED, Sensision.EMPTY_LABELS, 1);

    synchronized(clientCacheMutex) {
      //
      // Clear transportException
      //

      transportException.set(false);

      //
      // Rebuild the Client cache
      //

      List<ServiceInstance<Map>> instances = serviceCache.getInstances();

      //
      // Allocate new clients
      //

      List<String> newClients = new ArrayList<String>();
      Map<String,Integer> newModulus = new ConcurrentHashMap<String, Integer>();
      Map<String,Integer> newRemainder = new ConcurrentHashMap<String, Integer>();
      Map<String,String> newHosts = new ConcurrentHashMap<String,String>();
      Map<String,Integer> newStreamingPorts = new ConcurrentHashMap<String,Integer>();

      //
      // Determine which instances we should retain.
      // Only the instances which cover the full range of remainders for a given
      // modulus should be retained
      //

      // Set of available remainders per modulus
      Map<Integer,Set<Integer>> remaindersPerModulus = new HashMap<Integer,Set<Integer>>();

      for (ServiceInstance<Map> instance: instances) {
        int modulus = Integer.parseInt(instance.getPayload().get(Directory.PAYLOAD_MODULUS_KEY).toString());
        int remainder = Integer.parseInt(instance.getPayload().get(Directory.PAYLOAD_REMAINDER_KEY).toString());

        // Skip invalid modulus/remainder
        if (modulus <= 0 || remainder >= modulus) {
          continue;
        }

        if (!remaindersPerModulus.containsKey(modulus)) {
          remaindersPerModulus.put(modulus, new HashSet<Integer>());
        }

        remaindersPerModulus.get(modulus).add(remainder);
      }

      //
      // Only retain the moduli which have a full set of remainders
      //

      Set<Integer> validModuli = new HashSet<Integer>();

      for (Entry<Integer,Set<Integer>> entry: remaindersPerModulus.entrySet()) {
        if (entry.getValue().size() == entry.getKey()) {
          validModuli.add(entry.getKey());
        }
      }

      for (ServiceInstance<Map> instance: instances) {

        int modulus = Integer.parseInt(instance.getPayload().get(Directory.PAYLOAD_MODULUS_KEY).toString());

        //
        // Skip instance if it is not associated with a valid modulus
        //

        if (!validModuli.contains(modulus)) {
          continue;
        }

        String id = instance.getId();

        String host = instance.getAddress();
        int port = instance.getPort();

        if (instance.getPayload().containsKey(Directory.PAYLOAD_STREAMING_PORT_KEY)) {
          newHosts.put(id, instance.getAddress());
          newStreamingPorts.put(id, Integer.parseInt(instance.getPayload().get(Directory.PAYLOAD_STREAMING_PORT_KEY).toString()));
        }

        newClients.add(id);
        newModulus.put(id, modulus);
        newRemainder.put(id, Integer.parseInt(instance.getPayload().get(Directory.PAYLOAD_REMAINDER_KEY).toString()));
      }

      //
      // Close current clients and allocate new ones
      //

      synchronized(clientCacheMutex) {

        clientCache = newClients;
        modulus = newModulus;
        remainder = newRemainder;

        hosts = newHosts;
        streamingPorts = newStreamingPorts;

        //
        // Shut down the current executor
        //

        if (null != executor) {
          ExecutorService oldexecutor = executor;

          synchronized(executorMutex) {
            //
            // Allocate a new executor with 4x as many threads as there are clients
            //

            executor = Executors.newCachedThreadPool();
          }

          oldexecutor.shutdown();
        } else {
          synchronized(executorMutex) {
            executor = Executors.newCachedThreadPool();
          }
        }
      }
    }
  }

  @Override
  public List<Metadata> find(DirectoryRequest request) throws IOException {
    throw new IOException("USE ITERATOR");
  }

  @Override
  public Map<String,Object> stats(DirectoryRequest request) throws IOException {
    return statsHttp(request);
  }

  public Map<String,Object> statsHttp(DirectoryRequest req) throws IOException {

    List<String> classSelector = req.getClassSelectors();
    List<Map<String, String>> labelsSelectors = req.getLabelsSelectors();

    //
    // Extract the URLs we will use to retrieve the Metadata
    //

    // Set of already called remainders for the selected modulus
    Set<Integer> called = new HashSet<Integer>();

    long selectedmodulus = -1L;

    final List<URL> urls = new ArrayList<URL>();

    List<String> servers = new ArrayList<String>();

    synchronized(clientCacheMutex) {
      servers.addAll(clientCache);
    }

    // Shuffle the list
    Collections.shuffle(servers);

    for (String server: servers) {
      //
      // Make sure the current entry has a streaming port defined
      //

      if (!streamingPorts.containsKey(server)) {
        continue;
      }

      if (-1L == selectedmodulus) {
        selectedmodulus = modulus.get(server);
      }

      // Make sure we use a common modulus
      if (modulus.get(server) != selectedmodulus) {
        continue;
      }

      // Skip client if we already called one with this remainder
      if (called.contains(remainder.get(server))) {
        continue;
      }

      //
      // Extract host and port
      //

      String host = hosts.get(server);
      int port = streamingPorts.get(server);

      URL url = new URL("http://" + host + ":" + port + "" + Constants.API_ENDPOINT_DIRECTORY_STATS_INTERNAL);

      urls.add(url);

      // Track which remainders we already selected
      called.add(remainder.get(server));
    }

    final DirectoryStatsRequest request = new DirectoryStatsRequest();
    request.setTimestamp(System.currentTimeMillis());
    request.setClassSelector(classSelector);
    request.setLabelsSelectors(labelsSelectors);

    long hash = DirectoryUtil.computeHash(this.SIPHASH_PSK[0], this.SIPHASH_PSK[1], request);

    request.setHash(hash);

    List<Future<DirectoryStatsResponse>> responses = new ArrayList<Future<DirectoryStatsResponse>>();

    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

    byte[] bytes = null;

    try {
      bytes = OrderPreservingBase64.encode(serializer.serialize(request));
    } catch (TException te) {
      throw new IOException(te);
    }

    final byte[] encodedReq = bytes;

    synchronized(executorMutex) {
      for (URL urlx: urls) {

        final URL url = urlx;

        responses.add(executor.submit(new Callable<DirectoryStatsResponse>() {
          @Override
          public DirectoryStatsResponse call() throws Exception {
            HttpURLConnection conn = null;

            try {
              conn = (HttpURLConnection) (noProxy ? url.openConnection(Proxy.NO_PROXY) : url.openConnection());

              conn.setDoOutput(true);
              conn.setDoInput(true);
              conn.setRequestMethod("POST");
              conn.setChunkedStreamingMode(2048);
              conn.connect();

              OutputStream out = conn.getOutputStream();

              out.write(encodedReq);
              out.write('\r');
              out.write('\n');
              out.close();

              BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));

              DirectoryStatsResponse resp = new DirectoryStatsResponse();

              try {

                while(true) {
                  String line = reader.readLine();

                  if (null == line) {
                    break;
                  }

                  byte[] data = OrderPreservingBase64.decode(line.getBytes(StandardCharsets.US_ASCII));

                  TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());

                  deser.deserialize(resp, data);
                }

                reader.close();
                reader = null;

              } catch (IOException ioe){
                try { reader.close(); } catch (Exception e) {}
                throw ioe;
              }

              return resp;
            } finally {
              if (null != conn) { try { conn.disconnect(); } catch (Exception e) {}
            }
          }
        }}));
      }
    }


    //
    // Await for all requests to have completed, either successfully or not
    //

    int count = 0;

    while(count != responses.size()) {
      LockSupport.parkNanos(1000L);
      count = 0;
      for (Future<DirectoryStatsResponse> response: responses) {
        if (response.isDone()) {
          count++;
        }
      }
    }

    return mergeStatsResponses(responses);
  }

  public static Map<String,Object> mergeStatsResponses(Iterable<Future<DirectoryStatsResponse>> responses) throws IOException {
    //
    // Consolidate the results
    //

    Throwable error = null;

    boolean partial = false;

    HyperLogLogPlus gtsCardinalityEstimator = null;
    HyperLogLogPlus classCardinalityEstimator = null;
    HyperLogLogPlus labelNamesCardinalityEstimator = null;
    HyperLogLogPlus labelValuesCardinalityEstimator = null;

    Map<String,HyperLogLogPlus> perClassCardinality = new HashMap<String, HyperLogLogPlus>();
    Map<String,HyperLogLogPlus> perLabelValueCardinality = new HashMap<String, HyperLogLogPlus>();

    for (Future<DirectoryStatsResponse> response: responses) {
      try {
        DirectoryStatsResponse resp = response.get();

        if (resp.isSetError()) {
          partial = true;
          continue;
        }

        // Total number of GTS
        HyperLogLogPlus card = HyperLogLogPlus.fromBytes(resp.getGtsCount());

        if (null == gtsCardinalityEstimator) {
          gtsCardinalityEstimator = card;
        } else {
          gtsCardinalityEstimator.fuse(card);
        }

        // Number of distinct classes
        HyperLogLogPlus classCardinality = HyperLogLogPlus.fromBytes(resp.getClassCardinality());

        if (null == classCardinalityEstimator) {
          classCardinalityEstimator = classCardinality;
        } else {
          classCardinalityEstimator.fuse(classCardinality);
        }

        // Number of distinct label names
        HyperLogLogPlus labelNamesCardinality = HyperLogLogPlus.fromBytes(resp.getLabelNamesCardinality());

        if (null == labelNamesCardinalityEstimator) {
          labelNamesCardinalityEstimator = labelNamesCardinality;
        } else {
          labelNamesCardinalityEstimator.fuse(labelNamesCardinality);
        }

        // Number of distinct label values
        HyperLogLogPlus labelValuesCardinality = HyperLogLogPlus.fromBytes(resp.getLabelValuesCardinality());

        if (null == labelValuesCardinalityEstimator) {
          labelValuesCardinalityEstimator = labelValuesCardinality;
        } else {
          labelValuesCardinalityEstimator.fuse(labelValuesCardinality);
        }

        if (resp.getPerClassCardinalitySize() > 0) {
          for (Entry<String,ByteBuffer> entry: resp.getPerClassCardinality().entrySet()) {
            byte[] data = new byte[entry.getValue().remaining()];
            entry.getValue().get(data);
            HyperLogLogPlus estimator = HyperLogLogPlus.fromBytes(data);

            if (perClassCardinality.containsKey(entry.getKey())) {
              perClassCardinality.get(entry.getKey()).fuse(estimator);
            } else {
              perClassCardinality.put(entry.getKey(), estimator);
            }
          }
        }

        if (resp.getPerLabelValueCardinalitySize() > 0) {
          for (Entry<String,ByteBuffer> entry: resp.getPerLabelValueCardinality().entrySet()) {
            byte[] data = new byte[entry.getValue().remaining()];
            entry.getValue().get(data);
            HyperLogLogPlus estimator = HyperLogLogPlus.fromBytes(data);

            if (perLabelValueCardinality.containsKey(entry.getKey())) {
              perLabelValueCardinality.get(entry.getKey()).fuse(estimator);
            } else {
              perLabelValueCardinality.put(entry.getKey(), estimator);
            }
          }
        }
      } catch (ClassNotFoundException cnfe) {
        error = cnfe;
      } catch (CancellationException ce) {
        error = ce;
      } catch (ExecutionException ee) {
        error = ee.getCause();
      } catch (InterruptedException ie) {
        error = ie;
      } finally {
        if (null != error) {
          partial = true;
        }
      }
    }

    //
    // Build a map of results
    //

    Map<String,Object> stats = new HashMap<String,Object>();

    if (null != gtsCardinalityEstimator) {
      stats.put(STATS_GTS_ESTIMATOR, gtsCardinalityEstimator.cardinality());
    }
    if (null != classCardinalityEstimator) {
      stats.put(STATS_CLASSES_ESTIMATOR, classCardinalityEstimator.cardinality());
    }

    if (null != labelNamesCardinalityEstimator) {
      stats.put(STATS_LABEL_NAMES_ESTIMATOR, labelNamesCardinalityEstimator.cardinality());
    }

    if (null != labelValuesCardinalityEstimator) {
      stats.put(STATS_LABEL_VALUES_ESTIMATOR, labelValuesCardinalityEstimator.cardinality());
    }

    Map<String,Long> cardinalitiesPerClass = new HashMap<String, Long>();
    for (Entry<String,HyperLogLogPlus> entry: perClassCardinality.entrySet()) {
      cardinalitiesPerClass.put(entry.getKey(), entry.getValue().cardinality());
    }
    stats.put(STATS_PER_CLASS_ESTIMATOR, cardinalitiesPerClass);

    Map<String,Long> cardinalitiesPerLabel = new HashMap<String, Long>();
    for (Entry<String,HyperLogLogPlus> entry: perLabelValueCardinality.entrySet()) {
      cardinalitiesPerLabel.put(entry.getKey(), entry.getValue().cardinality());
    }
    stats.put(STATS_PER_LABEL_VALUE_ESTIMATOR, cardinalitiesPerLabel);

    if (partial) {
      stats.put(STATS_PARTIAL, true);
    }

    stats.put(STATS_ERROR, 1.04 / Math.sqrt(1L << Directory.ESTIMATOR_P));

    return stats;
  }

  /**
   * Return an iterator on Metadata which accesses the streaming endpoints of directories
   */
  @Override
  public MetadataIterator iterator(DirectoryRequest request) throws IOException {

    //
    // Extract the URLs we will use to retrieve the Metadata
    //

    // Set of already called remainders for the selected modulus
    Set<Integer> called = new HashSet<Integer>();

    long selectedmodulus = -1L;

    final List<URL> urls = new ArrayList<URL>();

    List<String> servers = new ArrayList<String>();

    synchronized(clientCacheMutex) {
      servers.addAll(clientCache);
    }

    // Shuffle the list
    Collections.shuffle(servers);

    for (String server: servers) {
      //
      // Make sure the current entry has a streaming port defined
      //

      if (!streamingPorts.containsKey(server)) {
        continue;
      }

      if (-1L == selectedmodulus) {
        selectedmodulus = modulus.get(server);
      }

      // Make sure we use a common modulus
      if (modulus.get(server) != selectedmodulus) {
        continue;
      }

      // Skip client if we already called one with this remainder
      if (called.contains(remainder.get(server))) {
        continue;
      }

      //
      // Extract host and port
      //

      String host = hosts.get(server);
      int port = streamingPorts.get(server);

      URL url = new URL("http://" + host + ":" + port + "" + Constants.API_ENDPOINT_DIRECTORY_STREAMING_INTERNAL);

      urls.add(url);

      // Track which remainders we already selected
      called.add(remainder.get(server));
    }

    return StreamingMetadataIterator.getIterator(SIPHASH_PSK, request, urls, this.noProxy);
  }
}
