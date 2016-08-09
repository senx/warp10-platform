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
import io.warp10.continuum.DirectoryUtil;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.continuum.thrift.data.GeoDirectoryRequest;
import io.warp10.continuum.thrift.data.GeoDirectoryResponse;
import io.warp10.continuum.thrift.service.GeoDirectoryService;
import io.warp10.continuum.thrift.service.GeoDirectoryService.Client;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.HyperLogLogPlus;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.x.discovery.ServiceCache;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.ServiceCacheListener;

public class GeoDirectoryThriftClient implements ServiceCacheListener, GeoDirectoryClient {
  
  private final CuratorFramework curatorFramework;
  
  private final ServiceCache<Map> serviceCache;
  
  /**
   * Clients, keyed by UUID
   */
  private Map<String,GeoDirectoryService.Client> clientCache = new ConcurrentHashMap<String, GeoDirectoryService.Client>();
  
  private Map<String,Integer> modulus = new ConcurrentHashMap<String, Integer>();
  private Map<String,Integer> remainder = new ConcurrentHashMap<String, Integer>();
  
  /**
   * Name of GeoDirectory per instance UUID
   */
  private Map<String,String> names = new ConcurrentHashMap<String, String>();
  
  private ExecutorService executor = null;
  
  public GeoDirectoryThriftClient(KeyStore keystore, Properties props) throws Exception {
  
    if (!props.containsKey(Configuration.GEODIR_ZK_SERVICE_QUORUM) || !props.containsKey(Configuration.GEODIR_ZK_SERVICE_ZNODE)) {
      curatorFramework = null;
      serviceCache = null;
      return;
    }
    
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(props.getProperty(Configuration.GEODIR_ZK_SERVICE_QUORUM))
        .build();
    curatorFramework.start();

    ServiceDiscovery<Map> discovery = ServiceDiscoveryBuilder.builder(Map.class)
        .basePath(props.getProperty(Configuration.GEODIR_ZK_SERVICE_ZNODE))
        .client(curatorFramework)
        .build();
    
    discovery.start();
    
    serviceCache = discovery.serviceCacheBuilder().name(GeoDirectory.GEODIR_SERVICE).build();
    
    serviceCache.addListener(this);
    serviceCache.start();
    cacheChanged();
  }
  
  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
  }
  
  @Override
  public void cacheChanged() {
    
    //
    // Rebuild the Client cache
    //
    
    List<ServiceInstance<Map>> instances = serviceCache.getInstances();
    
    //
    // Allocate new clients
    //
    
    Map<String,Client> newClients = new ConcurrentHashMap<String, GeoDirectoryService.Client>();
    Map<String,Integer> newModulus = new ConcurrentHashMap<String, Integer>();
    Map<String,Integer> newRemainder = new ConcurrentHashMap<String, Integer>();
    Map<String,String> newGeoDirectory = new ConcurrentHashMap<String, String>();    
    
    for (ServiceInstance<Map> instance: instances) {      
      String id = instance.getId();
      
      String host = instance.getAddress();
      int port = instance.getPort();
      TTransport transport = new TSocket(host, port);
      try {
        transport.open();
      } catch (TTransportException tte) {
        // FIXME(hbs): log
        continue;
      }
      
      if (instance.getPayload().containsKey(GeoDirectory.INSTANCE_PAYLOAD_THRIFT_MAXFRAMELEN)) {
        transport = new TFramedTransport(transport, Integer.parseInt(instance.getPayload().get(GeoDirectory.INSTANCE_PAYLOAD_THRIFT_MAXFRAMELEN).toString()));        
      } else {
        transport = new TFramedTransport(transport);        
      }

      GeoDirectoryService.Client client = new GeoDirectoryService.Client(new TCompactProtocol(transport));
      newClients.put(id, client);
      newModulus.put(id, Integer.parseInt(instance.getPayload().get(GeoDirectory.INSTANCE_PAYLOAD_MODULUS).toString()));
      newRemainder.put(id, Integer.parseInt(instance.getPayload().get(GeoDirectory.INSTANCE_PAYLOAD_REMAINDER).toString()));
      newGeoDirectory.put(id, instance.getPayload().get(GeoDirectory.INSTANCE_PAYLOAD_GEODIR).toString());
    }

    //
    // Close current clients and allocate new ones
    //
    
    synchronized(clientCache) {
    
      for (Entry<String,GeoDirectoryService.Client> entry: clientCache.entrySet()) {
        synchronized(entry.getValue()) {
          entry.getValue().getInputProtocol().getTransport().close();
        }
      }

      clientCache = newClients;
      modulus = newModulus;
      remainder = newRemainder;
      names = newGeoDirectory;
      
      //
      // Shut down the current executor
      //
      
      if (null != executor) {
        executor.shutdown();
      }
      
      //
      // Allocate a new executor with 4x as many threads as there are clients
      //
      
      executor = Executors.newCachedThreadPool();
    }    
  }
  
  @Override
  public List<Metadata> filter(String geodir, List<Metadata> metadatas, GeoXPShape area, boolean inside, long startTimestamp, long endTimestamp) throws IOException {
    
    final GeoDirectoryRequest request = new GeoDirectoryRequest();

    request.setEndTimestamp(endTimestamp);
    request.setStartTimestamp(startTimestamp);
    request.setInside(inside);
    
    Map<String,Metadata> meta = new HashMap<String, Metadata>();

    //
    // TODO(hbs): we could have another strategy, to only build requests with the set of GTS ids
    // that a given GeoDirectory will indeed handle (via modulus/remainder).
    // This would have to be deferred until we actually select the clients
    // For now let's keep it this way...
    //
    
    for (Metadata metadata: metadatas) {
      long classid = metadata.getClassId();
      long labelsid = metadata.getLabelsId();
    
      String id = GTSHelper.gtsIdToString(classid, labelsid);
      
      meta.put(id, metadata);
      
      if (0 == request.getGtsSize() || !request.getGts().containsKey(classid)) {
        request.putToGts(classid, new HashSet<Long>());
      }
      
      request.getGts().get(classid).add(labelsid);
    }
    
    long[] cells = GeoXPLib.getCells(area);
    
    List<Long> lcells = new ArrayList<Long>();
    
    for (long cell: cells) {
      lcells.add(cell);
    }
    
    request.setShape(lcells);
    
    List<Future<GeoDirectoryResponse>> responses = new ArrayList<Future<GeoDirectoryResponse>>();
    
    // Set of already called modulus:remainder combos
    Set<Integer> called = new HashSet<Integer>();

    long selectedmodulus = -1L;
    
    synchronized(clientCache) {
      for (Entry<String,GeoDirectoryService.Client> entry: clientCache.entrySet()) {
        //
        // Skip clients for the wrong GeoDirectory
        //
        if (!geodir.equals(this.names.get(entry.getKey()))) {
          continue;
        }
        if (-1L == selectedmodulus) {
          selectedmodulus = modulus.get(entry.getKey());
        }
        
        // Make sure we use a common modulus
        if (modulus.get(entry.getKey()) != selectedmodulus) {
          continue;
        }
        
        // Skip client if we already called one with this remainder
        if (called.contains(remainder.get(entry.getKey()))) {
          continue;
        }
        
        final GeoDirectoryService.Client clnt = entry.getValue();
        responses.add(executor.submit(new Callable<GeoDirectoryResponse>() {
          @Override
          public GeoDirectoryResponse call() throws Exception {
            synchronized(clnt) {
              try {
                GeoDirectoryResponse response = clnt.filter(request);
                return response;
              } catch (TTransportException tte) {
                cacheChanged();
                throw tte;
              }
            }
          }
        }));
        called.add(remainder.get(entry.getKey()));
      }      
    }
    
    //
    // Await for all requests to have completed, either successfully or not
    //
    
    int count = 0;
    
    while(count != responses.size()) {
      try { Thread.sleep(1L); } catch (InterruptedException ie) {}
      count = 0;
      for (Future<GeoDirectoryResponse> response: responses) {
        if (response.isDone()) {
          count++;
        }
      }
    }
    
    //
    // Build the set of extracted GTS Ids
    //
    
    Throwable error = null;

    Set<String> selected = new HashSet<String>();
    
    for (Future<GeoDirectoryResponse> response: responses) {
      try {        
        GeoDirectoryResponse resp = response.get();

        if (0 == resp.getGtsSize()) {
          continue;
        }
        
        for (Entry<Long,Set<Long>> entry: resp.getGts().entrySet()) {
          for (long labelsid: entry.getValue()) {
            String id = GTSHelper.gtsIdToString(entry.getKey(), labelsid);
            selected.add(id);
          }
        }
      } catch (CancellationException ce) {
        error = ce;
      } catch (ExecutionException ee) {
        error = ee.getCause();
      } catch (InterruptedException ie) {
        error = ie;
      } finally {
        if (null != error) {
          Map<String,String> labels = new HashMap<String, String>();
          labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, geodir);
          Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_CLIENT_ERRORS, labels, 1);

          throw new IOException(error);
        }
        
      }
    }
    
    List<Metadata> result = new ArrayList<Metadata>();
    
    for (String id: selected) {
      result.add(meta.get(id));
    }
    
    return metadatas;
  }
  
  @Override
  public Set<String> getDirectories() {
    Set<String> geodirs = new HashSet<String>();
    
    geodirs.addAll(this.names.values());

    return geodirs;
  }
  
  @Override
  public boolean knowsDirectory(String name) {
    return this.names.containsValue(name);
  }
}
