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

package io.warp10.standalone;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.geo.GeoDirectoryClient;
import io.warp10.continuum.geo.GeoIndex;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.PARSESELECTOR;
import io.warp10.sensision.Sensision;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.google.common.base.Charsets;

public class StandaloneGeoDirectory extends AbstractHandler implements StandalonePlasmaHandlerInterface, Runnable, GeoDirectoryClient {

  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  private final StoreClient storeClient;
  private final DirectoryClient directoryClient;
  
  /**
   * Map of index name to GeoIndex instance
   */
  private final Map<String,GeoIndex> indices = new HashMap<String, GeoIndex>();
  
  /**
   * Map of index name to map of token to set of subscribed GTS
   */
  private final Map<String, Map<String,Set<String>>> subscriptions = new HashMap<String, Map<String, Set<String>>>();

  /**
   * Map of token to consumer
   */
  private final Map<String,String> consumers = new HashMap<String, String>();
  
  /**
   * Map of index to token to associated selectors
   */
  private final Map<String, Map<String,Set<String>>> selectors = new HashMap<String, Map<String, Set<String>>>();

  /**
   * Delay between two subscription updates
   */
  private final long delay;
  
  /**
   * Maximum number of cells for search areas
   */
  private final int maxcells;

  private boolean hasSubscriptions = false;
  
  private final byte[] SUBS_AES;
  
  private final String subsDir;
  
  private final String subsPrefix;
  
  public StandaloneGeoDirectory(KeyStore keystore, StoreClient storeClient, DirectoryClient directoryClient, Properties props) {
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    this.delay = Long.parseLong(props.getProperty(Configuration.STANDALONE_GEODIR_DELAY, "300000")); 
    this.maxcells = Integer.parseInt(props.getProperty(Configuration.STANDALONE_GEODIR_MAXCELLS, "256"));
  
    this.subsDir = props.getProperty(Configuration.STANDALONE_GEODIR_SUBS_DIR, System.getProperty("java.io.tmpdir"));
    this.subsPrefix = props.getProperty(Configuration.STANDALONE_GEODIR_SUBS_PREFIX, "geodir");
    
    this.SUBS_AES = keystore.decodeKey(props.getProperty(Configuration.STANDALONE_GEODIR_AES));
    keystore.forget();
    
    //
    // Extract the various GeoDirectories
    //
    
    if (!props.containsKey(Configuration.STANDALONE_GEODIRS)) {
      return;
    }


    String[] geodirs = props.getProperty(Configuration.STANDALONE_GEODIRS).split(",");
    
    for (String geodir: geodirs) {
      String[] tokens = geodir.split("/");
      
      String name = tokens[0];
      int resolution = Integer.parseInt(tokens[1]);
      int chunks = Integer.parseInt(tokens[2]);
      long depth = Long.parseLong(tokens[3]);
      
      GeoIndex index = new GeoIndex(resolution, chunks, depth);
      
      this.indices.put(name, index);
      synchronized(this.selectors) {
        this.selectors.put(name, new HashMap<String, Set<String>>());
      }
      this.subscriptions.put(name, new HashMap<String, Set<String>>());
    }
    
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("Warp Standalone GeoDirectory");
    t.start();
  }
  
  @Override
  public boolean hasSubscriptions() {
    return hasSubscriptions;
  }
  
  @Override
  public void publish(GTSEncoder encoder) {
    // Compute String id for GTS
    String id = GTSHelper.gtsIdToString(encoder.getClassId(), encoder.getLabelsId());
    
    // Set of uuid which should be billed for the indexing, per index
    Map<String, Set<String>> billed = new HashMap<String, Set<String>>();
    
    for (Entry<String, Map<String,Set<String>>> entry: this.subscriptions.entrySet()) {
      billed.put(entry.getKey(), new HashSet<String>());
      for (Entry<String, Set<String>> subentry: entry.getValue().entrySet()) {
        if (subentry.getValue().contains(id)) {
          billed.get(entry.getKey()).add(this.consumers.get(subentry.getKey()));
        }
      }
    }

    //
    // Do the actual indexing and billing, per index
    //

    for (String geodir: billed.keySet()) {
      if (billed.get(geodir).isEmpty()) {
        continue;
      }
      
      // Do the indexing
      long indexed = this.indices.get(geodir).index(encoder);
      
      if (0 == indexed) {
        continue;
      }
      
      Map<String,String> labels = new HashMap<String, String>();
      
      for (String consumer: billed.get(geodir)) {
        if (null == consumer) {
          continue;
        }
        labels.clear();      
        labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, consumer);
        labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, geodir);
        Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_INDEXED_PERCONSUMER, labels, indexed);
      }
      
      labels.clear();      
      labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, geodir);
      Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_INDEXED, labels, indexed);
    }
  }
  
  private long publish(GeoIndex index, GTSEncoder encoder) {
    return index.index(encoder);
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    //
    // Only support GEO endpoint calls for our GeoDirectory instances
    //
    
    for (String name: this.indices.keySet()) {
      if ((Constants.API_ENDPOINT_GEO + "/" + name + Constants.API_ENDPOINT_GEO_LIST).equals(target)) {
        doList(name, baseRequest, request, response);
        break;
      } else if ((Constants.API_ENDPOINT_GEO + "/" + name + Constants.API_ENDPOINT_GEO_ADD).equals(target)) {
        doAdd(name, baseRequest, request, response);
        break;
      } else if ((Constants.API_ENDPOINT_GEO + "/" + name + Constants.API_ENDPOINT_GEO_REMOVE).equals(target)) {
        doRemove(name, baseRequest, request, response);
        break;
      } else if ((Constants.API_ENDPOINT_GEO + "/" + name + Constants.API_ENDPOINT_GEO_INDEX).equals(target)) {
        doIndex(name, baseRequest, request, response);
        break;
      }      
    }    
  }
  
  private void doList(String name, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    baseRequest.setHandled(true);
    
    String token = request.getParameter(Constants.HTTP_PARAM_TOKEN);

    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("text/plain");
    
    PrintWriter pw = response.getWriter();
    
    Set<String> selectorsSet = this.selectors.get(name).get(token);
    
    if (null == selectorsSet) {
      throw new IOException("Unknown token.");
    }

    for (String selector: selectorsSet) {
      pw.print("SELECTOR ");
      pw.println(selector);
    }    
        
    pw.print("SUBSCRIPTIONS ");
    pw.println(this.subscriptions.get(name).containsKey(token) ? this.subscriptions.get(name).get(token).size() : 0);
  }
  
  private void doAdd(String name, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    doAddRemove(name, true, baseRequest, request, response);
  }
  
  private void doRemove(String name, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    doAddRemove(name, false, baseRequest, request, response);
  }

  private void doAddRemove(String name, boolean add, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
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
    // Add selectors to the subscription for the given token
    //
    
    Set<String> subs = null;
    
    synchronized (this.selectors.get(name)) {
      subs = this.selectors.get(name).get(token);
      if (null == subs) {
        if (add) {
          subs = new HashSet<String>();
          this.selectors.get(name).put(token, subs);
        }
      }
    }
    
    for (String selector: selectors) {
      if (add) {
        subs.add(selector);
      } else if (null != subs) {
        subs.remove(selector);
      }
    }
    
    synchronized (this.selectors.get(name)) {
      if (!add && subs.isEmpty()) {
        this.selectors.get(name).remove(token);
      }
    }
    
    storeSelectors(name);
        
    updateSubscriptions(name, token);

    response.setStatus(HttpServletResponse.SC_OK);
  }

  /**
   * Force indexing of some data by fetching them and forwarding them onto the data topic
   * This enables someone with a ReadToken to re-index historical data
   */
  private void doIndex(String name, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    
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
    
    if (!this.subscriptions.get(name).containsKey(token)) {
      throw new IOException("The provided token does not have any current subscriptions in this index.");
    }
    
    //
    // INFO(hbs): this will trigger billing for every one subscribing to GTS in this index, we consider this a marginal case
    //

    //
    // Retrieve Metadata (below code is adapted from EgressFetchHandler
    //

    //
    // Extract the class and labels selectors
    // The class selector and label selectors are supposed to have
    // values which use percent encoding, i.e. explicit percent encoding which
    // might have been re-encoded using percent encoding when passed as parameter
    //
    //
    
    Set<Metadata> metadatas = new HashSet<Metadata>();
    
    String selector = selectors[0];
    
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
    // Force 'producer'/'owner'/'app' from token
    //
      
    ReadToken rtoken;
    
    try {
      rtoken = Tokens.extractReadToken(token);
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }

    labelsSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));

    List<Metadata> metas = null;
      
    List<String> clsSels = new ArrayList<String>();
    List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
      
    clsSels.add(classSelector);
    lblsSels.add(labelsSelectors);
      
    metas = directoryClient.find(clsSels, lblsSels);

    //
    // Now retrieve the associated data
    //
    
    long total = 0L;
    
    GeoIndex index = this.indices.get(name);
    
    String start = request.getParameter(Constants.HTTP_PARAM_START);
    String stop = request.getParameter(Constants.HTTP_PARAM_STOP);
    
    long now = Long.MIN_VALUE;
    long timespan = 0L;

    String nowParam = request.getParameter(Constants.HTTP_PARAM_NOW);
    String timespanParam = request.getParameter(Constants.HTTP_PARAM_TIMESPAN);
    String dedupParam = request.getParameter(Constants.HTTP_PARAM_DEDUP);
        
    boolean dedup = null != dedupParam && "true".equals(dedupParam);
    
    if (null != start && null != stop) {
      long tsstart = fmt.parseDateTime(start).getMillis() * Constants.TIME_UNITS_PER_MS;
      long tsstop = fmt.parseDateTime(stop).getMillis() * Constants.TIME_UNITS_PER_MS;
      
      if (tsstart < tsstop) {
        now = tsstop;
        timespan = tsstop - tsstart;
      } else {
        now = tsstart;
        timespan = tsstart - tsstop;
      }
    } else if (null != nowParam && null != timespanParam) {
      try {
        now = Long.valueOf(nowParam);
      } catch (Exception e) {
        now = fmt.parseDateTime(nowParam).getMillis() * Constants.TIME_UNITS_PER_MS;
      }
      
      timespan = Long.valueOf(timespanParam);
    }
    
    if (Long.MIN_VALUE == now) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing now/timespan or start/stop parameters.");
      return;
    }

    try (GTSDecoderIterator iter = this.storeClient.fetch(rtoken, metas, now, timespan, false, false)) {
      while(iter.hasNext()) {
        GTSDecoder decoder = iter.next();
        decoder.next();
        GTSEncoder encoder = decoder.getEncoder();
        total += publish(index, encoder);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    } finally {
    }

    response.setContentType("text/plain");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println(total);
  }

  private void storeSelectors(String geodir) {
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(this.subsDir + "/" + this.subsPrefix + "." + geodir));
      
      for (Entry<String,Map<String,Set<String>>> entry: this.selectors.entrySet()) {
        for (Entry<String,Set<String>> entry2: entry.getValue().entrySet()) {
          for (String selector: entry2.getValue()) {
            if (!geodir.equals(entry.getKey())) {
              continue;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey());
            sb.append("\t");
            sb.append(entry2.getKey());
            sb.append("\t");
            sb.append(selector);
            byte[] data = sb.toString().getBytes(Charsets.UTF_8);
            
            if (null != this.SUBS_AES) {
              data = CryptoUtils.wrap(this.SUBS_AES, data);
            }
            
            data = OrderPreservingBase64.encode(data);
            pw.println(new String(data, Charsets.US_ASCII));
          }
        }
      }
      
      pw.close();
    } catch (Exception e) {
      e.printStackTrace();
    }    
  }
  
  private void loadSelectors(String geodir) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(this.subsDir + "/" + this.subsPrefix + "." + geodir));
      
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        byte[] data = OrderPreservingBase64.decode(line.getBytes(Charsets.US_ASCII));
        
        if (null != this.SUBS_AES) {
          data = CryptoUtils.unwrap(this.SUBS_AES, data);
        }
        
        line = new String(data, Charsets.UTF_8);
        
        String[] tokens = line.split("\t");
        
        String name = tokens[0];
        String token = tokens[1];
        String selector = tokens[2];
        
        synchronized(this.selectors) {
          if (!this.selectors.containsKey(name)) {          
            this.selectors.put(name, new HashMap<String, Set<String>>());
          }          
        }
        
        if (!this.selectors.get(name).containsKey(token)) {
          this.selectors.get(name).put(token, new HashSet<String>());
        }
        
        this.selectors.get(name).get(token).add(selector);
      }
      
      br.close();
      
      //
      // Update subscriptions
      //
      
      updateSubscriptions(geodir, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private void updateSubscriptions(String geodir, String token) {
    //
    // Retrieve the Metadata from Directory
    //

    Set<String> tokens = new HashSet<String>();
    
    if (null == token) {
      tokens.addAll(this.selectors.get(geodir).keySet());
    } else {
      tokens.add(token);
    }
    
    for (String t: tokens) {
      
      Set<String> ids = new HashSet<String>();
      
      ReadToken rtoken = null;
      try {
        rtoken = Tokens.extractReadToken(t);        
      } catch (WarpScriptException ee) {
        ee.printStackTrace();
        // FIXME(hbs): we should 
        continue;
      }
      
      // Store 'billed' Id
      
      this.consumers.put(t, Tokens.getUUID(rtoken.getBilledId()));
      
      Map<String,String> tokenLabelSelectors = Tokens.labelSelectorsFromReadToken(rtoken);
      
      Set<String> sel = new HashSet<String>();
      
      if (this.selectors.get(geodir).containsKey(t)) {
        sel.addAll(this.selectors.get(geodir).get(t));
      }
      
      for (String selector: sel) {
        
        //
        // Parse selector
        //

        String classSelector = null;
        Map<String,String> labelSelectors = null;
        
        try {
          Object[] result = PARSESELECTOR.parse(selector);
          classSelector = (String) result[0];
          labelSelectors = (Map<String,String>) result[1];
        } catch (WarpScriptException ee) {
        }

        if (null == classSelector || null == labelSelectors) {
          //
          // Remove selector from set as it is bogus
          //
          
          if (this.selectors.get(geodir).containsKey(t)) {
            this.selectors.get(geodir).get(t).remove(sel);
          }
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
        
        List<Metadata> metadatas = null;
        
        try {
          metadatas = directoryClient.find(classSel, labelsSel);
        } catch (IOException ioe) {
          // Ignore error
          continue;
        }
        
        //
        // Compute the GTS Id for each GTS
        //
        
        for (Metadata metadata: metadatas) {
          long labelsid = metadata.getLabelsId();
          
          String gtsid = GTSHelper.gtsIdToString(metadata.getClassId(), metadata.getLabelsId());
          
          ids.add(gtsid);
        }
      }
      
      //
      // Store ids for token 't'
      //
      
      if (ids.size() > 0) {
        this.subscriptions.get(geodir).put(t, ids);
      } else {
        this.subscriptions.get(geodir).remove(t);
      }      
    }
    
    boolean subs = false;
    
    for (Map<String,Set<String>> entries: this.subscriptions.values()) {
      if (!entries.isEmpty()) {
        subs = true;
        break;
      }
    }
    
    this.hasSubscriptions = subs;
  }
  
  @Override
  public void run() {
    //
    // TODO(hbs): periodically reload subscriptions from Directory
    //
    
    //
    // Do the initial loading of subscriptions
    //

    for (String geodir: this.indices.keySet()) {
      loadSelectors(geodir);
    }
    
    while(true) {
      Set<String> keys;
      
      synchronized(this.selectors) {
        keys = this.selectors.keySet();
      }
      for (String geodir: keys) {
        updateSubscriptions(geodir, null);
      }
      try { Thread.sleep(this.delay); } catch (InterruptedException ie) {}
    }
  }
  
  public GeoDirectoryClient getClient() {
    return this;
  }

  @Override
  public List<Metadata> filter(String geodir, List<Metadata> metadatas, GeoXPShape area, boolean inside, long startTimestamp, long endTimestamp) throws IOException {
    if (!this.indices.containsKey(geodir)) {
      throw new IOException("Unknown GeoDirectory '" + geodir + "'.");
    }

    long nano = System.nanoTime();
    
    //
    // Limit shape to the maximum number of allowed cells
    //
    
    int shapeSize = GeoXPLib.getCells(area).length;
    
    area = GeoXPLib.limit(area, this.maxcells);

    //
    // Generate GTS ids
    //
    
    Map<String, Metadata> unfiltered = new HashMap<String, Metadata>();

    for (Metadata metadata: metadatas) {
      String id = GTSHelper.gtsIdToString(metadata.getClassId(), metadata.getLabelsId());
      unfiltered.put(id, metadata);
    }
    
    GeoIndex index = this.indices.get(geodir);
        
    //
    // Do the Geo filtering
    //
    
    Set<String> filtered = index.find(unfiltered.keySet(), area, inside, startTimestamp, endTimestamp);
    
    //
    // Update sensision metrics
    //
    
    Map<String,String> labels = new HashMap<String, String>();
    labels.put(SensisionConstants.SENSISION_LABEL_GEODIR, geodir);
    labels.put(SensisionConstants.SENSISION_LABEL_TYPE, inside ? "in" : "out");    
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_FILTERED, labels, filtered.size());
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_UNFILTERED, labels, unfiltered.size());
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_REQUESTS, labels, 1);
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_CELLS, labels, shapeSize);
    Sensision.update(SensisionConstants.SENSISION_CLASS_GEODIR_TIME_US, labels, (System.nanoTime() - nano) / 1000);
    
    List<Metadata> results = new ArrayList<Metadata>();

    for (String id: filtered) {
      results.add(unfiltered.get(id));
    }      
    
    return results;
  }
  
  @Override
  public Set<String> getDirectories() {
    return this.indices.keySet();
  }
  
  @Override
  public boolean knowsDirectory(String name) {
    return this.indices.containsKey(name);
  }
}
