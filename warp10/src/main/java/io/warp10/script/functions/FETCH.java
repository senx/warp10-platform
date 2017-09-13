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

package io.warp10.script.functions;

import io.warp10.WarpDist;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.geo.GeoDirectoryClient;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.MetaSet;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.sensision.Sensision;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.geoxp.GeoXPLib.GeoXPShape;
import com.google.common.base.Charsets;

/**
 * Fetch GeoTimeSeries from continuum
 * FIXME(hbs): we need to retrieve an OAuth token, where do we put it?
 *
 * The top of the stack must contain a list of the following parameters
 * 
 * @param token The OAuth 2.0 token to use for data retrieval
 * @param classSelector  Class selector.
 * @param labelsSelectors Map of label name to label selector.
 * @param now Most recent timestamp to consider (in us since the Epoch)
 * @param timespan Width of time period to consider (in us). Timestamps at or before now - timespan will be ignored.
 * 
 * The last two parameters can be replaced by String parameters representing the end and start ISO8601 timestamps
 */
public class FETCH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String PARAM_CLASS = "class";
  
  /**
   * Extra classes to retrieve after Directory/GeoDirectory have been called
   */
  private static final String PARAM_EXTRA = "extra";
  private static final String PARAM_LABELS = "labels";
  private static final String PARAM_SELECTOR = "selector";
  private static final String PARAM_SELECTORS = "selectors";
  private static final String PARAM_SELECTOR_PAIRS = "selpairs";
  private static final String PARAM_TOKEN = "token";
  private static final String PARAM_END = "end";
  private static final String PARAM_START = "start";
  private static final String PARAM_COUNT = "count";
  private static final String PARAM_TIMESPAN = "timespan";
  private static final String PARAM_TYPE = "type";
  private static final String PARAM_GEO = "geo";
  private static final String PARAM_GEODIR = "geodir";
  private static final String PARAM_GEOOP = "geoop";
  private static final String PARAM_GEOOP_IN = "in";
  private static final String PARAM_GEOOP_OUT = "out";
  private static final String PARAM_WRITE_TIMESTAMP = "wtimestamp";
  private static final String PARAM_SHOWUUID = "showuuid";
  private static final String PARAM_TYPEATTR = "typeattr";
  private static final String PARAM_METASET = "metaset";
  
  public static final String POSTFETCH_HOOK = "postfetch";
  
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();
  
  private WarpScriptStackFunction listTo = new LISTTO("");
  
  private final boolean fromArchive;
  
  private final TYPE forcedType;
  
  private final long[] SIPHASH_CLASS;
  private final long[] SIPHASH_LABELS;

  private final byte[] AES_METASET;
  
  public FETCH(String name, boolean fromArchive, TYPE type) {
    super(name);
    this.fromArchive = fromArchive;
    this.forcedType = type;
    KeyStore ks = null;
    
    try {
      ks = WarpDist.getKeyStore();
    } catch (Throwable t) {
      // Catch NoClassDefFound
    }
    
    if (null != ks) {
      this.SIPHASH_CLASS = SipHashInline.getKey(ks.getKey(KeyStore.SIPHASH_CLASS));
      this.SIPHASH_LABELS = SipHashInline.getKey(ks.getKey(KeyStore.SIPHASH_LABELS));
      this.AES_METASET = ks.getKey(KeyStore.AES_METASETS);
    } else {
      this.SIPHASH_CLASS = null;
      this.SIPHASH_LABELS = null;
      this.AES_METASET = null;
    }    
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    //
    // Extract parameters from the stack
    //

    Object top = stack.peek();
    
    //
    // Handle the new (as of 20150805) parameter passing mechanism as a map
    //
    
    Map<String,Object> params = null;
    
    if (top instanceof Map) {
      stack.pop();
      params = paramsFromMap(stack, (Map<String,Object>) top);
    }
    
    if (top instanceof List) {      
      if (5 != ((List) top).size()) {
        stack.drop();
        throw new WarpScriptException(getName() + " expects 5 parameters.");
      }

      //
      // Explode list and remove its size
      //
      
      listTo.apply(stack);
      stack.drop();
    }
  
    if (null == params) {
      
      params = new HashMap<String, Object>();
      
      //
      // Extract time span
      //
    
      Object oStop = stack.pop();
      Object oStart = stack.pop();
      
      long endts;
      long timespan;
      
      if (oStart instanceof String && oStop instanceof String) {
        long start = fmt.parseDateTime((String) oStart).getMillis() * Constants.TIME_UNITS_PER_MS;
        long stop = fmt.parseDateTime((String) oStop).getMillis() * Constants.TIME_UNITS_PER_MS;
        
        if (start < stop) {
          endts = stop;
          timespan = stop - start;
        } else {
          endts = start;
          timespan = start - stop;
        }
      } else if (oStart instanceof Long && oStop instanceof Long) {
        endts = (long) oStart;
        timespan = (long) oStop;       
      } else {
        throw new WarpScriptException("Invalid timespan specification.");
      }

      params.put(PARAM_END, endts);
      
      if (timespan < 0) {
        params.put(PARAM_COUNT, -timespan);
      } else {
        params.put(PARAM_TIMESPAN, timespan);
      }
      
      //
      // Extract labels selector
      //
      
      Object oLabelsSelector = stack.pop();
      
      if (!(oLabelsSelector instanceof Map)) {
        throw new WarpScriptException("Label selectors must be a map.");
      }
      
      Map<String,String> labelSelectors = new HashMap<String,String>((Map<String,String>) oLabelsSelector);
      
      params.put(PARAM_LABELS, labelSelectors);
      
      //
      // Extract class selector
      //
      
      Object oClassSelector = stack.pop();

      if (!(oClassSelector instanceof String)) {
        throw new WarpScriptException("Class selector must be a string.");
      }
      
      String classSelector = (String) oClassSelector;

      params.put(PARAM_CLASS, classSelector);
      
      //
      // Extract token
      //
      
      Object oToken = stack.pop();
      
      if (!(oToken instanceof String)) {
        throw new WarpScriptException("Token must be a string.");
      }
      
      String token = (String) oToken;
      
      params.put(PARAM_TOKEN, token);
    }
    
    StoreClient gtsStore = stack.getStoreClient();
    
    DirectoryClient directoryClient = stack.getDirectoryClient();
    
    GeoTimeSerie base = null;
    GeoTimeSerie[] bases = null;
    String typelabel = (String) params.get(PARAM_TYPEATTR);

    if (null != typelabel) {
      bases = new GeoTimeSerie[4];
    }
    
    ReadToken rtoken = Tokens.extractReadToken(params.get(PARAM_TOKEN).toString());

    List<String> clsSels = new ArrayList<String>();
    List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
    
    MetaSet metaset = null;
    
    List<Metadata> metadatas = null;
    Iterator<Metadata> iter = null;

    if (params.containsKey(PARAM_METASET)) {
      metaset = (MetaSet) params.get(PARAM_METASET);
      
      iter = metaset.getMetadatas().iterator();
    } else {
      if (params.containsKey(PARAM_SELECTOR_PAIRS)) {
        for (Pair<Object,Object> pair: (List<Pair<Object,Object>>) params.get(PARAM_SELECTOR_PAIRS)) {
          clsSels.add(pair.getLeft().toString());
          Map<String,String> labelSelectors = (Map<String,String>) pair.getRight();
          labelSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));
          lblsSels.add((Map<String,String>) labelSelectors);
        }
      } else {
        Map<String,String> labelSelectors = (Map<String,String>) params.get(PARAM_LABELS);
        labelSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));
        clsSels.add(params.get(PARAM_CLASS).toString());
        lblsSels.add(labelSelectors);
      }      
            
      try {
        metadatas = directoryClient.find(clsSels, lblsSels);
        iter = metadatas.iterator();
      } catch (IOException ioe) {
        try {
          iter = directoryClient.iterator(clsSels, lblsSels);
        } catch (Exception e) {
          throw new WarpScriptException(e);
        }
      }      
    }
       
    metadatas = new ArrayList<Metadata>();

    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();    
    AtomicLong fetched = (AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_FETCH_COUNT);    
    long fetchLimit = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_FETCH_LIMIT);
    long gtsLimit = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_GTS_LIMIT);

    AtomicLong gtscount = (AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_GTS_COUNT);    
    
    // Variables to keep track of the last Metadata and fetched count
    Metadata lastMetadata = null;
    long lastCount = 0L;
    
    try {
      while(iter.hasNext()) {
        
        metadatas.add(iter.next());
              
        if (gtscount.incrementAndGet() > gtsLimit) {
          throw new WarpScriptException(getName() + " exceeded limit of " + gtsLimit + " Geo Time Series, current count is " + gtscount);
        }
        
        if (metadatas.size() < EgressFetchHandler.FETCH_BATCHSIZE && iter.hasNext()) {
          continue;
        }
        
        //
        // Filter the retrieved Metadata according to geo
        //
        
        if (params.containsKey(PARAM_GEO)) {
          GeoDirectoryClient geoclient = stack.getGeoDirectoryClient();
          long end = (long) params.get(PARAM_END);
          long start = Long.MIN_VALUE;
          if (params.containsKey(PARAM_TIMESPAN)) {
            start = end - (long) params.get(PARAM_TIMESPAN);
          }
          
          boolean inside = false;
          
          if (PARAM_GEOOP_IN.equals(params.get(PARAM_GEOOP))) {
            inside = true;
          }
          
          try {
            metadatas = geoclient.filter((String) params.get(PARAM_GEODIR), metadatas, (GeoXPShape) params.get(PARAM_GEO), inside, start, end);
          } catch (IOException ioe) {
            throw new WarpScriptException(ioe);
          }
        }
        
        //
        // Generate extra Metadata if PARAM_EXTRA is set
        //
        
        if (params.containsKey(PARAM_EXTRA)) {
          
          Set<Metadata> withextra = new HashSet<Metadata>();
          
          withextra.addAll(metadatas);
          
          for (Metadata meta: metadatas) {
            for (String cls: (Set<String>) params.get(PARAM_EXTRA)) {
              // The following is safe, the constructor allocates new maps
              Metadata metadata = new Metadata(meta);
              metadata.setName(cls);
              metadata.setClassId(GTSHelper.classId(this.SIPHASH_CLASS, cls));
              metadata.setLabelsId(GTSHelper.labelsId(this.SIPHASH_LABELS, metadata.getLabels()));
              withextra.add(metadata);
            }
          }
          
          metadatas.clear();
          metadatas.addAll(withextra);
        }
        
        //
        // We assume that GTS will be fetched in a continuous way, i.e. without having a GTSDecoder from one
        // then one from another, then one from the first one.
        //
              
        long timespan = params.containsKey(PARAM_TIMESPAN) ? (long) params.get(PARAM_TIMESPAN) : - ((long) params.get(PARAM_COUNT));
        
        TYPE type = (TYPE) params.get(PARAM_TYPE);

        if (null != this.forcedType) {
          if (null != type) {
            throw new WarpScriptException(getName() + " type of fetched GTS cannot be changed.");
          }
          type = this.forcedType;
        }
        
        boolean writeTimestamp = Boolean.TRUE.equals(params.get(PARAM_WRITE_TIMESTAMP));
        
        boolean showUUID = Boolean.TRUE.equals(params.get(PARAM_SHOWUUID));
        
        
        try (GTSDecoderIterator gtsiter = gtsStore.fetch(rtoken, metadatas, (long) params.get(PARAM_END), timespan, fromArchive, writeTimestamp)) {
          while(gtsiter.hasNext()) {
            GTSDecoder decoder = gtsiter.next();
            
            boolean identical = true;
            
            if (null == lastMetadata || !lastMetadata.equals(decoder.getMetadata())) {
              lastMetadata = decoder.getMetadata();
              identical = false;
              lastCount = 0;
            }
                         
            GeoTimeSerie gts;
            
            //
            // If we should ventilate per type, do so now
            //
            
            if (null != typelabel) {
              
              Map<String,String> labels = new HashMap<String,String>(decoder.getMetadata().getLabels());
              labels.remove(Constants.PRODUCER_LABEL);
              labels.remove(Constants.OWNER_LABEL);

              java.util.UUID uuid = null;
              
              if (showUUID) {
                uuid = new java.util.UUID(decoder.getClassId(), decoder.getLabelsId());
              }

              long count = 0;
              
              Metadata decoderMeta = decoder.getMetadata();
              
              while(decoder.next()) {
                
                // If we've read enough data, exit
                if (identical && timespan < 0 && lastCount + count >= -timespan) {
                  break;
                }
                
                count++;
                long ts = decoder.getTimestamp();
                long location = decoder.getLocation();
                long elevation = decoder.getElevation();
                Object value = decoder.getValue();
                
                int gtsidx = 0;
                String typename = "DOUBLE";
                
                if (value instanceof Long) {
                  gtsidx = 1;
                  typename = "LONG";
                } else if (value instanceof Boolean) {
                  gtsidx = 2;
                  typename = "BOOLEAN";
                } else if (value instanceof String) {
                  gtsidx = 3;
                  typename = "STRING";
                }
                
                base = bases[gtsidx];
                
                if (null == base || !base.getMetadata().getName().equals(decoderMeta.getName()) || !base.getMetadata().getLabels().equals(decoderMeta.getLabels())) {
                  bases[gtsidx] = new GeoTimeSerie();
                  base = bases[gtsidx];
                  series.add(base);
                  base.setLabels(decoder.getLabels());
                  base.getMetadata().putToAttributes(typelabel, typename);
                  base.setName(decoder.getName());
                  if (null != uuid) {
                    base.getMetadata().putToAttributes(Constants.UUID_ATTRIBUTE, uuid.toString());
                  }
                }
                
                GTSHelper.setValue(base, ts, location, elevation, value, false);                
              }
              
              if (fetched.addAndGet(count) > fetchLimit) {
                Map<String,String> sensisionLabels = new HashMap<String, String>();
                sensisionLabels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, Tokens.getUUID(rtoken.getBilledId()));
                Sensision.update(SensisionConstants.SENSISION_CLASS_EINSTEIN_FETCHCOUNT_EXCEEDED, sensisionLabels, 1);
                throw new WarpScriptException(getName() + " exceeded limit of " + fetchLimit + " datapoints, current count is " + fetched.get());
              }

              lastCount += count;
              
              continue;
            }
            
            if (null != type) {
              gts = decoder.decode(type);
            } else {
              gts = decoder.decode();
            }
        
            if (identical && timespan < 0 && lastCount + GTSHelper.nvalues(gts) > -timespan) {
              // We would add too many datapoints, we will shrink the GTS.
              // As it it sorted in reverse order of the ticks (since the datapoints are organized
              // this way in HBase), we just need to shrink the GTS.
              gts = GTSHelper.shrinkTo(gts, (int) Math.max(-timespan - lastCount, 0));
            }
            
            lastCount += GTSHelper.nvalues(gts);
            
            //
            // Remove producer/owner labels
            //
        
            //
            // Add a .uuid attribute if instructed to do so
            //
            
            if (showUUID) {
              java.util.UUID uuid = new java.util.UUID(gts.getClassId(), gts.getLabelsId());
              gts.getMetadata().putToAttributes(Constants.UUID_ATTRIBUTE, uuid.toString());
            }
            
            Map<String,String> labels = new HashMap<String, String>();
            labels.putAll(gts.getMetadata().getLabels());
            labels.remove(Constants.PRODUCER_LABEL);
            labels.remove(Constants.OWNER_LABEL);
            gts.setLabels(labels);
            
            //
            // If it's the first GTS, take it as is.
            //
            
            if (null == base) {
              base = gts;
            } else {
              //
              // If name and labels are identical to the previous GTS, merge them
              // Otherwise add 'base' to the stack and set it to 'gts'.
              //
              if (!base.getMetadata().getName().equals(gts.getMetadata().getName()) || !base.getMetadata().getLabels().equals(gts.getMetadata().getLabels())) {
                series.add(base);
                base = gts;
              } else {
                base = GTSHelper.merge(base, gts);
              }
            }
            
            if (fetched.addAndGet(gts.size()) > fetchLimit) {
              Map<String,String> sensisionLabels = new HashMap<String, String>();
              sensisionLabels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, Tokens.getUUID(rtoken.getBilledId()));
              Sensision.update(SensisionConstants.SENSISION_CLASS_EINSTEIN_FETCHCOUNT_EXCEEDED, sensisionLabels, 1);
              throw new WarpScriptException(getName() + " exceeded limit of " + fetchLimit + " datapoints, current count is " + fetched.get());
              //break;
            }
          }      
        } catch (WarpScriptException ee) {
          throw ee;
        } catch (Throwable t) {          
          throw new WarpScriptException(t);
        }
        
        
        //
        // If there is one current GTS, push it onto the stack (only if not ventilating per type)
        //
        
        if (null != base && null == typelabel) {
          series.add(base);
        }     
        
        //
        // Reset state
        //
        
        base = null;
        metadatas.clear();
      }      
    } catch (Throwable t) {
      throw t;
    } finally {
      if (iter instanceof MetadataIterator) {
        try {
          ((MetadataIterator) iter).close();
        } catch (Exception e) {        
        }
      }
    }
           
    stack.push(series);
    
    //
    // Apply a possible postfetch hook
    //
    
    if (rtoken.getHooksSize() > 0 && rtoken.getHooks().containsKey(POSTFETCH_HOOK)) {
      stack.execMulti(rtoken.getHooks().get(POSTFETCH_HOOK));
    }
    
    return stack;
  }
  
  private Map<String,Object> paramsFromMap(WarpScriptStack stack, Map<String,Object> map) throws WarpScriptException {
    Map<String,Object> params = new HashMap<String, Object>();
    
    //
    // Handle the case where a MetaSet was passed as this will
    // modify some other parameters
    //
    
    MetaSet metaset = null;
    
    if (map.containsKey(PARAM_METASET)) {
      
      if (null == AES_METASET) {
        throw new WarpScriptException(getName() + " MetaSet support not available.");
      }
      
      Object ms = map.get(PARAM_METASET);
      
      if (!(ms instanceof byte[])) {
        // Decode
        byte[] decoded = OrderPreservingBase64.decode(ms.toString().getBytes(Charsets.US_ASCII));
        
        // Decrypt
        byte[] decrypted = CryptoUtils.unwrap(AES_METASET, decoded);
        
        // Decompress
        
        try {
          ByteArrayOutputStream out = new ByteArrayOutputStream(decrypted.length);
          InputStream in = new GZIPInputStream(new ByteArrayInputStream(decrypted));
          
          byte[] buf = new byte[1024];
          
          while(true) {
            int len = in.read(buf);
            if (len < 0) {
              break;
            }
            out.write(buf, 0, len);
          }
          
          in.close();
          out.close();
          
          ms = out.toByteArray();          
        } catch (IOException e) {
          throw new WarpScriptException(getName() + " encountered an invalid MetaSet.");
        }                
      }
      
      metaset = new MetaSet();
      TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
      
      try {
        deser.deserialize(metaset, (byte[]) ms);
      } catch (TException te) {
        throw new WarpScriptException(getName() + " was unable to decode the provided MetaSet.");
      }

      //
      // Check if MetaSet has expired
      //
      
      if (metaset.getExpiry() < System.currentTimeMillis()) {
        throw new WarpScriptException(getName() + " MetaSet has expired.");
      }
      
      // Attempt to extract token, this will raise an exception if token has expired or was revoked
      ReadToken rtoken = Tokens.extractReadToken(metaset.getToken());
      
      params.put(PARAM_METASET, metaset);      
      params.put(PARAM_TOKEN, metaset.getToken());
    }
    
    if (!params.containsKey(PARAM_TOKEN)) {
      if (!map.containsKey(PARAM_TOKEN)) {
        throw new WarpScriptException(getName() + " Missing '" + PARAM_TOKEN + "' parameter");
      }
      
      params.put(PARAM_TOKEN, map.get(PARAM_TOKEN));      
    }
    
    if (map.containsKey(PARAM_SELECTORS)) {
      Object sels = map.get(PARAM_SELECTORS);
      if (!(sels instanceof List)) {
        throw new WarpScriptException(getName() + " Invalid parameter '" + PARAM_SELECTORS + "'");
      }
      List<Pair<Object, Object>> selectors = new ArrayList<Pair<Object,Object>>();
      
      for (Object sel: (List) sels) {
        Object[] clslbls = PARSESELECTOR.parse(sel.toString());
        selectors.add(Pair.of(clslbls[0], clslbls[1]));
      }
      params.put(PARAM_SELECTOR_PAIRS, selectors);
    } else if (map.containsKey(PARAM_SELECTOR)) {
      Object[] clslbls = PARSESELECTOR.parse(map.get(PARAM_SELECTOR).toString());
      params.put(PARAM_CLASS, clslbls[0]);
      params.put(PARAM_LABELS, clslbls[1]);
    } else if (map.containsKey(PARAM_CLASS) && map.containsKey(PARAM_LABELS)) {
      params.put(PARAM_CLASS, map.get(PARAM_CLASS));
      params.put(PARAM_LABELS, new HashMap<String,String>((Map<String,String>) map.get(PARAM_LABELS)));
    } else if (!params.containsKey(PARAM_METASET)) {
      throw new WarpScriptException(getName() + " Missing '" + PARAM_SELECTOR + "', '" + PARAM_SELECTORS + "' or '" + PARAM_CLASS + "' and '" + PARAM_LABELS + "' parameters.");
    }
    
    if (!map.containsKey(PARAM_END)) {
      throw new WarpScriptException(getName() + " Missing '" + PARAM_END + "' parameter.");
    }
    
    if (map.get(PARAM_END) instanceof Long) {
      params.put(PARAM_END, map.get(PARAM_END));      
    } else if (map.get(PARAM_END) instanceof String) {
      params.put(PARAM_END, fmt.parseDateTime(map.get(PARAM_END).toString()).getMillis() * Constants.TIME_UNITS_PER_MS);      
    } else {
      throw new WarpScriptException(getName() + " Invalid format for parameter '" + PARAM_END + "'.");
    }
    
    if (map.containsKey(PARAM_TIMESPAN)) {
      params.put(PARAM_TIMESPAN, (long) map.get(PARAM_TIMESPAN));
    } else if (map.containsKey(PARAM_COUNT)) {
      params.put(PARAM_COUNT, (long) map.get(PARAM_COUNT));
    } else if (map.containsKey(PARAM_START)) {
      long end = (long) params.get(PARAM_END);
      long start;
      
      if (map.get(PARAM_START) instanceof Long) {
        start = (long) map.get(PARAM_START);
      } else {
        start = fmt.parseDateTime(map.get(PARAM_END).toString()).getMillis() * Constants.TIME_UNITS_PER_MS;
      }
      
      long timespan;
      
      if (start < end) {
        timespan = end - start;
      } else {
        timespan = start - end;
        end = start;
      }
      
      params.put(PARAM_END, end);
      params.put(PARAM_TIMESPAN, timespan);
    } else {
      throw new WarpScriptException(getName() + " Missing parameter '" + PARAM_TIMESPAN + "' or '" + PARAM_COUNT + "' or '" + PARAM_START + "'");
    }

    //
    // Check end/timespan against MetaSet, adjust limits accordingly
    //
    
    if (null != metaset) {
      
      long end = (long) params.get(PARAM_END);
      long timespan = params.containsKey(PARAM_TIMESPAN) ? (long) params.get(PARAM_TIMESPAN) : -1;
      long count = params.containsKey(PARAM_COUNT) ? (long) params.get(PARAM_COUNT) : -1;

      if (metaset.isSetMaxduration()) {
        // Force 'end' to 'now'
        params.put(PARAM_END, TimeSource.getTime());
        
        if (-1 != count && metaset.getMaxduration() >= 0) {
          throw new WarpScriptException(getName() + " MetaSet forbids count based requests.");
        }
        
        if (-1 != timespan && metaset.getMaxduration() <= 0) {
          throw new WarpScriptException(getName() + " MetaSet forbids duration based requests.");
        }
        
        if (-1 != count && count > -metaset.getMaxduration()) {
          count = -metaset.getMaxduration();
          params.put(PARAM_COUNT, count);
        }
        
        if (-1 != timespan && timespan > metaset.getMaxduration()) {
          timespan = metaset.getMaxduration();
          params.put(PARAM_TIMESPAN, timespan);
        }
      }

      if (metaset.isSetNotbefore()) {
        // forbid count based requests
        if (-1 != count) {
          throw new WarpScriptException(getName() + " MetaSet forbids count based requests.");
        }
        
        if (end < metaset.getNotbefore()) {
          throw new WarpScriptException(getName() + " MetaSet forbids time ranges before " + metaset.getNotbefore());
        }
        
        // Adjust timespan so maxDuration is respected
        if (timespan > metaset.getMaxduration()) {
          timespan = metaset.getMaxduration();
          params.put(PARAM_TIMESPAN, timespan);
        }
      }
      
      if (metaset.isSetNotafter() && end >= metaset.getNotafter()) {
        end = metaset.getNotafter();
        params.put(PARAM_END, end);
      }
    }

    if (map.containsKey(PARAM_GEO)) {      
      if (!(map.get(PARAM_GEO) instanceof GeoXPShape)) {
        throw new WarpScriptException(getName() + " Invalid '" + PARAM_GEO + "' type.");
      }
      
      if (!map.containsKey(PARAM_GEODIR)) {
        throw new WarpScriptException(getName() + " Missing '" + PARAM_GEODIR + "' parameter.");
      }
      
      if (!stack.getGeoDirectoryClient().knowsDirectory(map.get(PARAM_GEODIR).toString())) {
        throw new WarpScriptException(getName() + " Unknwon directory '" + map.get(PARAM_GEODIR) + "' for parameter '" + PARAM_GEODIR + "'.");        
      }
      
      params.put(PARAM_GEODIR, map.get(PARAM_GEODIR));
      params.put(PARAM_GEO, map.get(PARAM_GEO));
      
      if (map.containsKey(PARAM_GEOOP)) {
        if (PARAM_GEOOP_IN.equals(map.get(PARAM_GEOOP))) {
          params.put(PARAM_GEOOP, PARAM_GEOOP_IN);
        } else if (PARAM_GEOOP_OUT.equals(map.get(PARAM_GEOOP))) {
          params.put(PARAM_GEOOP, PARAM_GEOOP_OUT);
        } else {
          throw new WarpScriptException(getName() + " Invalid value for parameter '" + PARAM_GEOOP + "'");
        }
      } else {
        params.put(PARAM_GEOOP, PARAM_GEOOP_IN);
      }
    }
    
    if (map.containsKey(PARAM_TYPE)) {
      String type = map.get(PARAM_TYPE).toString();
      
      if (TYPE.LONG.name().equalsIgnoreCase(type)) {
        params.put(PARAM_TYPE, TYPE.LONG);
      } else if (TYPE.DOUBLE.name().equalsIgnoreCase(type)) {
        params.put(PARAM_TYPE, TYPE.DOUBLE);
      } else if (TYPE.STRING.name().equalsIgnoreCase(type)) {
        params.put(PARAM_TYPE, TYPE.STRING);
      } else if (TYPE.BOOLEAN.name().equalsIgnoreCase(type)) {
        params.put(PARAM_TYPE, TYPE.BOOLEAN);
      } else {
        throw new WarpScriptException(getName() + " Invalid value for parameter '" + PARAM_TYPE + "'.");
      }
    }

    if (map.containsKey(PARAM_TYPEATTR)) {
      if (map.containsKey(PARAM_TYPE)) {
        throw new WarpScriptException(getName() + " Incompatible parameters '" +  PARAM_TYPE + "' and '" + PARAM_TYPEATTR + "'.");
      }
      
      params.put(PARAM_TYPEATTR, map.get(PARAM_TYPEATTR).toString());
    }
    
    if (map.containsKey(PARAM_EXTRA)) {
      // Check that we are not using a MetaSet
      if (params.containsKey(PARAM_METASET)) {
        throw new WarpScriptException(getName() + " Cannot specify '" + PARAM_EXTRA + "' when '" + PARAM_METASET + "' is used.");
      }
      
      if (!(map.get(PARAM_EXTRA) instanceof List)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_EXTRA + "'.");
      }

      Set<String> extra = new HashSet<String>();
      
      for (Object o: (List) map.get(PARAM_EXTRA)) {
        if (!(o instanceof String)) {
          throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_EXTRA + "'.");
        }
        extra.add(o.toString());
      }
      
      params.put(PARAM_EXTRA, extra);
    }
    
    if (map.containsKey(PARAM_WRITE_TIMESTAMP)) {
      params.put(PARAM_WRITE_TIMESTAMP, Boolean.TRUE.equals(map.get(PARAM_WRITE_TIMESTAMP)));
    }
    
    return params;
  }
}
