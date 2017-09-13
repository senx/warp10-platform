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
import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.MetaSet;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Charsets;

/**
 * Find Geo Time Series matching some criteria
 *
 * The top of the stack must contain a list of the following parameters
 * 
 * @param token The token to use for data retrieval
 * @param classSelector  Class selector.
 * @param labelsSelectors Map of label name to label selector.
 */
public class FIND extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private WarpScriptStackFunction toList = new TOLIST("");
  private WarpScriptStackFunction listTo = new LISTTO("");
  
  /**
   * Flag indicating whether we want to include the detail of GTS or only the
   * elements (class / labels) values
   */
  private final boolean elements;
  
  /**
   * Flag indicating if we want to build a MetaSet
   */
  private final boolean metaset;
  
  private byte[] METASETS_KEY;
  
  public FIND(String name, boolean elements) {
    super(name);
    this.elements = elements;
    this.metaset = false;
    this.METASETS_KEY = null;
  }
  
  public FIND(String name, boolean elements, boolean metaset) {
    super(name);
    
    if (elements && metaset) {
      throw new RuntimeException("Invalid parameter combination.");
    }

    this.elements = false;
    this.metaset = metaset;    
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (this.metaset && null == this.METASETS_KEY) {
      synchronized(FIND.class) {
        this.METASETS_KEY = WarpDist.getKeyStore().getKey(KeyStore.AES_METASETS);      
      }
    }
    
    if (this.metaset && null == this.METASETS_KEY) {
      throw new WarpScriptException(getName() + " is disabled, as no key is set in '" + Configuration.WARP_AES_METASETS + "'.");
    }
    
    //
    // Extract parameters from the stack
    //

    Object top = stack.peek();
  
    boolean hasUUIDFlag = false;
    
    if (top instanceof List) {
      
      if (!this.metaset) {
        if (3 != ((List) top).size() && 4 != ((List) top).size()) {
          stack.drop();
          throw new WarpScriptException(getName() + " expects 3 or 4 parameters.");
        }

        //
        // Explode list and remove its size
        //
        
        listTo.apply(stack);
        Object n = stack.pop();
        
        hasUUIDFlag = 4 == ((Number) n).intValue() ? Boolean.TRUE.equals(stack.pop()) : false;        
      } else {
        
        if (7 != ((List) top).size()) {
          throw new WarpScriptException(getName() + " expects 7 parameters.");
        }
        
        //
        // Explode list and remove its size
        //
        
        listTo.apply(stack);
        Object n = stack.pop();        
      }
    }
    
    MetaSet set = null;
    
    if (this.metaset) {
      
      set = new MetaSet();
      
      top = stack.pop();
      
      if (!(top instanceof Long)) {
        throw new WarpScriptException(getName() + " expects and expiration timestamp on top of the stack.");
      }
      
      set.setExpiry(System.currentTimeMillis() + (((long) top) / Constants.TIME_UNITS_PER_MS));
      
      top = stack.pop();
      
      if (!(top instanceof Long) && !(top instanceof Double && Double.isNaN((double) top))) {
        throw new WarpScriptException(getName() + " expects a maximum duration or NaN below the expiration.");      
      }
      
      if (top instanceof Long) {
        set.setMaxduration((long) top);
      }
      
      top = stack.pop();
      
      if (!(top instanceof Long) && !(top instanceof Double && Double.isNaN((double) top))) {
        throw new WarpScriptException(getName() + " expects a 'notafter' parameter below the maximum duration.");      
      }
      
      if (top instanceof Long) {
        set.setNotafter((long) top);
      }
      
      top = stack.pop();
      
      if (!(top instanceof Long) && !(top instanceof Double && Double.isNaN((double) top))) {
        throw new WarpScriptException(getName() + " expects a 'notbefore' parameter below 'notafter'.");      
      }
      
      if (top instanceof Long) {
        set.setNotafter((long) top);
      }
    }
    
    //
    // Extract labels selector
    //
    
    Object oLabelsSelector = stack.pop();
    
    if (!(oLabelsSelector instanceof Map)) {
      throw new WarpScriptException("Label selectors must be a map.");
    }
    
    Map<String,String> labelSelectors = new HashMap<String,String>((Map<String,String>) oLabelsSelector);

    //
    // Extract class selector
    //
    
    Object oClassSelector = stack.pop();

    if (!(oClassSelector instanceof String)) {
      throw new WarpScriptException("Class selector must be a string.");
    }
    
    String classSelector = (String) oClassSelector;

    //
    // Extract token
    //
    
    Object oToken = stack.pop();
    
    if (!(oToken instanceof String)) {
      throw new WarpScriptException("Token must be a string.");
    }
    
    String token = (String) oToken;
    
    DirectoryClient directoryClient = stack.getDirectoryClient();
    
    ReadToken rtoken = Tokens.extractReadToken(token);

    labelSelectors.remove(Constants.PRODUCER_LABEL);
    labelSelectors.remove(Constants.OWNER_LABEL);
    labelSelectors.remove(Constants.APPLICATION_LABEL);
    labelSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));
    
    List<String> clsSels = new ArrayList<String>();
    List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
    
    clsSels.add(classSelector);
    lblsSels.add(labelSelectors);

    List<Metadata> metadatas = null;

    if (this.metaset) {
      set.setToken(token);      
    }
    
    Iterator<Metadata> iter = null;
    
//    try {
//      metadatas = directoryClient.find(clsSels, lblsSels);
//      iter = metadatas.iterator();
//    } catch (IOException ioe) {
//  try {
//  iter = directoryClient.iterator(clsSels, lblsSels);
//} catch (Exception e) {
//  throw new EinsteinException(e);
//}
//    }
    try {
      iter = directoryClient.iterator(clsSels, lblsSels);
    } catch (Exception e) {
      throw new WarpScriptException(e);
    }

    long gtsLimit = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_GTS_LIMIT);

    AtomicLong gtscount = (AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_GTS_COUNT);    
    
    List<GeoTimeSerie> series = null;
    Set<String> classes = null;
    Map<String,Set<String>> labels = null;
    Map<String,Set<String>> attributes = null;
    
    if (!elements) {
      if (!this.metaset) {
        series = new ArrayList<GeoTimeSerie>();
      }
    } else {
      classes = new HashSet<String>();
      labels = new HashMap<String, Set<String>>();
      attributes = new HashMap<String, Set<String>>();
    }
    
    try {
      while(iter.hasNext()) {
        Metadata metadata = iter.next();
        
        if (elements) {
          classes.add(metadata.getName());
          
          if (metadata.getLabelsSize() > 0) {
            for (Entry<String,String> entry: metadata.getLabels().entrySet()) {
              Set<String> values = labels.get(entry.getKey());
              if (null == values) {
                values = new HashSet<String>();
                labels.put(entry.getKey(), values);
              }
              values.add(entry.getValue());
            }
          }
          
          if (metadata.getAttributesSize() > 0) {
            for (Entry<String,String> entry: metadata.getAttributes().entrySet()) {
              Set<String> values = attributes.get(entry.getKey());
              if (null == values) {
                values = new HashSet<String>();
                attributes.put(entry.getKey(), values);
              }
              values.add(entry.getValue());
            }
          }

          continue;
        }
        
        if (gtscount.incrementAndGet() > gtsLimit) {
          throw new WarpScriptException(getName() + " exceeded limit of " + gtsLimit + " Geo Time Series, current count is " + gtscount.get());
        }

        GeoTimeSerie gts = new GeoTimeSerie();
        
        // Use safeSetMetadata since the Metadata were newly created by 'find'
        gts.safeSetMetadata(metadata);

        //
        // Add a .uuid attribute if instructed to do so
        //
        
        if (hasUUIDFlag) {
          java.util.UUID uuid = new java.util.UUID(gts.getClassId(), gts.getLabelsId());
          gts.getMetadata().putToAttributes(Constants.UUID_ATTRIBUTE, uuid.toString());
        }
        
        //
        // Remove producer/owner labels
        //
        
        if (!this.metaset) {
          Map<String,String> gtslabels = new HashMap<String, String>();
          gtslabels.putAll(gts.getLabels());
          gtslabels.remove(Constants.PRODUCER_LABEL);
          gtslabels.remove(Constants.OWNER_LABEL);
          gts.setLabels(gtslabels);          
          series.add(gts);
        } else {
          set.addToMetadatas(gts.getMetadata());
        }      
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
    
    //
    // Put all 'count' GTS into a list
    //
    
    if (!elements) {
      if (!this.metaset) {
        stack.push(series);
      } else {
        //
        // Encode the MetaSet
        //
        
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        
        try {
          byte[] serialized = serializer.serialize(set);
          
          // Compress the serialized content
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          GZIPOutputStream out = new GZIPOutputStream(baos);
          out.write(serialized);
          out.close();
          
          byte[] compressed = baos.toByteArray();
          
          // Now encrypt the content          
          byte[] wrapped = CryptoUtils.wrap(METASETS_KEY, compressed);
          
          // Encode it and push it on the stack
          stack.push(new String(OrderPreservingBase64.encode(wrapped), Charsets.UTF_8));
        } catch (TException | IOException e) {
          throw new WarpScriptException(getName() + " unable to build MetaSet.");
        }
      }
    } else {
      List<String> list = new ArrayList<String>();
      list.addAll(classes);
      stack.push(list);
      
      Map<String,List<String>> map = new HashMap<String,List<String>>();
      for (Entry<String,Set<String>> entry: labels.entrySet()) {
        list = new ArrayList<String>();
        list.addAll(entry.getValue());
        map.put(entry.getKey(), list);
      }
      
      stack.push(map);

      map = new HashMap<String,List<String>>();
      for (Entry<String,Set<String>> entry: attributes.entrySet()) {
        list = new ArrayList<String>();
        list.addAll(entry.getValue());
        map.put(entry.getKey(), list);
      }
      
      stack.push(map);
    }
    
    return stack;
  }  
}
