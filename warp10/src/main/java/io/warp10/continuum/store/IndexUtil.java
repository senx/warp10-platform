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

package io.warp10.continuum.store;

import io.warp10.continuum.index.thrift.data.IndexComponent;
import io.warp10.continuum.index.thrift.data.IndexComponentType;
import io.warp10.continuum.index.thrift.data.IndexSpec;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.geoxp.GeoXPLib;
import com.google.common.base.Charsets;

public class IndexUtil {
  
  private static final Pattern TIME_PATTERN = Pattern.compile("^time\\(([0-9]+)\\)$");
  private static final Pattern GEO_PATTERN = Pattern.compile("^geo\\(([0-9]+)\\)$");
  private static final Pattern SUBCLASS_PATTERN = Pattern.compile("^subclass\\(([0-9]+)\\)$");
  private static final Pattern SUBLABELS_PATTERN = Pattern.compile("^sublabels\\(([^\\)]+)\\)$");
  
  public static IndexSpec toIndexSpec(String spec) {
    //
    // Split on whitespace
    //
    
    String[] tokens = spec.split("\\s+");
    
    IndexSpec indexSpec = new IndexSpec();
    
    for (String token: tokens) {
      if ("class".equals(token)) {
        IndexComponent component = new IndexComponent();
        component.setType(IndexComponentType.CLASS);
        indexSpec.addToComponents(component);
        continue;        
      }
      
      if ("labels".equals(token)) {
        IndexComponent component = new IndexComponent();
        component.setType(IndexComponentType.LABELS);
        indexSpec.addToComponents(component);
        continue;
      }
      
      Matcher m = TIME_PATTERN.matcher(token);
        
      if (m.matches()) {
        IndexComponent component = new IndexComponent();
        component.setType(IndexComponentType.TIME);
        component.setModulus(Long.valueOf(m.group(1)));
        indexSpec.addToComponents(component);
        continue;
      }
      
      m = GEO_PATTERN.matcher(token);
      
      if (m.matches()) {
        IndexComponent component = new IndexComponent();
        component.setType(IndexComponentType.GEO);
        component.setResolution(Integer.valueOf(m.group(1)));
        indexSpec.addToComponents(component);
        continue;
      }
      
      m = SUBCLASS_PATTERN.matcher(token);
      
      if (m.matches()) {
        IndexComponent component = new IndexComponent();
        component.setType(IndexComponentType.SUBCLASS);
        component.setLevels(Integer.valueOf(m.group(1)));
        indexSpec.addToComponents(component);
        continue;
      }
      
      m = SUBLABELS_PATTERN.matcher(token);
      
     if (m.matches()) {
       IndexComponent component = new IndexComponent();
       component.setType(IndexComponentType.SUBLABELS);
       
       // Split list of label names at ','
       String[] labels = m.group(1).split(",");
       
       // Add label names, URLDecoding them on the fly
       for (String label: labels) {
         try {
           component.addToLabels(URLDecoder.decode(label, "UTF-8"));
         } catch (UnsupportedEncodingException uee) {
           // Can't happen, we're using UTF-8
         }
       }
       
       indexSpec.addToComponents(component);
       continue;
     }
    }    
    
    return indexSpec;
  }
  
  public static final String toString(IndexSpec spec) {
    StringBuilder sb = new StringBuilder();
    
    for (IndexComponent component: spec.getComponents()) {
      if (sb.length() > 0) {
        sb.append(" ");
      }
      switch(component.getType()) {
        case CLASS:
          sb.append("class");
          break;
        case GEO:
          sb.append("geo(");
          sb.append(component.getResolution());
          sb.append(")");
          break;
        case LABELS:
          sb.append("labels");
          break;
        case SUBCLASS:          
          sb.append("subclass(");
          sb.append(component.getLevels());
          sb.append(")");
          break;
        case SUBLABELS:
          sb.append("sublabels(");
          // Put labels in a list so we can sort them
          List<String> sublabels = new ArrayList<String>();
          sublabels.addAll(component.getLabels());
          Collections.sort(sublabels);
          boolean first = true;
          for (String label: sublabels) {
            if (!first) {
              sb.append(",");
            }
            try {
              sb.append(URLEncoder.encode(label, "UTF-8"));
            } catch (UnsupportedEncodingException uee) {
              // Can't happen, we're using UTF-8
            }
            first = false;
          }
          sb.append(")");
          break;
        case TIME:
          sb.append("time(");
          sb.append(component.getModulus());
          sb.append(")");
          break;
        default:          
      }
    }
    
    return sb.toString();
  }
  
  public static int getKeyLength(IndexSpec spec) {
    int len = 0;
    
    for (IndexComponent component: spec.getComponents()) {
      switch (component.getType()) {
        case CLASS:
        case LABELS:
        case SUBCLASS:
        case SUBLABELS:
        case TIME:
          len += 8;
          break;
        case GEO:
          len += GeoXPLib.bytesFromGeoXPPoint(0L, component.getResolution()).length;
          break;
        case BINARY:
          len += component.getKey().length;
          break;
        default:          
      }
    }
    
    return len;
  }
  
  public static long indexId(KeyStore keystore, IndexSpec spec) {
    StringBuilder sb = new StringBuilder().append(spec.getOwner()).append(" ").append(IndexUtil.toString(spec));
    return SipHashInline.hash24_palindromic(keystore.getKey(KeyStore.SIPHASH_INDEX), sb.toString().getBytes(Charsets.UTF_8));
  }
}
