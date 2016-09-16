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

package io.warp10.continuum;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.UnsafeString;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;

public class MetadataUtils {
  
  /**
   * Maximum size the labels (names + values) can occupy.
   */
  private static final int MAX_LABELS_SIZE = 2048;
  
  /**
   * Maximum size the attributes can occupy (names + values).
   */
  private static final int MAX_ATTRIBUTES_SIZE = 8192;
  
  private static final Pattern METADATA_PATTERN = Pattern.compile("^+([^\\}]+)\\{([^\\}]*)\\}\\{([^\\}]*)\\}$");
  
  public static Metadata parseMetadata(String str) {
    Matcher m = METADATA_PATTERN.matcher(str.trim());
    
    if (!m.matches()) {
      return null;
    }
    
    try {
      String name = URLDecoder.decode(m.group(1), "UTF-8");
      
      Map<String,String> labels = GTSHelper.parseLabels(m.group(2));
      Map<String,String> attributes = GTSHelper.parseLabels(m.group(3));
      
      Metadata metadata = new Metadata();
      metadata.setName(name);
      metadata.setLabels(labels);
      metadata.setAttributes(attributes);
      
      return metadata;
    } catch (UnsupportedEncodingException uee) {
      // Can't happen since we're using UTF-8
      return null;
    } catch (ParseException pe) {
      return null;
    }        
  }
  
  /**
   * Checks Metadata for sanity. This method controls that no attribute
   * hides a label with the same name.
   * It also controls that no reserved labels exist as attributes.
   * It also checks that attribute and label sizes do not exceed a threshold.
   * 
   * @param metadata
   * @return
   */
  public static boolean validateMetadata(Metadata metadata) {
    
    if (null == metadata) {
      return false;
    }
    
    //
    // Check that the reserved labels are not overridden in attributes
    //
    
    if (null != metadata.getAttributes()) {
      if (metadata.getAttributes().containsKey(Constants.PRODUCER_LABEL)) {
        return false;
      }
      if (metadata.getAttributes().containsKey(Constants.OWNER_LABEL)) {
        return false;
      }
      if (metadata.getAttributes().containsKey(Constants.APPLICATION_LABEL)) {
        return false;
      }
    }
    
    //
    // Check that no attribute hides a label
    //
    
    if (null != metadata.getLabels() && null != metadata.getAttributes()) {
      for (String key: metadata.getAttributes().keySet()) {
        if (metadata.getLabels().containsKey(key)) {
          return false;
        }
      }
    }
    
    int total = 0;
    
    if (null != metadata.getLabels()) {
      for (Entry<String,String> entry: metadata.getLabels().entrySet()) {
        total += entry.getKey().length();
        total += entry.getValue().length();
      }
      
      if (total > MAX_LABELS_SIZE) {
        return false;
      }
    }
    
    total = 0;
    
    if (null != metadata.getAttributes()) {
      for (Entry<String,String> entry: metadata.getAttributes().entrySet()) {
        total += entry.getKey().length();
        total += entry.getValue().length();
      }
      
      if (total > MAX_ATTRIBUTES_SIZE) {
        return false;
      }
    }
    
    return true;
  }

  /**
   * Fill a String's char array with the metadata id
   * @param container
   * @param meta
   */
  public static String idString(String container, Metadata meta) {
    
    if (null == container) {
      return idString(meta);
    }
    
    char[] chars = UnsafeString.getChars(container);
    if (chars.length != 8) {
      throw new RuntimeException("Invalid container size.");
    }
    //
    // Fill the char array
    //    
    long id = meta.getLabelsId();
    int offset = 4;
    for (int i = 3; i >= 0; i--) {
      chars[offset + i] = (char) (id & 0xFFFFL);
      id >>>= 16;
    }    
    offset = 0;
    id = meta.getClassId();
    for (int i = 3; i >= 0; i--) {
      chars[offset + i] = (char) (id & 0xFFFFL);
      id >>>= 16;
    }    
    //
    // Reset hashcode
    //
    UnsafeString.resetHash(container);
    
    return container;
  }
  
  public static String idString(Metadata meta) {
    String id = new String("01234567");
    return idString(id, meta);
  }
  
  public static byte[] idBytes(String id) {
    return id.getBytes(Charsets.UTF_16);
  }
}
