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
import io.warp10.continuum.store.Store;
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;

public class MetadataUtils {
  
  /**
   * Maximum size the labels (names + values) can occupy.
   */
  private static final int MAX_LABELS_SIZE = 2048;
  
  /**
   * Maximum size the attributes can occupy (names + values).
   */
  private static final int MAX_ATTRIBUTES_SIZE = 8192;
  
  private static final Pattern METADATA_PATTERN = Pattern.compile("^+([^\\{]+)\\{([^\\}]*)\\}\\{([^\\}]*)\\}$");
  
  public static class MetadataID {
    private long classId;
    private long labelsId;
    
    public MetadataID(Metadata metadata) {
      this.classId = metadata.getClassId();
      this.labelsId = metadata.getLabelsId();
    }
    
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof MetadataID) {
        MetadataID id = (MetadataID) obj;
        return this.classId == id.classId && this.labelsId == id.labelsId;
      } else {
        return false;
      }
    }
    
    @Override
    public int hashCode() {
      return (int) ((this.classId ^ this.labelsId) & 0xFFFFFFFFL);
    }
  }
  
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

  public static MetadataID id(Metadata meta) {
    return new MetadataID(meta);
  }
  
  public static MetadataID id(MetadataID id, Metadata meta) {
    if (null == id) {
      return id(meta);
    }
    
    id.classId = meta.getClassId();
    id.labelsId = meta.getLabelsId();
    
    return id;
  }
  
  /**
   * Compare Metadata according to class/labels Ids
   */
  public static int compare(Metadata m1, Metadata m2) {
    // 128bits
    
    //
    // Extract ids by shifting them to the right so all numbers are > 0
    //
    
    long c1 = m1.getClassId() >>> 1;    
    long c2 = m2.getClassId() >>> 1;
    
    if (c1 < c2) {
      return -1;
    }
    
    if (c1 > c2) {
      return 1;
    }
        
    // Check lower bit
        
    if ((m1.getClassId() & 0x1L) < (m2.getClassId() & 0x1L)) {
      return -1;
    }
    
    if ((m1.getClassId() & 0x1L) > (m2.getClassId() & 0x1L)) {
      return 1;
    }
    
    //
    // Classes are equal, check labels ids
    //
    
    long l1 = m1.getLabelsId() >>> 1;    
    long l2 = m2.getLabelsId() >>> 1;
    
    if (l1 < l2) {
      return -1;
    }
    
    if (l1 > l2) {
      return 1;
    }

    if ((m1.getLabelsId() & 0x1L) < (m2.getLabelsId() & 0x1L)) {
      return -1;
    }
    
    if ((m1.getLabelsId() & 0x1L) > (m2.getLabelsId() & 0x1L)) {
      return 1;
    }

    return 0;
  }
  
  public static byte[] HBaseRowKeyPrefix(Metadata meta) {
    // 128bits
    byte[] rowkey = new byte[Store.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];

    System.arraycopy(Store.HBASE_RAW_DATA_KEY_PREFIX, 0, rowkey, 0, Store.HBASE_RAW_DATA_KEY_PREFIX.length);
    // Copy classId/labelsId
    System.arraycopy(Longs.toByteArray(meta.getClassId()), 0, rowkey, Store.HBASE_RAW_DATA_KEY_PREFIX.length, 8);
    System.arraycopy(Longs.toByteArray(meta.getLabelsId()), 0, rowkey, Store.HBASE_RAW_DATA_KEY_PREFIX.length + 8, 8);
    
    return rowkey;
  }
}
