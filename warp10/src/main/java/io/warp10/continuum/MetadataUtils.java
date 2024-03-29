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

package io.warp10.continuum;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.WarpConfig;
import io.warp10.WarpURLDecoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.fdb.FDBUtils;

public class MetadataUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataUtils.class);

  /**
   * Maximum size of the class name
   */
  private static final int MAX_CLASS_SIZE;

  /**
   * Maximum size the labels (names + values) can occupy.
   */
  private static final int MAX_LABELS_SIZE;

  /**
   * Maximum size the attributes can occupy (names + values).
   */
  private static final int MAX_ATTRIBUTES_SIZE;

  private static final int FDB_SIZE_RESERVED = 1024;

  /**
   * Size over which to check for metadata validity
   */
  public static final int SIZE_THRESHOLD;

  private static final Pattern METADATA_PATTERN = Pattern.compile("^+([^\\{]+)\\{([^\\}]*)\\}\\{([^\\}]*)\\}$");

  static {
    // Add provision for 2 UUIDs and the producer/owner/application label names. Actual application name will count towards the total size
    int internalLabels = 36 + 36 + Constants.PRODUCER_LABEL.length() + Constants.OWNER_LABEL.length() + Constants.APPLICATION_LABEL.length();
    MAX_CLASS_SIZE =  Math.max(0, Integer.parseInt(WarpConfig.getProperty(Configuration.WARP_CLASS_MAXSIZE, "1024")));
    MAX_LABELS_SIZE =  internalLabels + Math.max(0, Integer.parseInt(WarpConfig.getProperty(Configuration.WARP_LABELS_MAXSIZE, "2048")));
    MAX_ATTRIBUTES_SIZE = Math.max(0, Integer.parseInt(WarpConfig.getProperty(Configuration.WARP_ATTRIBUTES_MAXSIZE, "8192")));
    SIZE_THRESHOLD = Math.min(MAX_CLASS_SIZE, Math.min(MAX_LABELS_SIZE - internalLabels, MAX_ATTRIBUTES_SIZE));

    //
    // Ensure that the limits will allow for serialized metadata to fit in FoundationDB
    // This is checked for all versions of Warp 10 since they could either store directly
    // in FoundationDB (standalone+ / distributed) or have a datalog replication in place
    // to another instance which could be using FoundationDB.
    // The check can be relaxed in the case of a standalone version by setting the config
    // key 'warp.relax.metadata.limit' to 'true'
    //

    if ((long) MAX_CLASS_SIZE + (long) MAX_LABELS_SIZE + (long) MAX_ATTRIBUTES_SIZE > (long) (FDBUtils.MAX_VALUE_SIZE - FDB_SIZE_RESERVED)) {
      if (!WarpConfig.isStandaloneMode() || Constants.BACKEND_FDB.equals(WarpConfig.getProperty(Configuration.BACKEND)) || !"true".equals(WarpConfig.getProperty(Configuration.WARP_RELAX_METADATA_MAXSIZE, "false"))) {
        String msg = "Invalid metadata limits, '" + Configuration.WARP_CLASS_MAXSIZE + "' + '" + Configuration.WARP_LABELS_MAXSIZE + "' + '" + Configuration.WARP_ATTRIBUTES_MAXSIZE + "' should be less than " + (FDBUtils.MAX_VALUE_SIZE - FDB_SIZE_RESERVED) + " to ensure compatibility with FoundationDB based instances.";
        if (WarpConfig.isStandaloneMode() && !Constants.BACKEND_FDB.equals(WarpConfig.getProperty(Configuration.BACKEND))) {
          msg = msg + " As the current instance of Warp 10 does not use FoundationDB, you may relax this limit check by setting '" + Configuration.WARP_RELAX_METADATA_MAXSIZE + "' to 'true'. Only do so if no replication is performed to a FoundationDB based instance.";
        }
        throw new RuntimeException(msg);
      } else {
        LOG.warn("Metadata limit '" + Configuration.WARP_CLASS_MAXSIZE + "' + '" + Configuration.WARP_LABELS_MAXSIZE + "' + '" + Configuration.WARP_ATTRIBUTES_MAXSIZE + "' is above " + (FDBUtils.MAX_VALUE_SIZE - FDB_SIZE_RESERVED) + " which may break replication to a FoundationDB backed Warp 10 instance.");
      }
    }
  }

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
      String name = WarpURLDecoder.decode(m.group(1), StandardCharsets.UTF_8);

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

    if (null != metadata.getName() && metadata.getName().length() > MAX_CLASS_SIZE) {
      return false;
    }

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
}
