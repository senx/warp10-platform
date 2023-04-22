//
//   Copyright 2022-2023  SenX S.A.S.
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

package io.warp10.fdb;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.json.JsonUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.Warp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FDBUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FDBUtils.class);

  public static final String CAPABILITY_ADMIN = "fdb.admin";
  public static final String CAPABILITY_STATUS = "fdb.status";
  public static final String CAPABILITY_TENANT = "fdb.tenant";
  public static final String CAPABILITY_SIZE = "fdb.size";
  public static final String CAPABILITY_GET = "fdb.get";

  public static final String KEY_ID = "id";
  public static final String KEY_PREFIX = "prefix";

  public static final int MAX_VALUE_SIZE = 100000;
  public static final long MAX_TXN_SIZE = 10000000;

  private static final String DEFAULT_FDB_API_VERSION = Integer.toString(720);

  static {
    if (!Warp.isStandaloneMode() || Constants.BACKEND_FDB.equals(WarpConfig.getProperty(Configuration.BACKEND))) {
      int version = Integer.parseInt(WarpConfig.getProperty(Configuration.FDB_API_VERSION, DEFAULT_FDB_API_VERSION));
      try {
        FDB.selectAPIVersion(version);
      } catch (UnsatisfiedLinkError t) {
        LOG.error("Unable to initialize FoundationDB API version, please ensure the FoundationDB clients package is installed.");
        throw new RuntimeException("Caught exception when initializing FoundationDB API version, please ensure the FoundationDB clients package is installed.");
      } catch (Throwable t) {
        LOG.error("Unable to initialize FoundationDB API version, " + t.getMessage());
        throw new RuntimeException("Caught exception when initializing FoundationDB API version, " + t.getMessage());
      }
    }
  }

  public static FDB getFDB() {
    return FDB.instance();
  }

  public static FDBContext getContext(String clusterFile, String tenant) {
    return new FDBContext(clusterFile, tenant);
  }

  public static byte[] getNextKey(byte[] key) {
    return getNextKey(key, 0, key.length);
  }

  public static byte[] getNextKey(byte[] key, int offset, int len) {
    if (0 != len) {
      // Return a byte array which is 'one bit after' prefix
      byte[] next = null;

      if ((byte) 0xff != key[offset + len - 1]) {
        next = Arrays.copyOfRange(key, offset, offset + len);
        next[next.length - 1] = (byte) (((((int) next[next.length - 1]) & 0xff) + 1) & 0xff);
      } else {
        next = Arrays.copyOfRange(key, offset, offset + len + 1);
        next[next.length - 1] = (byte) 0x00;
      }
      return next;
    } else {
      return key;
    }
  }

  public static byte[] addPrefix(byte[] prefix, byte[] key) {
    if (null == prefix) {
      return key;
    } else {
      byte[] pkey = Arrays.copyOf(prefix, prefix.length + key.length);
      System.arraycopy(key, 0, pkey, prefix.length, key.length);
      return pkey;
    }
  }

  public static void errorMetrics(String component, Throwable t) {
    if (!(t instanceof FDBException)) {
      return;
    }
    FDBException fdbe = (FDBException) t;
    Map<String,String> labels = new LinkedHashMap<String,String>();
    labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, component);
    labels.put(SensisionConstants.SENSISION_LABEL_CODE, Integer.toString(fdbe.getCode()));
    Sensision.update(SensisionConstants.CLASS_WARP_FDB_ERRORS, labels, 1);
  }

  public static byte[] getKey(Database db, byte[] key) {
    Transaction txn = db.createTransaction();
    txn.options().setRawAccess();
    txn.options().setAccessSystemKeys();

    try {
      byte[] value = txn.get(key).get();
      return value;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    } finally {
      try { txn.close(); } catch (Throwable t) {}
    }
  }

  public static Map<String,Object> getTenantInfo(FDBContext context, String tenant) {
    Database db = context.getDatabase();
    try {
      return getTenantInfo(db, tenant);
    } finally {
      try { db.close(); } catch (Throwable t) {}
    }
  }

  public static Map<String,Object> getTenantInfo(Database db, String tenant) {
    Transaction txn = db.createTransaction();
    txn.options().setRawAccess();
    txn.options().setAccessSystemKeys();

    Map<String,Object> map = new LinkedHashMap<String,Object>();

    // Retrieve the system key for the given tenant
    try {
      byte[] tenantMap = txn.get(getTenantSystemKey(tenant)).get();
      if (null != tenantMap) {
        Object json = JsonUtils.jsonToObject(new String(tenantMap, StandardCharsets.UTF_8));

        map.put(KEY_ID, ((Number) ((Map) json).get(KEY_ID)).longValue());
        map.put(KEY_PREFIX, ((String) ((Map) json).get(KEY_PREFIX)).getBytes(StandardCharsets.ISO_8859_1));
      }
    } catch (Throwable t) {
    } finally {
      try { txn.close(); } catch (Throwable t) {}
    }

    return map;
  }

  public static Map<Object,Object> getStatus(FDBContext context) throws WarpScriptException {
    Database db = null;
    Transaction txn = null;

    int attempts = 2;

    try {
      db = context.getDatabase();
      while(attempts > 0) {
        try {
          txn = db.createTransaction();
          txn.options().setRawAccess();
          txn.options().setAccessSystemKeys();

          byte[] statuskey = "xx/status/json".getBytes(StandardCharsets.US_ASCII);
          statuskey[0] = (byte) 0xff;
          statuskey[1] = (byte) 0xff;
          byte[] status = txn.get(statuskey).get();

          if (null != status) {
            Object json = JsonUtils.jsonToObject(new String(status, StandardCharsets.UTF_8));
            return (Map<Object,Object>) json;
          } else {
            return new LinkedHashMap<Object,Object>();
          }
        } catch (Throwable t) {
          FDBUtils.errorMetrics("status", t.getCause());

          if (t.getCause() instanceof FDBException) {
            FDBException fdbe = (FDBException) t.getCause();
            if (fdbe.getCode() == 1039) {
              attempts--;
              continue;
            }
          }

          throw new WarpScriptException("Error while fetching FoundationDB status.", t);
        } finally {
          if (null != txn) { try { txn.close(); } catch (Throwable t) {} }
        }
      }
    } finally {
      if (null != db) { try { db.close(); } catch (Throwable t) {} }
    }
    throw new RuntimeException("I got lost while fetching FoundationDB status.");
  }

  private static byte[] getTenantSystemKey(String tenant) {
    // Tenant key is '\xff\xff/management/tenant_map/TENANT'
    byte[] systemKey = ("xx/management/tenant_map/" + tenant).getBytes(StandardCharsets.UTF_8);
    systemKey[0] = (byte) 0xff;
    systemKey[1] = (byte) 0xff;
    return systemKey;
  }

  public static long getEstimatedRangeSizeBytes(FDBContext context, byte[] from, byte[] to) {
    Database db = context.getDatabase();
    Transaction txn = db.createTransaction();
    long size = 0L;

    try {
      txn.options().setRawAccess();
      size = txn.getEstimatedRangeSizeBytes(from, to).get().longValue();
    } catch (Throwable t) {
    } finally {
      try { txn.close(); } catch (Throwable t) {}
      try { db.close(); } catch (Throwable t) {}
    }

    return size;
  }

  /**
   * Return true or false depending on whether or not the encoder would fit in an FDB transaction but an additional data point
   * would make it possibly exceed the threshold.
   * @param encoder Encoder to test
   * @param maxValueSize Maximum value size.
   * @return
   */
  public static boolean hasCriticalTransactionSize(GTSEncoder encoder, long maxValueSize) {
    long size = 0L;

    // RFC 3629 states that UTF-8 encoding is using at most 4 bytes per character, so we
    // update 'size' with an overly pessimistic estimate of 4 x valueSize for the max possible value

    size += 4 * maxValueSize;
    // Add timestamp / location / elevation / header bytes
    size += 8 + 8 + 8 + 2;

    // If encoder is non null, use its count and pessimisticSize to update 'size'
    long count = 1;

    if (null != encoder) {
      size += encoder.getPessimisticSize();
      count += encoder.getCount();
    }

    size += (count + 1) * (103 + 8 /* tenant */ + 8 /* class ID */ + 8 /* labels ID */ + 1 /* key prefix, we do not count the timestamp which is already accounted for in the pessimistic size estimate */);

    //
    // If the estimated size is greater or equal to 4,500,000 (45 % of FDB max size), then we return true.
    //

    return size >= 4500000;
  }
}
