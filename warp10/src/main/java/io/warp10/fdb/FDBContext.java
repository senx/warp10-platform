//
//   Copyright 2022  SenX S.A.S.
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
import java.util.Map;

import com.apple.foundationdb.Database;

public class FDBContext {

  private final String clusterFile;
  private final byte[] tenantPrefix;
  private final byte[] tenantName;

  /**
   * Field to keep track of the last seen read version. Used by FDBKVScanner to retry
   * getRange operations which encountered an error 1037 (process_behind) which may occur
   * when FDB is experiencing a high load and some storage servers are lagging.
   */
  private long lastSeenReadVersion = Long.MIN_VALUE;

  // TODO(hbs): support creating context from tenant key prefix (for 6.x compatibility)?
  public FDBContext(String clusterFile, String tenant) {
    this.clusterFile = clusterFile;

    if (null != tenant && !"".equals(tenant.trim())) {
      Database db = null;

      try {
        db = FDBUtils.getFDB().open(clusterFile);
        Map<String,Object> map = FDBUtils.getTenantInfo(db, tenant);
        if (map.isEmpty()) {
          throw new RuntimeException("Unknown FoundationDB tenant '" + tenant + "'.");
        }
        this.tenantPrefix = (byte[]) map.get(FDBUtils.KEY_PREFIX);
        this.tenantName = tenant.getBytes(StandardCharsets.UTF_8);
      } catch (Throwable t) {
        throw t;
      } finally {
        if (null != db) {
          try { db.close(); } catch (Throwable t) {}
        }
      }
    } else {
      this.tenantPrefix = null;
      this.tenantName = null;
    }
  }

  public Database getDatabase() {
    return FDBUtils.getFDB().open(this.clusterFile);
  }

  public byte[] getTenantPrefix() {
    return this.tenantPrefix;
  }

  public byte[] getTenantName() {
    return this.tenantName;
  }

  public void setLastSeenReadVersion(long version) {
    if (version > this.lastSeenReadVersion) {
      this.lastSeenReadVersion = version;
    }
  }

  public long getLastSeenReadVersion() {
    return this.lastSeenReadVersion;
  }
}
