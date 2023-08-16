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
import java.util.Map;


import com.apple.foundationdb.Database;

import io.warp10.script.WarpScriptLib;

public class FDBContext {

  private final String clusterFile;
  private final byte[] tenantPrefix;
  private final byte[] tenantName;

  static {
    //
    // FDB related functions
    //

    WarpScriptLib.addNamedWarpScriptFunction(new FDBTENANT(WarpScriptLib.FDBTENANT));
    WarpScriptLib.addNamedWarpScriptFunction(new FDBSTATUS(WarpScriptLib.FDBSTATUS));
    WarpScriptLib.addNamedWarpScriptFunction(new FDBSIZE(WarpScriptLib.FDBSIZE));
    WarpScriptLib.addNamedWarpScriptFunction(new FDBGET(WarpScriptLib.FDBGET));
  }

  /**
   * Field to keep track of the last seen read version. Used by FDBKVScanner to retry
   * getRange operations which encountered an error 1037 (process_behind) which may occur
   * when FDB is experiencing a high load and some storage servers are lagging.
   */
  private long lastSeenReadVersion = Long.MIN_VALUE;

  // TODO(hbs): support creating context from tenant key prefix (for 6.x compatibility)?
  public FDBContext(String clusterFile, Object tenant) {
    this.clusterFile = clusterFile;

    if (null != tenant && tenant instanceof String && !"".equals(((String) tenant).trim())) {
      Database db = null;

      try {
        db = FDBUtils.getFDB().open(clusterFile);
        Map<String,Object> map = FDBUtils.getTenantInfo(db, (String) tenant);
        if (map.isEmpty()) {
          throw new RuntimeException("Unknown FoundationDB tenant '" + tenant + "'.");
        }
        this.tenantPrefix = (byte[]) map.get(FDBUtils.KEY_PREFIX);
        this.tenantName = ((String) tenant).getBytes(StandardCharsets.UTF_8);
      } catch (Throwable t) {
        throw t;
      } finally {
        if (null != db) {
          try { db.close(); } catch (Throwable t) {}
        }
      }
    } else if (null != tenant && tenant instanceof byte[]) {
      if (8 != ((byte[]) tenant).length) {
        throw new RuntimeException("Invalid tenant prefix length, MUST be of length 8.");
      }
      this.tenantPrefix = Arrays.copyOf((byte[]) tenant, ((byte[]) tenant).length);
      this.tenantName = new byte[0];
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

  public boolean hasTenant() {
    return null != this.tenantPrefix;
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
