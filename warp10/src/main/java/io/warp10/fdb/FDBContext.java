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
  private final byte[] tenant;

  // TODO(hbs): support creating context from tenant key prefix (for 6.x compatibility)?
  public FDBContext(String clusterFile, String tenant) {
    this.clusterFile = clusterFile;

    if (null != tenant) {
      Database db = null;

      try {
        db = FDBUtils.getFDB().open(clusterFile);
        Map<String,Object> map = FDBUtils.getTenantInfo(db, tenant);
        if (map.isEmpty()) {
          throw new RuntimeException("Unknown FoundationDB tenant '" + tenant + "'.");
        }
        this.tenant = (byte[]) map.get(FDBUtils.KEY_PREFIX);
      } catch (Throwable t) {
        throw t;
      } finally {
        if (null != db) {
          try { db.close(); } catch (Throwable t) {}
        }
      }
    } else {
      this.tenant = null;
    }
  }

  public Database getDatabase() {
    return FDBUtils.getFDB().open(this.clusterFile);
  }

  public byte[] getTenant() {
    return this.tenant;
  }
}
