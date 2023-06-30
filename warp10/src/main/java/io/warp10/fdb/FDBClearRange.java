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

import java.util.Arrays;

import com.apple.foundationdb.Transaction;

public class FDBClearRange implements FDBMutation {

  private final byte[] tenant;
  private final byte[] startKey;
  private final byte[] endKey;

  /**
   * Create a clear range mutation which will clear from startKey (inclusive) to
   * endKey (exclusive)
   */
  public FDBClearRange(byte[] tenant, byte[] startKey, byte[] endKey) {
    this.tenant = tenant;
    this.startKey = startKey;
    this.endKey = endKey;
  }

  @Override
  public void apply(Transaction txn) {
    if (null != tenant) {
      byte[] tskey = Arrays.copyOf(this.tenant, tenant.length + startKey.length);
      System.arraycopy(this.startKey, 0, tskey, this.tenant.length, startKey.length);
      byte[] tekey = Arrays.copyOf(this.tenant, tenant.length + endKey.length);
      System.arraycopy(this.endKey, 0, tekey, this.tenant.length, endKey.length);
      txn.clear(tskey, tekey);
    } else {
      txn.clear(this.startKey, this.endKey);
    }
  }

  @Override
  public int size() {
    return 103 + startKey.length + endKey.length + 2 * (null != this.tenant ? this.tenant.length : 0);
  }
}
