//
//   Copyright 2022-2025  SenX S.A.S.
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

public class FDBClear implements FDBMutation {

  private final byte[] tenant;
  private final byte[] key;

  public FDBClear(byte[] key) {
    this(null, key);
  }

  public FDBClear(byte[] tenant, byte[] key) {
    this.tenant = tenant;
    this.key = key;
  }

  @Override
  public void apply(Transaction txn) {
    if (null != tenant) {
      byte[] tkey = Arrays.copyOf(this.tenant, tenant.length + key.length);
      System.arraycopy(this.key, 0, tkey, this.tenant.length, key.length);
      txn.clear(tkey);
    } else {
      txn.clear(this.key);
    }

  }

  @Override
  public int size() {
    return 120 + key.length + (null != this.tenant ? tenant.length : 0);
  }
}
