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

public class FDBSet implements FDBMutation {

  /**
   * Tenant prefix
   */
  private final byte[] tenant;
  private final byte[] key;
  private final byte[] value;

  public FDBSet(byte[] key, byte[] value) {
    this(null, key, value);
  }

  public FDBSet(byte[] tenant, byte[] key, byte[] value) {
    this.tenant = tenant;
    this.key = key;
    this.value = value;
  }

  @Override
  public void apply(Transaction txn) {
    if (null != tenant) {
      byte[] tkey = Arrays.copyOf(this.tenant, tenant.length + key.length);
      System.arraycopy(this.key, 0, tkey, this.tenant.length, key.length);
      txn.set(tkey, this.value);
    } else {
      txn.set(this.key, this.value);
    }
  }

  @Override
  public int size() {
    return 103 + (null != tenant ? tenant.length : 0) + key.length + value.length;
  }
}
