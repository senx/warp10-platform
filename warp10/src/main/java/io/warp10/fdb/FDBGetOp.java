//
//   Copyright 2025  SenX S.A.S.
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.apple.foundationdb.Transaction;

public class FDBGetOp implements FDBMutation {

  /**
   * Tenant prefix
   */
  private final byte[] tenant;
  private final byte[] key;

  CompletableFuture<byte[]> future = null;

  public FDBGetOp(byte[] key) {
    this(null, key);
  }

  public FDBGetOp(byte[] tenant, byte[] key) {
    this.tenant = tenant;
    this.key = key;
  }

  @Override
  public void apply(Transaction txn) {
    synchronized (this.key) {
      if (null != this.future) {
        throw new IllegalStateException("Operation has already been applied.");
      }
      if (null != tenant) {
        byte[] tkey = Arrays.copyOf(this.tenant, tenant.length + key.length);
        System.arraycopy(this.key, 0, tkey, this.tenant.length, key.length);
        this.future = txn.get(tkey);
      } else {
        this.future = txn.get(this.key);
      }
    }
  }

  @Override
  public int size() {
    return 103 + (null != tenant ? tenant.length : 0) + key.length;
  }

  public byte[] getKey() {
    return this.key;
  }

  public byte[] getValue() {
    if (null == future) {
      throw new IllegalStateException("Operation has not been applied.");
    }
    try {
      return this.future.get();
    } catch (ExecutionException|InterruptedException e) {
      throw new RuntimeException("Error retrieving value.", e);
    }
  }

  public byte[] getTenant() {
    return this.tenant;
  }
}
