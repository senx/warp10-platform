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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.codec.binary.Hex;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;

public class FDBScan {

  private byte[] tenantPrefix = null;

  /**
   * First key, included
   */
  private byte[] startKey = null;

  /**
   * End key, excluded
   */
  private byte[] endKey = null;

  /**
   * Reverse scan direction
   */
  private Boolean reverse = null;

  public void setTenantPrefix(byte[] tenant) throws IOException {
    if (null != this.tenantPrefix) {
      throw new IOException("Tenant already set.");
    }
    this.tenantPrefix = tenant;
  }

  public void setStartKey(byte[] key) throws IOException {
    if (null != startKey) {
      throw new IOException("Start key already set.");
    }
    this.startKey = key;
  }

  public void setEndKey(byte[] key) throws IOException {
    if (null != endKey) {
      throw new IOException("End key already set.");
    }
    this.endKey = key;
  }

  public void setReverse(boolean reverse) throws IOException {
    if (null != this.reverse) {
      throw new IOException("Reverse already set.");
    }
    this.reverse = reverse;
  }

  public byte[] getTenantPrefix() {
    return this.tenantPrefix;
  }

  public byte[] getEndKey() {
    return this.endKey;
  }
  public byte[] getStartKey() {
    return this.startKey;
  }
  public boolean isReverse() {
    return this.reverse;
  }

  public FDBKVScanner getScanner(FDBContext fdbContext, Database db, StreamingMode mode) throws IOException {
    return getScanner(fdbContext, db, mode, null);
  }

  public FDBKVScanner getScanner(FDBContext fdbContext, Database db, StreamingMode mode, AtomicReference<Transaction> persistentTransaction) throws IOException {
    if (null == reverse) {
      reverse = false;
    }

    return new FDBKVScanner(fdbContext, db, this, mode, persistentTransaction);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Scan\n");
    if (null != tenantPrefix) {
      sb.append("  tenantPrefix = " + Hex.encodeHexString(tenantPrefix) + "\n");
    }
    sb.append("  start  = " + Hex.encodeHexString(startKey) + "\n");
    sb.append("  end    = " + Hex.encodeHexString(endKey) + "\n");
    return sb.toString();
  }
}
