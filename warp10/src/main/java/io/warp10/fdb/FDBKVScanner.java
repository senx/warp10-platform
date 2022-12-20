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
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;

public class FDBKVScanner implements Iterator<FDBKeyValue> {

  private static final Logger LOG = LoggerFactory.getLogger(FDBKVScanner.class);

  private final Database db;
  private final StreamingMode mode;

  private Transaction txn = null;

  private KeyValue lastkv = null;
  private byte[] lastKey = null;

  private boolean done = false;

  private final FDBScan scan;

  private AsyncIterator<KeyValue> iter = null;

  private AtomicReference<Transaction> transaction = null;

  /**
   * Number of attempts when cluster is lagging, 0 means no lag mitigation
   */
  private int versionAttempts = 32;

  /**
   * Read version offset to apply when we need to go back in time (post error 1037). In microseconds.
   * The offset will be modified along the attempts.
   */
  private long offset = 1000000L;

  private Long forceReadVersion = null;

  private final FDBContext context;

  private long count = 0L;

  public FDBKVScanner(FDBContext fdbContext, Database db, FDBScan scan, StreamingMode mode) {
    this(fdbContext, db, scan, mode, null);
  }

  public FDBKVScanner(FDBContext fdbContext, Database db, FDBScan scan, StreamingMode mode, AtomicReference<Transaction> transaction) {
    this.db = db;
    this.mode = mode;
    this.scan = scan;
    this.context = fdbContext;
    this.transaction = transaction;
    if (null != this.transaction) {
      this.txn = this.transaction.get();
    }
  }

  @Override
  public boolean hasNext() {
    while(true) {
      if (done) {
        return false;
      }

      if (null != this.lastkv) {
        return true;
      }

      if (null == txn) {
        this.txn = db.createTransaction();
        // Allow RAW access because we may manually force a tenant key prefix without actually setting a tenant
        this.txn.options().setRawAccess();
        this.txn.options().setCausalReadRisky();
        this.txn.options().setReadYourWritesDisable();
        this.txn.options().setSnapshotRywDisable();
        if (null != this.transaction) {
          this.transaction.set(txn);
        }
        this.iter = null;
      }

      try {
        if (null != this.iter) {
          if (this.iter.hasNext()) {
            this.lastkv = this.iter.next();
            this.lastKey = lastkv.getKey();
            this.count++;
            return true;
          } else {
            this.done = true;
            return false;
          }
        }

        byte[] startKey = this.scan.getStartKey();
        byte[] endKey = this.scan.getEndKey();
        boolean startPrefixed = false;
        boolean endPrefixed = false;

        if (null != lastKey) {
          if (!this.scan.isReverse()) {
            // We start just after the last read key
            startKey = Arrays.copyOf(lastKey, lastKey.length + 1);
            startPrefixed = true;
          } else {
            // We change the endKey to be the one we just read
            endKey = lastKey;
            endPrefixed = true;
          }
        }
        Range range = new Range(startPrefixed ? startKey : FDBUtils.addPrefix(this.scan.getTenantPrefix(), startKey), endPrefixed ? endKey : FDBUtils.addPrefix(this.scan.getTenantPrefix(), endKey));
        ReadTransaction snapshot = txn.snapshot();

        if (null != forceReadVersion && versionAttempts > 0) {
          versionAttempts--;
          if (Long.MIN_VALUE != this.forceReadVersion) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("FORCING READ VERSION ON TX " + txn + " TO " + this.forceReadVersion);
            }
            try {
              // Do not set the version too much in the past, limit it to the last seen read version we recorded
              snapshot.setReadVersion(Math.max(this.context.getLastSeenReadVersion(), this.forceReadVersion));
            } catch (Throwable tt) {
              throw tt;
            }
          }
        }

        this.iter = snapshot.getRange(range, ReadTransaction.ROW_LIMIT_UNLIMITED, scan.isReverse(), mode).iterator();
        continue; //return this.hasNext();
      } catch (Throwable t) {
        FDBUtils.errorMetrics("scanner", t.getCause());
        if (t.getCause() instanceof FDBException) {
          FDBException fdbe = (FDBException) t.getCause();
          int code = fdbe.getCode();

          // We support retrying after a 'transaction too old' error
          if (1007 == code) { // Transaction too old
            // If we were forcing a read version and read nothing then we probably set the read version
            // too far in the past, adjust it 0.5s in the future
            if (null != this.forceReadVersion && 0L == this.count) {
              this.forceReadVersion += 500000;
            }
            // Update last seen read version
            if (this.count > 0L) {
              try { this.context.setLastSeenReadVersion(this.txn.getReadVersion().get()); } catch (Throwable tt) {}
              this.count = 0L;
            }
            try { this.txn.close(); } catch (Throwable tt) {}
            this.iter = null;
            this.txn = null;
            if (null != this.transaction) {
              this.transaction.set(null);
            }
            continue; //return hasNext();
          } else if (1009 == code        // 1009: request for future version, we were too aggressive when forcing the version.
                     || 1037 == code) {  // 1037: process_behind, the cluster is lagging, attempt to force a known valid read version
            long readVersion = 0L;
            try {
              // Call getReadVersion so version gets a chance to be initialized and we don't risk getting a transaction_too_old (1007)
              // @see https://forums.foundationdb.org/t/use-of-setreadversion/3696/2
              readVersion = txn.getReadVersion().get();
              if (LOG.isDebugEnabled()) {
                LOG.debug("ENCOUNTERED ERROR " + code + " RV = " + readVersion + "   LastSeenRV = " + this.context.getLastSeenReadVersion());
              }
            } catch (Throwable tt) {}
            // We have reached the maximum number of forced version attempts
            if (0 >= this.versionAttempts) {
              throw new RuntimeException("Maximum number of forced read version attempts reached", t);
            }
            this.iter = null;
            if (null == this.forceReadVersion) {
              this.forceReadVersion = this.context.getLastSeenReadVersion();

              if (this.forceReadVersion < readVersion) {
                this.forceReadVersion = readVersion - offset;
              }
            } else {
              // forced read version was already set but reading failed anyway, so attempt with a read version further in the past
              this.forceReadVersion -= offset;
            }
            // Close the transaction since we cannot force the read version on a transaction which alread
            try { this.txn.close(); } catch (Throwable tt) {}
            this.iter = null;
            this.txn = null;
            if (null != this.transaction) {
              this.transaction.set(null);
            }
            continue; //return hasNext();
          } else if (1039 == code) { // cluster_version_changed, may happen when using the multi-version client upon first connection
            // Close the transaction
            try { this.txn.close(); } catch (Throwable tt) {}
            this.iter = null;
            this.txn = null;
            if (null != this.transaction) {
              this.transaction.set(null);
            }
            continue;
          }
        }
        throw t;
      }
    }
  }

  @Override
  public FDBKeyValue next() {
    if (this.done) {
      throw new RuntimeException("Scanner has reached the end of its range.");
    }

    if (null != this.lastkv) {
      KeyValue kv = this.lastkv;
      lastkv = null;

      // Strip tenant prefix
      if (null != this.scan.getTenantPrefix()) {
        return new FDBKeyValue(kv, this.scan.getTenantPrefix().length, kv.getKey().length - this.scan.getTenantPrefix().length, 0, kv.getValue().length);
      } else {
        return new FDBKeyValue(kv);
      }
    } else {
      throw new RuntimeException("Need to call hasNext prior to calling next.");
    }
  }

  public void close() {
    this.done = true;
    if (null == this.transaction && null != this.txn) {
      try {
        // Attempt to update the last seen version
        if (this.count > 0L) {
          try { this.context.setLastSeenReadVersion(this.txn.getReadVersion().get()); } catch (Throwable t) {}
        }
        this.txn.close();
      } catch (Throwable t) {
        LOG.error("Error closing FoundationDB transaction, will continue anyway.", t);
      }
    }
  }
}
