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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.bouncycastle.util.Arrays;
import org.bouncycastle.util.encoders.Hex;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.MetadataIdComparator;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.MultiScanGTSDecoderIterator;
import io.warp10.continuum.store.ParallelGTSDecoderIteratorWrapper;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.KVFetchRequest;
import io.warp10.continuum.store.thrift.data.KVStoreRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.functions.KVSTORE;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandalonePlasmaHandlerInterface;

public class FDBStoreClient implements StoreClient {

  private final FDBPool pool;

  public final KeyStore keystore;
  public final boolean FDBUseTenantPrefix;
  private final long fdbRetryLimit;

  private static final String EGRESS_FDB_POOLSIZE_DEFAULT = Integer.toString(1);

  public static final String DEFAULT_FDB_RETRYLIMIT = Long.toString(4);

  public FDBStoreClient(KeyStore keystore, Properties properties) throws IOException {
    this.keystore = keystore;
    this.FDBUseTenantPrefix = "true".equals(properties.getProperty(Configuration.FDB_USE_TENANT_PREFIX));
    this.fdbRetryLimit = Long.parseLong(properties.getProperty(Configuration.EGRESS_FDB_RETRYLIMIT, DEFAULT_FDB_RETRYLIMIT));

    if (FDBUseTenantPrefix && (null != properties.getProperty(Configuration.EGRESS_FDB_TENANT) || null != properties.getProperty(Configuration.EGRESS_FDB_TENANT_PREFIX))) {
      throw new RuntimeException("Cannot set '" + Configuration.EGRESS_FDB_TENANT + "' or '" + Configuration.EGRESS_FDB_TENANT_PREFIX + "' when '" + Configuration.FDB_USE_TENANT_PREFIX + "' is true.");
    }

    Object tenant = properties.getProperty(Configuration.EGRESS_FDB_TENANT);

    if (null != properties.getProperty(Configuration.EGRESS_FDB_TENANT_PREFIX)) {
      if (null != tenant) {
        throw new IOException("Invalid configuration, only one of '" + Configuration.EGRESS_FDB_TENANT_PREFIX + "' and '" + Configuration.EGRESS_FDB_TENANT + "' can be set.");
      }
      String prefix = properties.getProperty(Configuration.EGRESS_FDB_TENANT_PREFIX);
      if (prefix.startsWith("hex:")) {
        tenant = Hex.decode(prefix.substring(4));
      } else {
        tenant = OrderPreservingBase64.decode(prefix, 0, prefix.length());
      }
    }

    FDBContext context = FDBUtils.getContext(properties.getProperty(Configuration.EGRESS_FDB_CLUSTERFILE), tenant);
    int maxsize = Integer.parseInt(properties.getProperty(Configuration.EGRESS_FDB_POOLSIZE, EGRESS_FDB_POOLSIZE_DEFAULT));
    this.pool = new FDBPool(context, maxsize);
  }

  @Override
  public GTSDecoderIterator fetch(FetchRequest req) throws IOException {
    long preBoundary = req.getPreBoundary();
    long postBoundary = req.getPostBoundary();
    long step = req.getStep();
    long timestep = req.getTimestep();
    double sample = req.getSample();
    long skip = req.getSkip();
    long count = req.getCount();
    long then = req.getThents();
    List<Metadata> metadatas = req.getMetadatas();

    if (preBoundary < 0) {
      preBoundary = 0;
    }

    if (postBoundary < 0) {
      postBoundary = 0;
    }

    if (step < 1L) {
      step = 1L;
    }

    if (timestep < 1L) {
      timestep = 1L;
    }

    if (sample <= 0.0D || sample > 1.0D) {
      sample = 1.0D;
    }

    if (skip < 0L) {
      skip = 0L;
    }

    if (count < -1L) {
      count = -1L;
    }

    //
    // If we are fetching up to Long.MIN_VALUE, then don't fetch a pre boundary
    //
    if (Long.MIN_VALUE == then) {
      preBoundary = 0;
    }

    //
    // Determine the execution plan given the metadatas of the GTS we will be retrieving.
    // Some hints to choose the best plan:
    // # of different classes
    // # of different instances among each class
    // # among those instances, common labels
    //
    // TODO(hbs): we might have to gather statistics to better determine the exec plan


    //
    // Sort metadatas to optimize the range scans
    //

    Collections.sort(metadatas, MetadataIdComparator.COMPARATOR);

    // Remove Metadatas from FetchRequest otherwise new FetchRequest(req) will do a deep copy
    List<Metadata> lm = req.getMetadatas();
    req.unsetMetadatas();
    FetchRequest freq = new FetchRequest(req);
    req.setMetadatas(lm);
    freq.setMetadatas(lm);
    freq.setCount(count);
    freq.setSkip(skip);
    freq.setStep(step);
    freq.setTimestep(timestep);
    freq.setSample(sample);
    freq.setPreBoundary(preBoundary);
    freq.setPostBoundary(postBoundary);

    if (!freq.isParallelScanners() || metadatas.size() < ParallelGTSDecoderIteratorWrapper.getMinGTSPerScanner() || !ParallelGTSDecoderIteratorWrapper.useParallelScanners()) {
      return new MultiScanGTSDecoderIterator(this.FDBUseTenantPrefix, freq, this.pool, this.keystore);
    } else {
      return new ParallelGTSDecoderIteratorWrapper(this.FDBUseTenantPrefix, freq, this.pool, this.keystore);
    }
  }


  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public void store(GTSEncoder encoder) throws IOException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    throw new RuntimeException("Not Implemented.");
  }

  public FDBPool getPool() {
    return this.pool;
  }

  @Override
  public void kvstore(KVStoreRequest request) throws IOException {

    if (null == request) {
      return;
    }

    // Convert to FDBMutations, then batch those in tx of less than 10M

    WriteToken token = request.getToken();

    byte[] tenantPrefix = this.pool.getContext().getTenantPrefix();

    if (this.FDBUseTenantPrefix) {
      if (token.getAttributesSize() > 0 && token.getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX)) {
        tenantPrefix = OrderPreservingBase64.decode(token.getAttributes().get(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX));
        if (8 != tenantPrefix.length) {
          throw new IOException("Invalid tenant prefix, length should be 8 bytes.");
        }
      } else {
        throw new IOException("Configuration mandates a tenant and none was found in the provided token");
      }
    } else {
      if (token.getAttributesSize() > 0 && token.getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX)) {
        throw new IOException("Configuration has no tenant support but provided token had a defined tenant.");
      }
    }

    //
    // Extract kvprefix from token attributes
    //

    byte[] kvprefix = KVSTORE.KVPREFIX;

    if (token.getAttributesSize() > 0 && null != token.getAttributes().get(KVSTORE.ATTR_KVPREFIX)) {
      if (!token.getAttributes().get(KVSTORE.ATTR_KVPREFIX).isEmpty()) {
        byte[] tkvprefix = OrderPreservingBase64.decode(token.getAttributes().get(KVSTORE.ATTR_KVPREFIX));
        kvprefix = Arrays.copyOf(KVSTORE.KVPREFIX, KVSTORE.KVPREFIX.length + tkvprefix.length);
        System.arraycopy(tkvprefix, 0, kvprefix, KVSTORE.KVPREFIX.length, tkvprefix.length);
      }
    } else {
      throw new IOException("Token is missing attribute '" + KVSTORE.ATTR_KVPREFIX + "'.");
    }

    List<List<FDBMutation>> batches = new ArrayList<List<FDBMutation>>();
    List<FDBMutation> batch = new ArrayList<FDBMutation>();
    batches.add(batch);
    long batchsize = 0;

    for (int i = 0; i < request.getKeysSize(); i++) {
      ByteBuffer bb = request.getKeys().get(i);
      bb.mark();
      byte[] key = Arrays.copyOf(kvprefix, kvprefix.length + bb.remaining());

      if (key.length + (null != tenantPrefix ? tenantPrefix.length : 0) >= FDBUtils.MAX_KEY_SIZE) {
        throw new IOException("Key size (" + key.length + ") exceed maximum size.");
      }

      bb.get(key, kvprefix.length, bb.remaining());
      bb.reset();

      FDBMutation mutation = null;
      if (i > request.getValuesSize() || 0 == request.getValues().get(i).remaining()) {
        mutation = new FDBClear(tenantPrefix, key);
      } else {
        bb = request.getValues().get(i);
        bb.mark();
        if (bb.remaining() >= FDBUtils.MAX_VALUE_SIZE) {
          throw new IOException("Value size (" + bb.remaining() + ") exceed maximum size.");
        }

        byte[] value = new byte[bb.remaining()];
        bb.get(value);
        bb.reset();
        mutation = new FDBSet(tenantPrefix, key, value);
      }

      if (batchsize + mutation.size() > FDBUtils.MAX_TXN_SIZE * 0.95) {
        batch = new ArrayList<FDBMutation>();
        batches.add(batch);
        batchsize = 0;
      }

      batch.add(mutation);
      batchsize += mutation.size();
    }

    //
    // Persist the batches via one tx per batch
    //


    for (List<FDBMutation> btch: batches) {
      Database db = this.pool.getDatabase();
      Transaction txn = null;
      boolean retry = false;
      long retries = fdbRetryLimit;

      do {
        try {
          retry = false;
          txn = db.createTransaction();
          // Allow RAW access because we may manually force a tenant key prefix without actually setting a tenant
          if (this.pool.getContext().hasTenant() || this.FDBUseTenantPrefix) {
            txn.options().setRawAccess();
          }

          int sets = 0;
          int clears = 0;

          for (FDBMutation mutation: btch) {
            if (mutation instanceof FDBSet) {
              sets++;
            } else if (mutation instanceof FDBClearRange) {
              clears++;
            }
            mutation.apply(txn);
          }
          txn.commit().get();
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_EGRESS_FDB_SETS_COMMITTED, Sensision.EMPTY_LABELS, sets);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_EGRESS_FDB_CLEARS_COMMITTED, Sensision.EMPTY_LABELS, clears);
        } catch (Throwable t) {
          FDBUtils.errorMetrics("egress", t.getCause());
          if (t.getCause() instanceof FDBException && ((FDBException) t.getCause()).isRetryable() && retries-- > 0) {
            retry = true;
          } else {
            throw new IOException("Error while commiting to FoundationDB.", t);
          }
        } finally {
          if (null != txn) {
            try { txn.close(); } catch (Throwable t) {}
            txn = null;
          }
        }
      } while(retry);
    }
  }

  @Override
  public KVIterator<Entry<byte[], byte[]>> kvfetch(KVFetchRequest request) throws IOException {

    ReadToken token = request.getToken();

    if (FDBUseTenantPrefix && (0 == token.getAttributesSize() || !token.getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX))) {
      throw new IOException("Invalid token, missing tenant prefix.");
    } else if (!FDBUseTenantPrefix && (0 != token.getAttributesSize() && token.getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX))) {
      throw new IOException("Invalid token, no support for tenant prefix.");
    }

    byte[] tenantPrefix = null;

    if (FDBUseTenantPrefix) {
      tenantPrefix = OrderPreservingBase64.decode(token.getAttributes().get(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX));
      if (8 != tenantPrefix.length) {
        throw new IOException("Invalid tenant prefix, length should be 8 bytes.");
      }
    } else {
      tenantPrefix = pool.getContext().getTenantPrefix();
    }

    //
    // Extract kvprefix from token attributes
    //

    byte[] kvprefix = KVSTORE.KVPREFIX;

    if (token.getAttributesSize() > 0 && null != token.getAttributes().get(KVSTORE.ATTR_KVPREFIX)) {
      if (!token.getAttributes().get(KVSTORE.ATTR_KVPREFIX).isEmpty()) {
        byte[] tkvprefix = OrderPreservingBase64.decode(token.getAttributes().get(KVSTORE.ATTR_KVPREFIX));
        kvprefix = Arrays.copyOf(KVSTORE.KVPREFIX, KVSTORE.KVPREFIX.length + tkvprefix.length);
        System.arraycopy(tkvprefix, 0, kvprefix, KVSTORE.KVPREFIX.length, tkvprefix.length);
      }
    } else {
      throw new IOException("Token is missing attribute '" + KVSTORE.ATTR_KVPREFIX + "'.");
    }

    if (request.isSetStart() && request.isSetStop()) {
      FDBScan scan = new FDBScan();
      scan.setReverse(false);
      scan.setTenantPrefix(tenantPrefix);

      byte[] start = Arrays.copyOf(kvprefix, kvprefix.length + request.getStart().length);
      System.arraycopy(request.getStart(), 0, start, kvprefix.length, request.getStart().length);

      byte[] stop = Arrays.copyOf(kvprefix, kvprefix.length + request.getStop().length);
      System.arraycopy(request.getStop(), 0, stop, kvprefix.length, request.getStop().length);

      scan.setStartKey(start);
      scan.setEndKey(stop);

      AtomicReference<Transaction> tx = new AtomicReference<Transaction>();

      FDBKVScanner scanner = scan.getScanner(pool.getContext(), pool.getDatabase(), StreamingMode.ITERATOR, null);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FDB_CLIENT_SCANNERS, Sensision.EMPTY_LABELS, 1);

      final byte[] fkvprefix = kvprefix;

      return new KVIterator<Map.Entry<byte[],byte[]>>() {
        Entry<byte[],byte[]> element = null;
        boolean done = false;
        @Override

        public boolean hasNext() {
          if (done) {
            return false;
          }
          if (null != element) {
            return true;
          }

          if (!scanner.hasNext()) {
            done = true;
            return false;
          }

          FDBKeyValue kv = scanner.next();

          byte[] key = kv.getKey();

          // Strip kvprefix from key
          if (fkvprefix.length > 0) {
            byte[] k = Arrays.copyOfRange(key, fkvprefix.length, key.length);
            element = new AbstractMap.SimpleEntry<byte[],byte[]>(k, kv.getValue());
          } else {
            element = new AbstractMap.SimpleEntry<byte[],byte[]>(key, kv.getValue());
          }

          return null != element;
        }

        @Override
        public Entry<byte[], byte[]> next() {
          if (done) {
            throw new IllegalStateException();
          }
          if (null == element && !hasNext()) {
            throw new NoSuchElementException();
          }
          Entry<byte[],byte[]> elt = element;
          element = null;
          return elt;
        }

        @Override
        public void close() throws Exception {
          scanner.close();
        }
      };
    } else {
      // Keys

      long txsize = 0L;

      List<FDBGetOp> batch = new ArrayList<FDBGetOp>();

      List<Entry<byte[],byte[]>> kvs = new ArrayList<Entry<byte[],byte[]>>();

      for (int i = 0; i < request.getKeysSize(); i++) {
        ByteBuffer kbb = request.getKeys().get(i);
        byte[] key = Arrays.copyOf(kvprefix, kvprefix.length + kbb.remaining());
        kbb.get(key, kvprefix.length, kbb.remaining());

        FDBGetOp get = new FDBGetOp(tenantPrefix, key);

        // We've reached the size limit for a transaction, issue it to FDB
        if (txsize + get.size() >= FDBUtils.MAX_TXN_SIZE * 0.95) {
          kvs.addAll(flushGetOps(batch));
          batch.clear();
          txsize = 0;
        }
        batch.add(get);
        txsize += get.size();
      }

      if (!batch.isEmpty()) {
        kvs.addAll(flushGetOps(batch));
      }

      //
      // Return an iterator which will strip kvprefix from keys as elements are returned
      //

      Iterator<Entry<byte[],byte[]>> iter = kvs.iterator();

      final byte[] fkvprefix = kvprefix;

      return new KVIterator<Entry<byte[],byte[]>>() {
        boolean done = false;
        Entry<byte[],byte[]> element = null;
        @Override
        public void close() throws Exception {
          done = true;
        }
        @Override
        public boolean hasNext() {
          if (done) {
            return false;
          }
          if (null != element) {
            return true;
          }
          if (!iter.hasNext()) {
            done = true;
            return false;
          }
          Entry<byte[],byte[]> entry = iter.next();

          // Strip kvprefix from key
          if (fkvprefix.length > 0) {
            byte[] k = Arrays.copyOfRange(entry.getKey(), fkvprefix.length, entry.getKey().length);
            element = new AbstractMap.SimpleEntry<byte[],byte[]>(k, entry.getValue());
          } else {
            element = entry;
          }
          return true;
        }

        @Override
        public Entry<byte[], byte[]> next() {
          if (done) {
            throw new IllegalStateException();
          }

          if (null == element) {
            if (!hasNext()) {
              throw new IllegalStateException();
            }
          }

          Entry<byte[],byte[]> elt = element;
          element = null;
          return elt;
        }
      };
    }
  }

  private List<Entry<byte[],byte[]>> flushGetOps(List<FDBGetOp> batch) {
    List<Entry<byte[],byte[]>> kvs = new ArrayList<Entry<byte[],byte[]>>(batch.size());

    if (batch.isEmpty()) {
      return kvs;
    }

    Transaction txn = null;

    /**
     * Number of attempts when cluster is lagging, 0 means no lag mitigation
     */
    int versionAttempts = 32;

    /**
     * Read version offset to apply when we need to go back in time (post error 1037). In microseconds.
     * The offset will be modified along the attempts.
     */
    long offset = 1000000L;

    Long forceReadVersion = null;

    long count = 0L;

    long attempts = this.fdbRetryLimit;

    while(attempts-- > 0) {
      try {
        txn = this.pool.getDatabase().createTransaction();
        // Allow RAW access because we may manually force a tenant key prefix without actually setting a tenant
        if (this.pool.getContext().hasTenant() || null != batch.get(0).getTenant()) {
          txn.options().setRawAccess();
        }

        txn.options().setCausalReadRisky();
        txn.options().setReadYourWritesDisable();
        txn.options().setSnapshotRywDisable();

        for (FDBGetOp op: batch) {
          op.apply(txn);
        }

        txn.commit();

        for (FDBGetOp op: batch) {
          kvs.add(new AbstractMap.SimpleEntry(op.getKey(), op.getValue()));
        }

        return kvs;
      } catch (Throwable t) {
        FDBUtils.errorMetrics("kvfetch", t.getCause());
        if (t.getCause() instanceof FDBException) {
          FDBException fdbe = (FDBException) t.getCause();
          int code = fdbe.getCode();

          // We support retrying after a 'transaction too old' error
          if (1007 == code) { // Transaction too old
            // If we were forcing a read version and read nothing then we probably set the read version
            // too far in the past, adjust it 0.5s in the future
            if (null != forceReadVersion && 0L == count) {
              forceReadVersion += 500000;
            }
            // Update last seen read version
            if (count > 0L) {
              try { this.pool.getContext().setLastSeenReadVersion(txn.getReadVersion().get()); } catch (Throwable tt) {}
              count = 0L;
            }
            try { txn.close(); } catch (Throwable tt) {}
            txn = null;
            continue; //return hasNext();
          } else if (1009 == code        // 1009: request for future version, we were too aggressive when forcing the version.
                     || 1037 == code) {  // 1037: process_behind, the cluster is lagging, attempt to force a known valid read version
            long readVersion = 0L;
            try {
              // Call getReadVersion so version gets a chance to be initialized and we don't risk getting a transaction_too_old (1007)
              // @see https://forums.foundationdb.org/t/use-of-setreadversion/3696/2
              readVersion = txn.getReadVersion().get();
            } catch (Throwable tt) {}
            // We have reached the maximum number of forced version attempts
            if (0 >= versionAttempts) {
              Sensision.update(SensisionConstants.CLASS_WARP_FDB_MAXFORCEDVERSION, Sensision.EMPTY_LABELS, 1);
              throw new RuntimeException("Maximum number of forced read version attempts reached", t);
            }

            if (null == forceReadVersion) {
              forceReadVersion = this.pool.getContext().getLastSeenReadVersion();

              if (forceReadVersion < readVersion) {
                forceReadVersion = readVersion - offset;
              }
            } else {
              // forced read version was already set but reading failed anyway, so attempt with a read version further in the past
              forceReadVersion -= offset;
            }
            // Close the transaction since we cannot force the read version on a transaction which alread
            try { txn.close(); } catch (Throwable tt) {}
            txn = null;
            continue; //return hasNext();
          } else if (1039 == code) { // cluster_version_changed, may happen when using the multi-version client upon first connection
            // Close the transaction
            try { txn.close(); } catch (Throwable tt) {}
            txn = null;
            continue;
          }
        }
        throw t;
      } finally {
        if (null != txn) {
          try { txn.close(); } catch (Throwable tt) {}
        }
      }
    }

    throw new RuntimeException("Unable to retrieve Key/Value pairs from FoundationDB");
  }
}
