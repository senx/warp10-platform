//
//   Copyright 2023  SenX S.A.S.
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

package io.warp10.standalone;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.Store;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.fdb.FDBClearRange;
import io.warp10.fdb.FDBContext;
import io.warp10.fdb.FDBMutation;
import io.warp10.fdb.FDBSet;
import io.warp10.fdb.FDBStoreClient;
import io.warp10.fdb.FDBUtils;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.sensision.Sensision;

public class StandaloneFDBStoreClient extends FDBStoreClient {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneFDBStoreClient.class);

  private final List<StandalonePlasmaHandlerInterface> plasmaHandlers = new ArrayList<StandalonePlasmaHandlerInterface>();

  private final FDBContext fdbContext;
  private final Database fdb;
  private final long fdbMaxTransactionSize;
  private final long fdbRetryLimit;

  private ThreadLocal<List<FDBMutation>> perThreadMutations = new ThreadLocal<List<FDBMutation>>() {
    protected List<FDBMutation> initialValue() {
      return new ArrayList<FDBMutation>();
    };
  };

  private ThreadLocal<AtomicLong> perThreadMutationsSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong(0L);
    };
  };

  public StandaloneFDBStoreClient(KeyStore keystore, Properties properties) throws IOException {
    super(keystore, properties);

    // Check that STORE_FDB_CLUSTERFILE and EGRESS_CLUSTER_FILE are identical
    if (null == properties.getProperty(Configuration.STORE_FDB_CLUSTERFILE) || !String.valueOf(properties.getProperty(Configuration.STORE_FDB_CLUSTERFILE)).equals(String.valueOf(properties.getProperty(Configuration.EGRESS_FDB_CLUSTERFILE)))) {
      throw new IOException("Invalid configuration, '" + Configuration.STORE_FDB_CLUSTERFILE + "' and '" + Configuration.EGRESS_FDB_CLUSTERFILE + "' must be set to the same value.");
    }

    // Check that tenants are also identical
    if (!String.valueOf(properties.getProperty(Configuration.STORE_FDB_TENANT)).equals(String.valueOf(properties.getProperty(Configuration.EGRESS_FDB_TENANT)))) {
      throw new IOException("Invalid configuration, '" + Configuration.STORE_FDB_TENANT + "' and '" + Configuration.EGRESS_FDB_TENANT + "' must have identical values.");
    }

    this.fdbContext = FDBUtils.getContext(properties.getProperty(Configuration.STORE_FDB_CLUSTERFILE), properties.getProperty(Configuration.STORE_FDB_TENANT));
    // TODO(hbs): implement a pool for writing?
    this.fdb = fdbContext.getDatabase();
    this.fdbMaxTransactionSize = (long) Math.min(FDBUtils.MAX_TXN_SIZE * 0.95, Long.parseLong(properties.getProperty(Configuration.STORE_FDB_DATA_PENDINGMUTATIONS_MAXSIZE, Constants.DEFAULT_FDB_DATA_PENDINGMUTATIONS_MAXSIZE)));
    this.fdbRetryLimit = Long.parseLong(properties.getProperty(Configuration.STORE_FDB_RETRYLIMIT, Store.DEFAULT_FDB_RETRYLIMIT));
  }

  @Override
  public void store(GTSEncoder encoder) throws IOException {
    if (null == encoder) {
      flushMutations();
      return;
    }

    GTSDecoder decoder = encoder.getDecoder();

    List<FDBMutation> mutations = perThreadMutations.get();
    AtomicLong mutationsSize = perThreadMutationsSize.get();

    byte[] tenantPrefix = this.fdbContext.getTenantPrefix();

    WriteToken token = (WriteToken) WarpConfig.getThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN);

    if (null == token) {
      throw new IOException("Missing token!");
    }

    if (this.FDBUseTenantPrefix) {
      if (token.getAttributesSize() > 0 && token.getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX)) {
        tenantPrefix = OrderPreservingBase64.decode(token.getAttributes().get(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX));
        if (8 != tenantPrefix.length) {
          throw new IOException("Invalid tenant prefix, length should be 8 bytes.");
        }
      } else {
        LOG.error("Incoherent configuration, Warp 10 mandates a tenant but the specified write token did not have a tenant prefix set, aborting.");
        throw new IOException("Incoherent configuration, Warp 10 mandates a tenant but the specified write token did not have a tenant prefix set, aborting.");
      }
    } else {
      if (token.getAttributesSize() > 0 && token.getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX)) {
        LOG.error("Incoherent configuration, Warp 10 has no tenant support but the specified write token has a tenant prefix set, aborting.");
        throw new IOException("Incoherent configuration, Store has no tenant support and the specified write token has a tenant prefix set.");
      }
    }

    while(decoder.next()) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8]).order(ByteOrder.BIG_ENDIAN);

      bb.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
      bb.putLong(encoder.getClassId());
      bb.putLong(encoder.getLabelsId());
      bb.putLong(Long.MAX_VALUE - decoder.getTimestamp());

      GTSEncoder enc = new GTSEncoder(decoder.getTimestamp(), this.keystore.getKey(KeyStore.AES_LEVELDB_DATA));

      enc.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());

      byte[] value = enc.getBytes();

      FDBSet set = new FDBSet(tenantPrefix, bb.array(), value);
      mutations.add(set);

      if (mutationsSize.addAndGet(set.size()) >= this.fdbMaxTransactionSize) {
        flushMutations();
      }
    }

    //
    // Notify the registered plasma handlers
    // Note that when using replication, plasma handlers will not see the whole set of data points
    // since data is only written once across the nodes.
    //

    for (StandalonePlasmaHandlerInterface plasmaHandler: this.plasmaHandlers) {
      if (plasmaHandler.hasSubscriptions()) {
        plasmaHandler.publish(encoder);
      }
    }
  }

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {

    if (null == token) {
      throw new IOException("Missing token!");
    }

    if (null == metadata) {
      flushMutations();
      return 0L;
    }

    byte[] tenantPrefix = this.fdbContext.getTenantPrefix();

    if (this.FDBUseTenantPrefix) {
      if (token.getAttributesSize() > 0 && token.getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX)) {
        tenantPrefix = OrderPreservingBase64.decode(token.getAttributes().get(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX));
        if (8 != tenantPrefix.length) {
          throw new IOException("Invalid tenant prefix, length should be 8 bytes.");
        }
      } else {
        LOG.error("Incoherent configuration, Warp 10 mandates a tenant but the specified write token did not have a tenant prefix set, aborting.");
        throw new IOException("Incoherent configuration, Warp 10 mandates a tenant but the specified write token did not have a tenant prefix set, aborting.");
      }
    } else {
      if (token.getAttributesSize() > 0 && token.getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX)) {
        LOG.error("Incoherent configuration, Warp 10 has no tenant support but the specified write token has a tenant prefix set, aborting.");
        throw new IOException("Incoherent configuration, Store has no tenant support and the specified write token has a tenant prefix set.");
      }
    }


    ByteBuffer startKey = ByteBuffer.wrap(new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8]).order(ByteOrder.BIG_ENDIAN);

    startKey.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
    startKey.putLong(metadata.getClassId());
    startKey.putLong(metadata.getLabelsId());
    startKey.putLong(Long.MAX_VALUE - end);

    FDBClearRange clearRange = null;

    // Endkey is 1 byte longer to include the start timestamp in the deletion
    ByteBuffer endKey = ByteBuffer.wrap(new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8 + 1]).order(ByteOrder.BIG_ENDIAN);

    endKey.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
    endKey.putLong(metadata.getClassId());
    endKey.putLong(metadata.getLabelsId());
    endKey.putLong(Long.MAX_VALUE - start);

    clearRange = new FDBClearRange(tenantPrefix, startKey.array(), endKey.array());

    List<FDBMutation> mutations = perThreadMutations.get();
    mutations.add(clearRange);

    if (perThreadMutationsSize.get().addAndGet(clearRange.size()) >= this.fdbMaxTransactionSize) {
      flushMutations();
    }

    return 0;
  }

  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {
    this.plasmaHandlers.add(handler);
  }

  private void flushMutations() throws IOException {
    List<FDBMutation> mutations = perThreadMutations.get();

    Transaction txn = null;

    int startidx = 0;

    while(startidx < mutations.size()) {
      boolean retry = false;
      long retries = this.fdbRetryLimit;

      do {
        try {
          retry = false;
          txn = this.fdb.createTransaction();
          // Allow RAW access because we may manually force a tenant key prefix without actually setting a tenant
          txn.options().setRawAccess();
          int sets = 0;
          int clearranges = 0;

          long size = 0L;

          int idx = startidx;

          while(idx < mutations.size() && size < fdbMaxTransactionSize) {
            FDBMutation mutation = mutations.get(idx++);

            if (mutation instanceof FDBSet) {
              sets++;
            } else if (mutation instanceof FDBClearRange) {
              clearranges++;
            }

            mutation.apply(txn);
            size += mutation.size();
          }

          txn.commit().get();
          startidx = idx;
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_SETS_COMMITTED, Sensision.EMPTY_LABELS, sets);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STORE_FDB_CLEARRANGES_COMMITTED, Sensision.EMPTY_LABELS, clearranges);
        } catch (Throwable t) {
          FDBUtils.errorMetrics("store", t.getCause());
          if (t.getCause() instanceof FDBException && ((FDBException) t.getCause()).isRetryable() && retries-- > 0) {
            retry = true;
          } else {
            throw new IOException("Error while commiting to FoundationDB.", t);
          }
        } finally {
          if (null != txn) {
            try { txn.close(); } catch (Throwable t) {}
          }
        }
      } while(retry);
    }

    mutations.clear();
    perThreadMutationsSize.get().set(0);
  }
}
