//
//   Copyright 2019-2023  SenX S.A.S.
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

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.datalog.DatalogHelper;

public class StandaloneShardedStoreClientWrapper implements StoreClient {
  private final long[] modulus;
  private final long[] remainder;

  /**
   * Number of bits to shift the shard key right.
   * If this is 24, then only the class Id will be considered
   */
  private final long shardkeyshift;

  private final StoreClient client;

  private final long[] classKeyLongs;
  private final long[] labelsKeyLongs;

  public StandaloneShardedStoreClientWrapper(KeyStore keystore, StoreClient client) {

    byte[] classKey = keystore.getKey(KeyStore.SIPHASH_CLASS);
    this.classKeyLongs = SipHashInline.getKey(classKey);

    byte[] labelsKey = keystore.getKey(KeyStore.SIPHASH_LABELS);
    this.labelsKeyLongs = SipHashInline.getKey(labelsKey);

    this.client = client;

    // Check if we have a limit on the shards we should store
    if (null != WarpConfig.getProperty(Configuration.DATALOG_SHARDS)) {
      this.shardkeyshift = Long.parseLong(WarpConfig.getProperty(Configuration.DATALOG_SHARDKEY_SHIFT, "48"));

      String[] shards = WarpConfig.getProperty(Configuration.DATALOG_SHARDS).split(",");

      this.modulus = new long[shards.length];
      this.remainder = new long[shards.length];

      int idx = 0;

      for (String shard: shards) {
        String[] tokens = shard.trim().split(":");
        if (2 != tokens.length) {
          throw new RuntimeException("Invalid shard specification " + shard);
        }
        this.modulus[idx] = Long.parseLong(tokens[0]);
        this.remainder[idx] = Long.parseLong(tokens[1]);

        if (this.modulus[idx] < 1 || this.remainder[idx] >= this.modulus[idx] || this.remainder[idx] < 0) {
          throw new RuntimeException("Invalid shard specification " + shard);
        }

        idx++;
      }
    } else {
      this.modulus = null;
      this.remainder = null;
      this.shardkeyshift = 0;
    }
  }

  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {
    this.client.addPlasmaHandler(handler);
  }

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    return this.client.delete(token, metadata, start, end);
  }

  @Override
  public GTSDecoderIterator fetch(FetchRequest req) throws IOException {
    return this.client.fetch(req);
  }

  @Override
  public void store(GTSEncoder encoder) throws IOException {
    // No shards, store unconditionnaly
    if (null == this.modulus || null == encoder) {
      this.client.store(encoder);
      return;
    }

    //
    // Determine if the encoder is for a shard we handle
    //

    // Extract shardkey 128BITS
    long shardkey = DatalogHelper.getShardId(GTSHelper.classId(classKeyLongs, encoder.getMetadata().getName()), GTSHelper.labelsId(labelsKeyLongs, encoder.getMetadata().getLabels()), shardkeyshift);

    boolean skip = true;

    for (int i = 0; i < this.modulus.length; i++) {
      if (shardkey % this.modulus[i] == this.remainder[i]) {
        skip = false;
        break;
      }
    }

    if (!skip) {
      this.client.store(encoder);
    }
  }
}
