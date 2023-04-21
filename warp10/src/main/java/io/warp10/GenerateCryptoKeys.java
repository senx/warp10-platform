//
//   Copyright 2020-2023  SenX S.A.S.
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

package io.warp10;

import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;

import org.bouncycastle.util.encoders.Hex;

import io.warp10.continuum.Configuration;

public class GenerateCryptoKeys {

  private static final String CLASS_HASH_KEY = "class.hash.key";
  private static final String LABELS_HASH_KEY = "labels.hash.key";
  private static final String TOKEN_HASH_KEY = "token.hash.key";
  private static final String APP_HASH_KEY = "app.hash.key";
  private static final String TOKEN_AES_KEY = "token.aes.key";
  private static final String SCRIPTS_AES_KEY = "scripts.aes.key";
  private static final String METASETS_AES_KEY = "metasets.aes.key";
  private static final String LOGGING_AES_KEY = "logging.aes.key";
  private static final String FETCH_HASH_KEY = "fetch.hash.key";
  private static final SecureRandom sr = new SecureRandom();


  private static final String KAFKA_DATA_MAC = "kafka.data.mac";
  private static final String KAFKA_DATA_AES = "kafka.data.aes";
  private static final String FDB_DATA_AES = "fdb.data.aes";
  private static final String FDB_METADATA_AES = "fdb.metadata.aes";
  private static final String KAFKA_METADATA_MAC = "kafka.metadata.mac";
  private static final String KAFKA_METADATA_AES = "kafka.metadata.aes";

  private static final Map<String,Integer> keys = new LinkedHashMap<String,Integer>();

  static {

    // Always generate the longest key possible.

    //
    // Common keys
    //

    keys.put(Configuration.WARP_HASH_CLASS, 128);
    keys.put(Configuration.WARP_HASH_LABELS, 128);
    keys.put(Configuration.WARP_HASH_TOKEN, 128);
    keys.put(Configuration.WARP_HASH_APP, 128);
    keys.put(Configuration.WARP_AES_TOKEN, 256);
    keys.put(Configuration.WARP_AES_SCRIPTS, 256);
    keys.put(Configuration.WARP_AES_METASETS, 256);
    keys.put(Configuration.WARP_AES_LOGGING, -256);
    keys.put(Configuration.EGRESS_FETCHER_AES, 256);
    keys.put(Configuration.CONFIG_FETCH_PSK, 128);
    keys.put(Configuration.RUNNER_PSK, 256);
  }


  public static void main(String[] args) {

    if (args.length > 0) {
      if ("distributed".equals(args[0])) {
        keys.put(KAFKA_DATA_MAC, 128);
        keys.put(KAFKA_DATA_AES, -256);

        keys.put(FDB_METADATA_AES, 256);
        keys.put(FDB_DATA_AES, -256);

        keys.put(KAFKA_METADATA_MAC, 128);
        keys.put(KAFKA_METADATA_AES, -256);

        keys.put(Configuration.DIRECTORY_PSK, 128);
        keys.put(Configuration.RUNNER_KAFKA_MAC, 128);
      } else if ("standalone".equals(args[0])) {
        keys.put(Configuration.LEVELDB_METADATA_AES, 256);
        keys.put(Configuration.LEVELDB_DATA_AES, -256);
      } else if ("standalone+".equals(args[0])) {
        keys.put(FDB_METADATA_AES, 256);
        keys.put(FDB_DATA_AES, -256);
      } else if ("in-memory".equals(args[0])) {
        // Nothing special for in-memory
      } else {
        System.err.println("Unknown mode.");
        System.exit(1);
      }
    }

    for (Map.Entry<String,Integer> keyEntry: keys.entrySet()) {
      byte[] key = new byte[(int) Math.abs(keyEntry.getValue()) / 8];
      sr.nextBytes(key);
      if (keyEntry.getValue() < 0) {
        System.out.print("#");
      }
      System.out.println(keyEntry.getKey() + " = hex:" + Hex.toHexString(key));
    }
  }
}
