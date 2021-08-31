//
//   Copyright 2018-2020  SenX S.A.S.
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

package io.warp10.crypto;

import com.google.common.base.Preconditions;

import java.util.Properties;

/**
 * The interface Key store.
 */
public interface KeyStore {
  /**
   * Name of key for hashing class names
   */
  public static final String SIPHASH_CLASS = "warp.siphash.class";

  /**
   * Name of key for hash labels
   */
  public static final String SIPHASH_LABELS = "warp.siphash.labels";

  /**
   * Name of secondary key for class name hashing
   */
  public static final String SIPHASH_CLASS_SECONDARY = "warp.siphash.class.secondary";

  /**
   * Name of secondary key for labels hashing
   */
  public static final String SIPHASH_LABELS_SECONDARY = "warp.siphash.labels.secondary";

  /**
   * Name of key for application Ids
   */
  public static final String SIPHASH_APPID = "warp.siphash.appid";

  /**
   * Name of key for token Ids.
   */
  public static final String SIPHASH_TOKEN = "warp.siphash.token";

  /**
   * Name of key for wrapping metadata
   */
  public static final String AES_HBASE_METADATA = "warp.aes.hbase.metadata";

  /**
   * Name of key for wrapping data (readings)
   */
  public static final String AES_HBASE_DATA = "warp.aes.hbase.data";

  /**
   * Name of key for wrapping Tokens
   */
  public static final String AES_TOKEN = "warp.aes.token";

  /**
   * PreShared key for Runner so scripts coming from runners can be identified
   */
  public static final String AES_RUNNER_PSK = "warp.aes.runner.psk";

  /**
   * Name of key for computing MAC for kafka runn requests
   */
  public static final String SIPHASH_KAFKA_RUNNER = "warp.siphash.kafka.runner";

  /**
   * Name of key for computing MAC for kafka data messages
   */
  public static final String SIPHASH_KAFKA_DATA = "warp.siphash.kafka.data";

  /**
   * Name of key for computing MAC for kafka metadata messages
   */
  public static final String SIPHASH_KAFKA_METADATA = "warp.siphash.kafka.metadata";

  /**
   * Name of key for computing MAC for kafka messages consumed by plasma backends
   */
  public static final String SIPHASH_KAFKA_PLASMA_BACKEND_IN = "warp.siphash.kafka.plasma.backend.in";

  /**
   * Name of key for computing MAC for kafka messages produced by plasma backends
   */
  public static final String SIPHASH_KAFKA_PLASMA_BACKEND_OUT = "warp.siphash.kafka.plasma.backend.out";

  /**
   * Name of key for computing MAC for kafka messages consumed by plasma frontends
   */
  public static final String SIPHASH_KAFKA_PLASMA_FRONTEND_IN = "warp.siphash.kafka.plasma.frontend.in";

  /**
   * Name of key for computing MAC for WebCall requests in Kafka
   */
  public static final String SIPHASH_KAFKA_WEBCALL = "warp.siphash.kafka.webcall";

  /**
   * Name of key for computing MAC for DirectoryFindRequest instances
   */
  public static final String SIPHASH_DIRECTORY_PSK = "warp.siphash.directory.psk";

  /**
   * Name of key for computing MAC for fetch requests
   */
  public static final String SIPHASH_FETCH_PSK = "warp.siphash.fetch.psk";

  /**
   * Name of key for wrapping kafka data messages
   */
  public static final String AES_KAFKA_DATA = "warp.aes.kafka.data";

  /**
   * Name of key for wrapping kafka medata messages
   */
  public static final String AES_KAFKA_METADATA = "warp.aes.kafka.metadata";

  /**
   * Name of key for wrapping kafka messages consumed by plasma backends
   */
  public static final String AES_KAFKA_PLASMA_BACKEND_IN = "warp.aes.kafka.plasma.backend.in";

  /**
   * Name of key for wrapping kafka messages produced by plasma backends
   */
  public static final String AES_KAFKA_PLASMA_BACKEND_OUT = "warp.aes.kafka.plasma.backend.out";

  /**
   * Name of key for wrapping kafka messages consumed by plasma frontends
   */
  public static final String AES_KAFKA_PLASMA_FRONTEND_IN = "warp.aes.kafka.plasma.frontend.in";

  /**
   * Name of key for wrapping WebCall requests in Kafka
   */
  public static final String AES_KAFKA_WEBCALL = "warp.aes.kafka.webcall";

  /**
   * AES key to use for wrapping sensitive logging messages
   */
  public static final String AES_LOGGING = "warp.aes.logging";

  /**
   * Name of key for wrapping metadata
   */
  public static final String AES_LEVELDB_METADATA = "warp.aes.leveldb.metadata";

  /**
   * Name of key for wrapping data (readings)
   */
  public static final String AES_LEVELDB_DATA = "warp.aes.leveldb.data";

  /**
   * Key for wrapping secure scripts
   */
  public static final String AES_SECURESCRIPTS = "warp.aes.securescripts";

  /**
   * Key for wrapping MetaSets
   */
  public static final String AES_METASETS = "warp.aes.metasets";

  /**
   * Key for wrapping GTSSplit instances
   */
  public static final String AES_FETCHER = "warp.aes.fetcher";

  /**
   * Get key byte [ ].
   *
   * @param name the name
   * @return the byte [ ]
   */
  public byte[] getKey(String name);

  /**
   * Sets key.
   *
   * @param name the name
   * @param key  the key
   */
  public void setKey(String name, byte[] key);

  /**
   * Decode key byte [ ].
   *
   * @param encoded the encoded
   * @return the byte [ ]
   */
  public byte[] decodeKey(String encoded);

  public KeyStore clone();

  /**
   * Forget.
   */
  public void forget();



  /**
   * Get a key from the configuration, and, if it exists and if the number of bits of the key is in the given values,
   * add it to the keystore.
   * @param keystore The KeyStore to add the key to.
   * @param keystoreKey The key name under which to add the key in the keystore.
   * @param props The properties from which to get the key.
   * @param configurationKey The configuration key holding the key value.
   * @param sizeInBits The valid number of bits of the key. Typically 128 or 128, 192, 256.
   * @return The key.
   */
  public static byte[] checkAndSetKey(KeyStore keystore, String keystoreKey, Properties props, String configurationKey, int... sizeInBits) {
    return checkAndSetKey(keystore, keystoreKey, props, configurationKey, null, sizeInBits);
  }

  /**
   * Get a key from the configuration, and, if it exists or has a non-null default and if the number of bits of the key
   * is in the given values, add it to the keystore.
   * @param keystore The KeyStore to add the key to.
   * @param keyname The key name under which to add the key in the keystore.
   * @param props The properties from which to get the key.
   * @param configurationKey The configuration key holding the key value.
   * @param defaultKeyValue The default key in case it is not found in the properties.
   * @param sizeInBits The valid number of bits of the key. Typically 128 or 128, 192, 256.
   * @return The key.
   */
  public static byte[] checkAndSetKey(KeyStore keystore, String keyname, Properties props, String configurationKey, String defaultKeyValue, int... sizeInBits) {
    String keyspec = props.getProperty(configurationKey, defaultKeyValue);

    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);

      // Check the size of the key
      StringBuilder sizesStr = new StringBuilder();
      boolean correctSize = false;
      for (int sizeIndex = 0; sizeIndex < sizeInBits.length; sizeIndex++) {
        if (sizeInBits[sizeIndex] == key.length * 8) {
          correctSize = true;
          break;
        }

        if (sizeIndex > 0) {
          if (sizeIndex == sizeInBits.length - 1) {
            sizesStr.append(" or ");
          } else {
            sizesStr.append(", ");
          }
        }
        sizesStr.append(sizeInBits[sizeIndex]);
      }
      Preconditions.checkArgument(correctSize, "Key %s MUST be %s bits long.", configurationKey, sizesStr);

      keystore.setKey(keyname, key);

      return key;
    }

    return null;
  }
}
