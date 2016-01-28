//
//   Copyright 2016  Cityzen Data
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
   * Name of key for index Ids
   */
  public static final String SIPHASH_INDEX = "warp.siphash.index";
  
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
   * Name of key for wrapping index specs
   */
  public static final String AES_HBASE_INDEX = "warp.aes.hbase.index";
  
  /**
   * Name of key for wrapping Tokens
   */
  public static final String AES_TOKEN = "warp.aes.token";
  
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
   * Name of key for computing MAC for kafka index messages
   */
  public static final String SIPHASH_KAFKA_INDEX = "warp.siphash.kafka.index";

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
   * Name of key for computing MAC for kafka messages produced by plasma frontends
   */
  public static final String SIPHASH_KAFKA_PLASMA_FRONTEND_OUT = "warp.siphash.kafka.plasma.frontend.out";

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
   * Name of key for wrapping kafka run requests
   */
  public static final String AES_KAFKA_RUNNER = "warp.aes.kafka.runner";
  
  /**
   * Name of key for wrapping kafka data messages
   */
  public static final String AES_KAFKA_DATA = "warp.aes.kafka.data";

  /**
   * Name of key for wrapping kafka medata messages
   */
  public static final String AES_KAFKA_METADATA = "warp.aes.kafka.metadata";

  /**
   * Name of key for wrapping kafka index messages
   */
  public static final String AES_KAFKA_INDEX = "warp.aes.kafka.index";

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
   * Name of key for wrapping kafka messages produced by plasma frontends
   */
  public static final String AES_KAFKA_PLASMA_FRONTEND_OUT = "warp.aes.kafka.plasma.frontend.out";

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
   * Name of key for wrapping index specs
   */
  public static final String AES_LEVELDB_INDEX = "warp.aes.leveldb.index";

  /**
   * Key for wrapping secure scripts
   */
  public static final String AES_SECURESCRIPTS = "warp.aes.securescripts";
  
  public byte[] getKey(String name);
  public void setKey(String name, byte[] key);
  public byte[] decodeKey(String encoded);
  public KeyStore clone();
  public void forget();
}
