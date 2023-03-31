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

import io.warp10.continuum.Configuration;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateCryptoKey {

  private static final String CLASS_HASH_KEY = "class.hash.key";
  private static final String LABELS_HASH_KEY = "labels.hash.key";
  private static final String TOKEN_HASH_KEY = "token.hash.key";
  private static final String APP_HASH_KEY = "app.hash.key";
  private static final String TOKEN_AES_KEY = "token.aes.key";
  private static final String SCRIPTS_AES_KEY = "scripts.aes.key";
  private static final String METASETS_AES_KEY = "metasets.aes.key";
  private static final String LOGGING_AES_KEY = "logging.aes.key";
  private static final String FETCH_HASH_KEY = "fetch.hash.key";

  private static final Logger LOG = LoggerFactory.getLogger(GenerateCryptoKey.class);
  private static final SecureRandom sr = new SecureRandom();

  private static final Map<String, Integer> keys = new HashMap<String, Integer>();

  static {
    // Always generate the longest key possible.
    keys.put(Configuration.WARP_HASH_CLASS, 128);
    keys.put(Configuration.WARP_HASH_LABELS, 128);
    keys.put(Configuration.WARP_HASH_TOKEN, 128);
    keys.put(Configuration.WARP_HASH_APP, 128);
    keys.put(Configuration.WARP_AES_TOKEN, 256);
    keys.put(Configuration.WARP_AES_SCRIPTS, 256);
    keys.put(Configuration.WARP_AES_METASETS, 256);
    keys.put(Configuration.WARP_AES_LOGGING, 256);
    keys.put(Configuration.LEVELDB_METADATA_AES, 256);
    keys.put(Configuration.LEVELDB_DATA_AES, 256);
    keys.put(Configuration.CONFIG_FETCH_PSK, 128);
    keys.put(Configuration.RUNNER_PSK, 256);
    keys.put(Configuration.RUNNER_KAFKA_MAC, 128);
    keys.put(Configuration.STORE_KAFKA_DATA_MAC, 128);
    keys.put(Configuration.STORE_KAFKA_DATA_AES, 256);
    keys.put(Configuration.STORE_FDB_DATA_AES, 256);
    keys.put(Configuration.DIRECTORY_KAFKA_METADATA_MAC, 128);
    keys.put(Configuration.DIRECTORY_KAFKA_METADATA_AES, 256);
    keys.put(Configuration.DIRECTORY_FDB_METADATA_AES, 256);
    keys.put(Configuration.DIRECTORY_PSK, 128);
    keys.put(Configuration.PLASMA_FRONTEND_KAFKA_MAC, 128);
    keys.put(Configuration.PLASMA_FRONTEND_KAFKA_AES, 256);
    keys.put(Configuration.PLASMA_BACKEND_KAFKA_IN_MAC, 128);
    keys.put(Configuration.PLASMA_BACKEND_KAFKA_IN_AES, 256);
    keys.put(Configuration.PLASMA_BACKEND_KAFKA_OUT_MAC, 128);
    keys.put(Configuration.PLASMA_BACKEND_KAFKA_OUT_AES, 256);
    keys.put(Configuration.INGRESS_KAFKA_META_MAC, 128);
    keys.put(Configuration.INGRESS_KAFKA_DATA_MAC, 128);
    keys.put(Configuration.INGRESS_KAFKA_META_AES, 256);
    keys.put(Configuration.INGRESS_KAFKA_DATA_AES, 256);
    keys.put(Configuration.EGRESS_FDB_DATA_AES, 256);
    keys.put(Configuration.EGRESS_FETCHER_AES, 256);
    keys.put(CLASS_HASH_KEY, 128);
    keys.put(LABELS_HASH_KEY, 128);
    keys.put(TOKEN_HASH_KEY, 128);
    keys.put(APP_HASH_KEY, 128);
    keys.put(TOKEN_AES_KEY, 256);
    keys.put(SCRIPTS_AES_KEY, 256);
    keys.put(METASETS_AES_KEY, 256);
    keys.put(LOGGING_AES_KEY, 256);
    keys.put(FETCH_HASH_KEY, 128);
  }


  public static void main(String[] args) {
    if (0 == args.length) {
      LOG.error("GenerateCryptoKey expects 1 or more configuration filenames");
    }

    GenerateCryptoKey.fillConfigFile(args);
  }

  public static String generateCryptoKey(final int size) {
    byte[] key = new byte[size / 8];
    sr.nextBytes(key);
    return new String(Hex.encode(key));
  }

  public static void fillConfigFile(final String[] configFiles) {
    // Generate crypto key in each file passed as argument
    for (String configFile: configFiles) {
      // Whether the file should be rewritten or not.
      boolean rewrite = false;

      Path configPath = Paths.get(configFile);
      // Read current file
      try {
        List<String> lines = Files.readAllLines(configPath);

        // Inline replace.
        for (int i = 0; i < lines.size(); i++) {
          String line = lines.get(i);

          // Search of any of the keys.
          for (Map.Entry<String, Integer> keyEntry: keys.entrySet()) {
            // Only look for "hex:hhh" in case the template configuration contains pre-filled keys.
            int indexOfLine = line.indexOf(keyEntry.getKey() + " = hex:hhh");
            if (indexOfLine >= 0) {
              // Replace, keeping the start of the line, so that we're leaving commented lines commented.
              String replacedLine = line.substring(0, indexOfLine) + keyEntry.getKey() + " = hex:" + generateCryptoKey(keyEntry.getValue());
              lines.set(i, replacedLine);
              rewrite = true;
              break; // Key found, no need to look for another key.
            }
          }
        }

        if (rewrite) {
          Files.write(configPath, lines);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
