//
//   Copyright 2020  SenX S.A.S.
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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateCryptoKey {

  private static final Logger LOG = LoggerFactory.getLogger(GenerateCryptoKey.class);
  private static final SecureRandom sr = new SecureRandom();

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
    Arrays.asList(configFiles).forEach(file ->
      {
        Path p = FileSystems.getDefault().getPath(file);
        // Read current file
        try (Stream<String> lines = Files.lines(p)) {
          List<String> replaced = lines
            .map(line -> line.replaceAll("(.* = hex:)(.*)", "$1" + generateCryptoKey(256)))
            // *.hash.* keys need shorted crypto key
            .map(line -> line.replaceAll("(.*\\.hash\\..* = hex:)(.*)", "$1" + generateCryptoKey(128)))
            .map(line -> line.replaceAll("(.*fetch.psk.* = hex:)(.*)", "$1" + generateCryptoKey(128)))
            .collect(Collectors.toList());
          Files.write(p, replaced);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    );
  }
}