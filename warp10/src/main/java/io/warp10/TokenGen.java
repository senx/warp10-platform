//
//   Copyright 2018-2023  SenX S.A.S.
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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Properties;

import io.warp10.continuum.Configuration;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OSSKeyStore;
import io.warp10.crypto.UnsecureKeyStore;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.ext.token.TokenWarpScriptExtension;
import io.warp10.standalone.Warp;

public class TokenGen {
  public static void main(String[] args) throws Exception {
    if (StandardCharsets.UTF_8 != Charset.defaultCharset()) {
      throw new RuntimeException("Default encoding MUST be UTF-8 but it is " + Charset.defaultCharset() + ". Aborting.");
    }

    TokenGen instance = new TokenGen();

    instance.usage(args);

    instance.parse(args);

    instance.process(args);
  }

  public void parse(String[] args) throws Exception {
    WarpConfig.setProperties(Arrays.copyOf(args, args.length - 1));

    Properties properties = WarpConfig.getProperties();

    KeyStore keystore;

    if (properties.containsKey(Configuration.OSS_MASTER_KEY)) {
      keystore = new OSSKeyStore(properties.getProperty(Configuration.OSS_MASTER_KEY));
    } else {
      keystore = new UnsecureKeyStore();
    }

    //
    // Decode generic keys
    // We do that first so those keys do not have precedence over the specific
    // keys.
    //

    for (Entry<Object, Object> entry : properties.entrySet()) {
      if (entry.getKey().toString().startsWith(Configuration.WARP_KEY_PREFIX)) {
        byte[] key = keystore.decodeKey(entry.getValue().toString());
        if (null == key) {
          throw new RuntimeException("Unable to decode key '" + entry.getKey() + "'.");
        }
        keystore.setKey(entry.getKey().toString().substring(Configuration.WARP_KEY_PREFIX.length()), key);
      }
    }

    Warp.extractKeys(keystore, properties);

    keystore.forget();

    WarpScriptLib.registerExtensions();

    TokenWarpScriptExtension ext = new TokenWarpScriptExtension(keystore);
    WarpScriptLib.register(ext);
  }

  public void usage(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: TokenGen config ... in");
      System.exit(-1);
    }
  }

  public void process(String[] args) throws Exception {
    PrintWriter pw = new PrintWriter(System.out);

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, WarpConfig.getProperties());
    stack.maxLimits();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] buf = new byte[8192];

    InputStream in = null;

    if ("-".equals(args[args.length - 1])) {
      in = System.in;
    } else {
      in = new FileInputStream(args[args.length - 1]);
    }

    while (true) {
      int len = in.read(buf);

      if (len <= 0) {
        break;
      }

      baos.write(buf, 0, len);
    }

    in.close();

    String script = new String(baos.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);

    stack.execMulti(script);

    StackUtils.toJSON(pw, stack);

    pw.flush();
    pw.close();
  }
}
