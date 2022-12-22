//
//   Copyright 2018-2022  SenX S.A.S.
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

package io.warp10.script.ext.token;

import java.util.HashMap;
import java.util.Map;

import io.warp10.crypto.KeyStore;
import io.warp10.standalone.Warp;
import io.warp10.warp.sdk.WarpScriptExtension;

public class TokenWarpScriptExtension extends WarpScriptExtension {

  private final Map<String, Object> functions = new HashMap<String, Object>();

  private static final KeyStore keystore;

  public static final String CAPABILITY_TOKENGEN = "tokengen";
  public static final String CAPABILITY_TOKENDUMP = "tokendump";

  static {
    keystore = Warp.getKeyStore();
  }

  public TokenWarpScriptExtension() {
    functions.put("TOKENGEN", new TOKENGEN("TOKENGEN", keystore, true));
    functions.put("TOKENDUMP", new TOKENDUMP("TOKENDUMP", keystore, true));
  }

  public TokenWarpScriptExtension(KeyStore keystore) {
    functions.put("TOKENGEN", new TOKENGEN("TOKENGEN", keystore, false));
    functions.put("TOKENDUMP", new TOKENDUMP("TOKENDUMP", keystore, false));
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
