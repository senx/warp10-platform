//
//   Copyright 2018  SenX S.A.S.
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
import java.util.Properties;

import io.warp10.WarpConfig;
import io.warp10.WarpDist;
import io.warp10.crypto.KeyStore;
import io.warp10.standalone.Warp;
import io.warp10.warp.sdk.WarpScriptExtension;

public class TokenWarpScriptExtension extends WarpScriptExtension {
  
  /*
   *  Name of configuration key with the token secret. 
   */
  public static final String CONF_TOKEN_SECRET = "token.secret";
  
  /**
   * Current Token Secret
   */
  public static String TOKEN_SECRET = null;
  
  private static final Map<String,Object> functions = new HashMap<String,Object>();
  private static final KeyStore keystore;
  
  static {
    TOKEN_SECRET = WarpConfig.getProperty(CONF_TOKEN_SECRET);

    if (null != TOKEN_SECRET) {
      keystore = Warp.getKeyStore();
    } else {
      keystore = null;
    }
  }
  
  public TokenWarpScriptExtension() {
    if (null != keystore) {
      functions.put("TOKENGEN", new TOKENGEN("TOKENGEN", keystore));
      functions.put("TOKENDUMP", new TOKENDUMP("TOKENDUMP", keystore));      
    } else {
      functions.put("TOKENGEN", new TOKENGEN("TOKENGEN"));
      functions.put("TOKENDUMP", new TOKENDUMP("TOKENDUMP"));
    }
    functions.put("TOKENSECRET", new TOKENSECRET("TOKENSECRET"));
  }
  
  public TokenWarpScriptExtension(KeyStore keystore) {
    functions.put("TOKENGEN", new TOKENGEN("TOKENGEN", keystore));
    functions.put("TOKENDUMP", new TOKENDUMP("TOKENDUMP", keystore));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
