//
//   Copyright 2021-2023  SenX S.A.S.
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

package io.warp10.script.ext.http;

import io.warp10.warp.sdk.WarpScriptExtension;

import java.util.HashMap;
import java.util.Map;

/**
 * Extension for HTTP function
 */
public class HttpWarpScriptExtension extends WarpScriptExtension {

  //
  // Web control
  //

  /**
   * Allowed and excluded host patterns.
   */
  public static final String WARPSCRIPT_HTTP_HOST_PATTERNS = "warpscript.http.host.patterns";

  //
  // Stack attributes
  //

  /**
   * Number of calls to HTTP so far in the sessions and cap name for raising related limit
   */
  public static final String ATTRIBUTE_HTTP_REQUESTS = "http.requests";

  /**
   * Current HTTP so far in the sessions and cap name for raising related limit
   */
  public static final String ATTRIBUTE_HTTP_SIZE = "http.size";

  /**
   * Cap name for raising max chunk size
   */
  public static final String ATTRIBUTE_CHUNK_SIZE = "http.chunksize";

  //
  // Configurable limits (can be raised with capabilities)
  //

  /**
   * Maximum number of calls to HTTP
   */
  public static final String WARPSCRIPT_HTTP_REQUESTS = "warpscript.http.maxrequests";

  /**
   * Maximum cumulative size allowed to be downloaded by HTTP
   */
  public static final String WARPSCRIPT_HTTP_SIZE = "warpscript.http.maxsize";

  /**
   * Maximum chunk size allowed when downloading per chunk using HTTP
   */
  public static final String WARPSCRIPT_CHUNK_SIZE = "warpscript.http.maxchunksize";

  /**
   * Configuration for maximum allowed timeout (in milliseconds)
   */
  public static final String WARPSCRIPT_HTTP_TIMEOUT = "warpscript.http.maxtimeout";

  /**
   * Capability name for maximum allowed timeout (capability value in milliseconds)
   */
  public static final String CAPABILITY_HTTP_TIMEOUT = "http.maxtimeout";

  //
  // Defaults limits if configuration not present
  //

  public static final long DEFAULT_HTTP_REQUESTS = 1L;
  public static final long DEFAULT_HTTP_MAXSIZE = 65536L;
  public static final long DEFAULT_HTTP_CHUNK_SIZE = 65536L;
  public static final int DEFAULT_HTTP_TIMEOUT = 60000; 

  //
  // Init extension
  //

  private static final Map<String, Object> functions = new HashMap<String, Object>();
  static {
    functions.put("HTTP", new HTTP("HTTP"));
  }

  public Map<String, Object> getFunctions() {
    return functions;
  }
}
