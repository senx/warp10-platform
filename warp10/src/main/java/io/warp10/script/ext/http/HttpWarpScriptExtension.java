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

package io.warp10.script.ext.http;

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptStack;
import io.warp10.warp.sdk.WarpScriptExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Extension for HTTP and the associated function to change limits: MAXURLCOUNT and MAXDOWNLOADSIZE
 */
public class HttpWarpScriptExtension extends WarpScriptExtension {

  //
  // CONFIGURATION
  //

  /**
   * Maximum number of calls to HTTP in a session
   */
  public static final String WARPSCRIPT_HTTP_LIMIT = "warpscript.http.limit";
  public static final String WARPSCRIPT_HTTP_LIMIT_HARD = "warpscript.http.limit.hard";

  /**
   * Maximum cumulative size of content retrieved via calls to HTTP in a session
   */
  public static final String WARPSCRIPT_HTTP_MAXSIZE = "warpscript.http.maxsize";
  public static final String WARPSCRIPT_HTTP_MAXSIZE_HARD = "warpscript.http.maxsize.hard";

  /**
   * Allowed and excluded host patterns.
   */
  public static final String WARPSCRIPT_HTTP_HOST_PATTERNS = "warpscript.http.host.patterns";

  //
  // STACK
  //

  /**
   * Maximum number of calls to HTTP in a session
   */
  public static final String ATTRIBUTE_HTTP_LIMIT = "http.limit";
  public static final String ATTRIBUTE_HTTP_LIMIT_HARD = "http.limit.hard";

  /**
   * Number of calls to HTTP so far in the sessions
   */
  public static final String ATTRIBUTE_HTTP_COUNT = "http.count";

  /**
   * Maximum cumulative size of content retrieved via calls to HTTP in a session
   */
  public static final String ATTRIBUTE_HTTP_MAXSIZE = "http.maxsize";
  public static final String ATTRIBUTE_HTTP_MAXSIZE_HARD = "http.maxsize.hard";

  /**
   * Current  HTTP so far in the sessions
   */
  public static final String ATTRIBUTE_HTTP_SIZE = "http.size";

  //
  // DEFAULTS
  //

  public static final long DEFAULT_HTTP_LIMIT = 64L;
  public static final long DEFAULT_HTTP_MAXSIZE = 1000000L;

  //
  // ASSOCIATIONS attributes to either configuration or defaults
  //

  /**
   * Associates the attribute name to the configuration name
   */
  private static final Map<String, String> attributeToConf;

  /**
   * Associates the attribute name to its default value
   */
  private static final Map<String, Long> attributeToDefault;

  private static final Map<String, Object> functions;

  static {
    // Initialize attribute->configuration
    Map<String, String> a2c = new HashMap<String, String>();
    a2c.put(ATTRIBUTE_HTTP_LIMIT, WARPSCRIPT_HTTP_LIMIT);
    a2c.put(ATTRIBUTE_HTTP_MAXSIZE, WARPSCRIPT_HTTP_MAXSIZE);
    a2c.put(ATTRIBUTE_HTTP_LIMIT_HARD, WARPSCRIPT_HTTP_LIMIT_HARD);
    a2c.put(ATTRIBUTE_HTTP_MAXSIZE_HARD, WARPSCRIPT_HTTP_MAXSIZE_HARD);
    attributeToConf = Collections.unmodifiableMap(a2c);

    // Initialize attribute->default
    Map<String, Long> a2d = new HashMap<String, Long>();
    a2d.put(ATTRIBUTE_HTTP_LIMIT, DEFAULT_HTTP_LIMIT);
    a2d.put(ATTRIBUTE_HTTP_MAXSIZE, DEFAULT_HTTP_MAXSIZE);
    a2d.put(ATTRIBUTE_HTTP_LIMIT_HARD, DEFAULT_HTTP_LIMIT);
    a2d.put(ATTRIBUTE_HTTP_MAXSIZE_HARD, DEFAULT_HTTP_MAXSIZE);
    attributeToDefault = Collections.unmodifiableMap(a2d);

    // Create functions and map
    functions = new HashMap<String, Object>();

    functions.put("HTTP", new HTTP("HTTP"));
    functions.put("MAXURLCOUNT", new MAXURLCOUNT("MAXURLCOUNT"));
    functions.put("MAXDOWNLOADSIZE", new MAXDOWNLOADSIZE("MAXDOWNLOADSIZE"));
  }

  public Map<String, Object> getFunctions() {
    return functions;
  }

  /**
   * Get the value of the attribute in the stack, if not present get the value in the configuration and it not present either, get the default value.
   * Also set the attribute in the stack once the value is found.
   *
   * @param stack     The stack the get the attribute from, if present.
   * @param attribute The attribute name to get.
   * @return The first available value in the list: attribute value, configuration value, default.
   */
  public static Long getLongAttribute(WarpScriptStack stack, String attribute) {
    // Get the value from stack attributes if available
    Object attributeValue = stack.getAttribute(attribute);

    if (null != attributeValue) {
      return (Long) attributeValue;
    }

    // Get the value from conf or default
    String associatedConf = attributeToConf.get(attribute);

    Long longValue = attributeToDefault.get(attribute);

    // Overwrite the default with the conf, if available
    String confValue = WarpConfig.getProperty(associatedConf);
    if (null != confValue) {
      longValue = Long.valueOf(confValue);
    }

    // The the stack attribute for future usage
    stack.setAttribute(attribute, longValue);

    return longValue;
  }
}
