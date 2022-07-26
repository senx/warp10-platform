//
//   Copyright 2022  SenX S.A.S.
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

package io.warp10.ext.interpolation;

import java.util.HashMap;
import java.util.Map;

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptStack;
import io.warp10.warp.sdk.Capabilities;
import io.warp10.warp.sdk.WarpScriptExtension;

public class InterpolationWarpScriptExtension extends WarpScriptExtension {

  //
  // Functions
  //

  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String,Object>();
    
    functions.put("MICROSPHEREFIT", new MICROSPHEREFIT("MICROSPHEREFIT", false));
    functions.put("SMICROSPHEREFIT", new MICROSPHEREFIT("SMICROSPHEREFIT", true));
    functions.put("BICUBICFIT", new BICUBICFIT("BICUBICFIT"));
    functions.put("TRICUBICFIT", new TRICUBICFIT("TRICUBICFIT"));
  }

  //
  // Function special configuration
  //

  static int getIntLimitValue(WarpScriptStack stack, String limitName, int defaultValue) {

    // this is the default limit value
    int val = defaultValue;

    // if the config exists with the limit name, it is used to determine the value of the limit
    if (null != WarpConfig.getProperty(limitName)) {
      val = Integer.parseInt(WarpConfig.getProperty(limitName));
    }

    // if the capability with the limit name is present within the stack, its value supersedes the default or configured limit
    String capValue = Capabilities.get(stack, limitName);
    if (null != capValue) {
      val = Integer.parseInt(capValue);
    }

    return val;
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
