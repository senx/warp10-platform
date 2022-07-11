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

package io.warp10.script.ext.sensision;

import java.util.HashMap;
import java.util.Map;

import io.warp10.WarpConfig;
import io.warp10.warp.sdk.WarpScriptExtension;

/**
 * WarpScript Extension which exposes functions to
 * set/update/clear/get Sensision metrics
 */
public class SensisionWarpScriptExtension extends WarpScriptExtension {

  private static final Map<String, Object> functions;

  public static final String READ_CAPABILITY = "sensision.read";
  public static final String WRITE_CAPABILITY = "sensision.write";
  private static final String CONF_USE_CAPABILITY = "sensision.capability";
  private static final boolean useCapability;

  static {
    functions = new HashMap<String, Object>();

    functions.put("SENSISION.EVENT", new SENSISIONEVENT("SENSISION.EVENT"));
    functions.put("SENSISION.UPDATE", new SENSISIONUPDATE("SENSISION.UPDATE"));
    functions.put("SENSISION.SET", new SENSISIONSET("SENSISION.SET"));
    functions.put("SENSISION.GET", new SENSISIONGET("SENSISION.GET"));
    functions.put("SENSISION.DUMP", new SENSISIONDUMP("SENSISION.DUMP"));
    functions.put("SENSISION.DUMPEVENTS", new SENSISIONDUMPEVENTS("SENSISION.DUMPEVENTS"));

    useCapability = "true".equals(WarpConfig.getProperty(CONF_USE_CAPABILITY, "false"));
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }

  public static boolean useCapability() {
    return useCapability;
  }
}
