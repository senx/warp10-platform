//
//   Copyright 2020-2022  SenX S.A.S.
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

package io.warp10.script.ext.stackps;

import java.util.HashMap;
import java.util.Map;

import io.warp10.script.WarpScriptStack.Signal;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.warp.sdk.WarpScriptExtension;

public class StackPSWarpScriptExtension extends WarpScriptExtension {
  public static final String ATTRIBUTE_SESSION = "stackps.session";

  public static final String HEADER_SESSION = "X-Warp10-WarpScriptSession";

  public static final String CAPABILITY = "stackps";

  private static final Map<String,Object> functions;

  static {
    WarpScriptStackRegistry.enable();

    functions = new HashMap<String,Object>();

    functions.put("WSPS", new WSPS("WSPS"));
    functions.put("WSINFO", new WSINFO("WSINFO"));
    functions.put("WSSTOP", new WSKILL("WSSTOP", Signal.STOP));
    functions.put("WSKILL", new WSKILL("WSKILL", Signal.KILL));
    functions.put("WSSTOPSESSION", new WSKILLSESSION("WSSTOPSESSION", Signal.STOP));
    functions.put("WSKILLSESSION", new WSKILLSESSION("WSKILLSESSION", Signal.KILL));
    functions.put("WSNAME", new WSNAME("WSNAME", false));
    functions.put("WSSESSION", new WSNAME("WSSESSION", true));
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
