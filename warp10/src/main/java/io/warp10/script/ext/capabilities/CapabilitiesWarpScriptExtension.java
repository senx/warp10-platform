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

package io.warp10.script.ext.capabilities;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class CapabilitiesWarpScriptExtension extends WarpScriptExtension {
  
  public static final String CAPABILITIES_PREFIX = ".cap:";
  public static final String CAPABILITIES_ATTR = "stack.capabilities";
  
  public static final String CAPADD = "CAPADD";
  public static final String CAPDEL = "CAPDEL";
  public static final String CAPGET = "CAPGET";
  public static final String CAPCHECK = "CAPCHECK";
  
  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String,Object>();
    
    functions.put(CAPADD, new CAPADD(CAPADD));
    functions.put(CAPDEL, new CAPDEL(CAPDEL));
    functions.put(CAPCHECK, new CAPCHECK(CAPCHECK));
    functions.put(CAPGET, new CAPGET(CAPGET));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }  
}
