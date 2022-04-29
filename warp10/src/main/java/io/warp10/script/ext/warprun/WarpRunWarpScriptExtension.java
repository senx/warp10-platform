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

package io.warp10.script.ext.warprun;

import java.util.LinkedHashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class WarpRunWarpScriptExtension extends WarpScriptExtension {

  private static final String FLOAD = "FLOAD";
  private static final String FREAD = "FREAD";
  private static final String FSTORE= "FSTORE";
  private static final String STDIN = "STDIN";

  private static final Map<String,Object> functions;

  static {
    functions = new LinkedHashMap<String,Object>();

    functions.put(FLOAD, new FLOAD(FLOAD));
    functions.put(FREAD, new FREAD(FREAD));
    functions.put(FSTORE, new FSTORE(FSTORE));
    functions.put(STDIN, new STDIN(STDIN));
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }

}
