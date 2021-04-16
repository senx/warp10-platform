//
//   Copyright 2018-2021  SenX S.A.S.
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

package io.warp10.script;

import io.warp10.WarpURLEncoder;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class NamedWarpScriptFunction {
  private String name = "##UNDEF##";
  
  public NamedWarpScriptFunction(String name) {
    this.name = name;
  }

  public String toString() {
    return refSnapshot();
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public String getName() {
    return this.name;
  }

  /**
   * Gives the snapshot to get the reference to this function.
   */
  public final String refSnapshot() {
    if (null == name) {
      return null;
    }
    try {
      return "'" + WarpURLEncoder.encode(name, StandardCharsets.UTF_8) + "' " + WarpScriptLib.FUNCREF;
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }
}
