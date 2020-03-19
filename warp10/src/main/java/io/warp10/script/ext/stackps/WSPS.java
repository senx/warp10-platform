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

package io.warp10.script.ext.stackps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStackRegistry;

public class WSPS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String KEY_NAME = "name";
  private static final String KEY_UUID = "uuid";
  private static final String KEY_SESSION = "session";
  private static final String KEY_SECTION = "section";
  private static final String KEY_TIME = "time";
  
  public WSPS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    //
    // A non null stackps secret was configured, check it
    //
    String secret = StackPSWarpScriptExtension.STACKPS_SECRET;
    
    if (null != secret) {
      Object top = stack.pop();
      
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a secret.");
      }
      if (!secret.equals(top)) {
        throw new WarpScriptException(getName() + " invalid secret.");
      }
    }      

    List<Object> results = new ArrayList<Object>();
    
    for (WarpScriptStack stck: WarpScriptStackRegistry.stacks()) {
      Map<Object,Object> result = new HashMap<Object,Object>();
      
      result.put(KEY_UUID, stck.getUUID());
      result.put(KEY_TIME, stck.getAttribute(WarpScriptStack.ATTRIBUTE_CREATION_TIME));
      result.put(KEY_NAME, stck.getAttribute(WarpScriptStack.ATTRIBUTE_NAME));
      result.put(KEY_SESSION, stck.getAttribute(StackPSWarpScriptExtension.ATTRIBUTE_SESSION));
      result.put(KEY_SECTION, stck.getAttribute(WarpScriptStack.ATTRIBUTE_SECTION_NAME));
      
      results.add(result);
    }
    
    stack.push(results);
    
    return stack;
  }
}
