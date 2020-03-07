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

import java.util.HashMap;
import java.util.Map;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStackRegistry;

public class WSINFO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String[] EXPOSED_ATTRIBUTES = new String[] {
      WarpScriptStack.ATTRIBUTE_CREATION_TIME,
      WarpScriptStack.ATTRIBUTE_FETCH_COUNT,
      WarpScriptStack.ATTRIBUTE_GTS_COUNT,
      WarpScriptStack.ATTRIBUTE_MACRO_NAME,
      WarpScriptStack.ATTRIBUTE_NAME,
      WarpScriptStack.ATTRIBUTE_SECTION_NAME,
  };
  
  public WSINFO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    //
    // A non null stackps secret was configured, check it
    //
    String secret = StackPSWarpScriptExtension.STACKPS_SECRET;
    
    if (null != secret) {     
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a secret.");
      }
      if (!secret.equals(top)) {
        throw new WarpScriptException(getName() + " invalid secret.");
      }
      
      top = stack.pop();
    }      

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a UUID.");
    }
    
    Map<Object,Object> infos = new HashMap<Object,Object>();

    String uuid = top.toString();
    
    for (WarpScriptStack stck: WarpScriptStackRegistry.stacks()) {
      if (uuid.equals(stck.getUUID())) {
        infos.put("uuid", stck.getUUID());
        
        Map<String,Object> attributes = new HashMap<String,Object>();
        infos.put("attributes", attributes);
        
        for (String attr: EXPOSED_ATTRIBUTES) {
          attributes.put(attr, stck.getAttribute(attr));          
        }        
      }
    }
    
    stack.push(infos);
    
    return stack;
  }
}