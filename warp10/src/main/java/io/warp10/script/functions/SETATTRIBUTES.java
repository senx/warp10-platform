//
//   Copyright 2016  Cityzen Data
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

package io.warp10.script.functions;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Set attributes on GTS
 * 
 * SETATTRIBUTES expects the following parameters on the stack:
 * 
 * 1: a map of attributes
 * 
 * If a null key exists, then the attributes will be overriden, otherwise
 * they will be modified. If an attribute has an empty or null assocaited value,
 * the attribute will be removed.
 */
public class SETATTRIBUTES extends GTSStackFunction  {

  private static final String ATTRIBUTES = "attributes";

  public SETATTRIBUTES(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    if (!(stack.get(1) instanceof GTSEncoder)) {
      return super.apply(stack);
    }
    
    Map<String,Object> params = retrieveParameters(stack);
    
    GTSEncoder encoder = (GTSEncoder) stack.peek();
    
    GeoTimeSerie gts = new GeoTimeSerie();
    gts.setMetadata(encoder.getMetadata());
    
    gts = (GeoTimeSerie) gtsOp(params, gts);
    
    encoder.setMetadata(gts.getMetadata());  
    
    return stack;
  }
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map of attributes as parameter.");
    }
        
    Map<String,Object> params = new HashMap<String, Object>();
    
    Map<String,String> attr = (Map<String,String>) top;
    
    params.put(ATTRIBUTES, attr);

    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    Map<String,String> attributes = (Map<String,String>) params.get(ATTRIBUTES);    

    Map<String,String> attr = new HashMap<String, String>();

    if (!attributes.containsKey(null)) {
      if (gts.getMetadata().getAttributesSize() > 0) {
        attr.putAll(gts.getMetadata().getAttributes());
      }
    }
    
    for (Entry<String, String> entry: attributes.entrySet()) {
      if (null == entry.getKey()) {
        continue;
      }
      if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
        throw new WarpScriptException(getName() + " attribute key and value MUST be of type String.");
      }
      if (null == entry.getValue() || "".equals(entry.getValue())) {
        attr.remove(entry.getKey());
      } else {
        attr.put(entry.getKey(), entry.getValue());
      }
    }
    
    gts.getMetadata().setAttributes(attr);
    
    return gts;
  }
}
