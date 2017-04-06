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
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Apply rename on GTS instances
 * 
 * RENAME expects the following parameters on the stack:
 * 
 * 1: name The new name
 */
public class RENAME extends GTSStackFunction  {
  
  private static final String NAME = "name";

  public RENAME(String name) {
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

    Map<String,Object> params = new HashMap<String, Object>();
    
    params.put(NAME, top.toString());

    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {

    String name = (String) params.get(NAME);
    return GTSHelper.rename(gts ,name);
  }
}
