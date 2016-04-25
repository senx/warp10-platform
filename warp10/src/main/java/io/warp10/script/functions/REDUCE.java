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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Reduce GTS instances
 *
 * [ [GTS] [GTS] ... [labels] reducer ] REDUCE
 * 
 */
public class REDUCE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean flatten;
  
  public REDUCE(String name, boolean flatten) {
    super(name);
    this.flatten = flatten;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list as input.");
    }
        
    List<Object> params = (List<Object>) top;
   
    if (params.size() < 3) {
      throw new WarpScriptException(getName() + " expects at least 3 parameters.");
    }
    
    for (int i = 0; i < params.size() - 2; i++) {
      if (!(params.get(i) instanceof List)) {
        throw new WarpScriptException(getName() + " expects lists of geo time series as first parameter.");
      }              
    }
      
    if (null != params.get(params.size() - 2) && !(params.get(params.size() - 2) instanceof Collection<?>)) {
      throw new WarpScriptException(getName() + " expects a list of label names or null as penultimate parameter.");                
    } else {
      if (null != params.get(params.size() - 2)) {
        for (Object o: ((Collection<?>) params.get(params.size() - 2))) {
          if (! (o instanceof String)) {
            throw new WarpScriptException(getName() + " expects a list of label names as penultimate parameter.");                            
          }
        }        
      }
    }
      
    if (!(params.get(params.size() - 1) instanceof WarpScriptReducerFunction)) {
      throw new WarpScriptException(getName() + " expects a function as last parameter.");        
    }

    Collection<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();
    Collection<String> bylabels = (Collection<String>) params.get(params.size() - 2);

    for (int i = 0; i < params.size() - 2; i++) {
      series.addAll((Collection<GeoTimeSerie>) params.get(i));
    }    

    if (this.flatten) {
      stack.push(GTSHelper.reduce((WarpScriptReducerFunction) params.get(params.size() - 1), series, bylabels));
    } else {
      stack.push(GTSHelper.reduceUnflattened((WarpScriptReducerFunction) params.get(params.size() - 1), series, bylabels));
    }
    return stack;
  }
}
