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

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Compute number of devices from a list of GTS
 */
public class DEVICES extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public DEVICES(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of GTS as input.");
    }
    
    //
    // Map of label name to set of values
    Map<String,Set<String>> labelValueSet = new HashMap<String,Set<String>>();
    
    for (Object o: (List) top) {
      if (!(o instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " expects a list of GTS as input.");
      }
      
      Map<String,String> labels = ((GeoTimeSerie) o).getMetadata().getLabels();
      
      for (Entry<String,String> entry: labels.entrySet()) {
        Set<String> labelValues = labelValueSet.get(entry.getKey());
        if (null == labelValues) {
          labelValues = new HashSet<String>();
          labelValueSet.put(entry.getKey(), labelValues);
        }
        labelValues.add(entry.getValue());
      }
    }
    
    //
    // Compute max cardinality
    //
    
    Map<String,Integer> labelCardinalities = new HashMap<String,Integer>();
    
    String label = null;
    int cardinality = 0;
    
    for (Entry<String,Set<String>> entry: labelValueSet.entrySet()) {
      int card = entry.getValue().size();
      if (card > cardinality) {
        label = entry.getKey();
        cardinality = card;
      }
    }

    stack.push(label);
    stack.push(cardinality);
    // Push per device number of GTS
    stack.push(((List) top).size() / (double) cardinality);

    return stack;
  }
}
