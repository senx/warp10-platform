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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

/**
 * Perform Dynamic Time Warping distance computation
 * on subsequences of an array and return the indices
 * of subsequences with minimal distance and the associated distance.
 */
public class OPTDTW extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private DTW dtw = new DTW("");

  public OPTDTW(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a numeric threshold on top of the stack.");
    }
    
    double threshold = ((Number) o).doubleValue();
    
    o = stack.pop();
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a query number list below the threshold.");
    }

    double[] query = new double[((List) o).size()];
    int i = 0;
    for (Object oo: (List) o) {
      query[i++] = ((Number) oo).doubleValue();
    }
    
    o = stack.pop();
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of numbers below the query list.");
    }

    double[] sequence = new double[((List) o).size()];
    i = 0;
    for (Object oo: (List) o) {
      sequence[i++] = ((Number) oo).doubleValue();
    }

    if (sequence.length <= query.length) {
      throw new WarpScriptException(getName() + " expects the query list to be shorter than the sequence list.");
    }
    
    double mindist = Double.POSITIVE_INFINITY;
    
    List<Integer> bestmatches = new ArrayList<Integer>();
        
    for (i = 0; i <= sequence.length - query.length; i++) {
      double dist = dtw.compute(query, 0, query.length, sequence, i, query.length, mindist);
      
      if (dist < 0) {
        continue;
      }
      
      if (dist < mindist) {
        mindist = dist;
        bestmatches.clear();
      }
      
      bestmatches.add(i);
    }
    
    stack.push(bestmatches);
    stack.push(mindist);

    return stack;
  }
}
