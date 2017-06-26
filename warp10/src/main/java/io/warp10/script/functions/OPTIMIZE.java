//
//   Copyright 2017  Cityzen Data
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

import java.io.IOException;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.CapacityExtractorOutputStream;

/**
 * Optimize storage of GeoTimeSerie or encoder instances
 */
public class OPTIMIZE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public OPTIMIZE(String name) {
    super(name);
  }  

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Double)) {
      throw new WarpScriptException(getName() + " expects a ratio on top of the stack.");
    }
    
    double ratio = (double) top;
    
    top = stack.pop();
    
    if (top instanceof GeoTimeSerie) {
      GeoTimeSerie gts = (GeoTimeSerie) top;
      GTSHelper.shrink(gts, ratio);
      stack.push(gts);
    } else if (top instanceof GTSEncoder) {
      GTSEncoder encoder = (GTSEncoder) top;
      
      if (encoder.size() > 0) {
        CapacityExtractorOutputStream extractor = new CapacityExtractorOutputStream();
        try {
          encoder.writeTo(extractor);
          if ((double) extractor.getCapacity() / (double) encoder.size() > ratio) {
            encoder.resize(encoder.size());
          }                    
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " encountered an error while shrinking encoder.", ioe);
        }
      }      
      stack.push(encoder);
    } else {
      throw new WarpScriptException(getName() + " operates on a GeoTimeSerie or encoder instance.");
    }
    
    return stack;
  }  
}
