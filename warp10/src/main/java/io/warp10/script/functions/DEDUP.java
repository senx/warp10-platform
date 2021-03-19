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

package io.warp10.script.functions;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ListRecursiveStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;

import java.util.List;

/**
 * Remove duplicates from GTS instances.
 * <p>
 * DEDUP expects an optional mapper or macro (as MAP) and GTS instances
 */
public class DEDUP extends ListRecursiveStackFunction {

  private final ElementStackFunction SIMPLE_DEDUP = new ElementStackFunction() {
    @Override
    public Object applyOnElement(Object element) throws WarpScriptException {
      if (element instanceof GeoTimeSerie) {
        return GTSHelper.dedup((GeoTimeSerie) element);
      } else {
        return UNHANDLED;
      }
    }
  };


  public DEDUP(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    Object peeked = stack.peek();
    
    if (peeked instanceof WarpScriptStack.Macro || peeked instanceof WarpScriptMapperFunction) {
      Object macroOrMapper = stack.pop();
      return new ElementStackFunction() {
        @Override
        public Object applyOnElement(Object element) throws WarpScriptException {
          if (element instanceof GeoTimeSerie) {
            List<GeoTimeSerie> res = GTSHelper.map((GeoTimeSerie) element, macroOrMapper, 0, 0, 0, false, 1, false, macroOrMapper instanceof WarpScriptStack.Macro ? stack : null, null, true);

            if (1 == res.size()) {
              return res.get(0);
            } else {
              return res;
            }
          } else {
            return UNHANDLED;
          }
        }
      };
    } else {
      return SIMPLE_DEDUP;
    }
  }

  @Override
  public String getUnhandledErrorMessage() {
    return getName() + " expects a Geo Time Series instance or a list thereof under an optional macro or mapper.";
  }

}
