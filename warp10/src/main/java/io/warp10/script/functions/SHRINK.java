//
//   Copyright 2018-2023  SenX S.A.S.
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
import io.warp10.script.WarpScriptStack;

/**
 * Shrinks the number of values of a GTS.
 * <p>
 * This function has the side effect of sorting the GTS.
 * In case of duplicates in the input, the output will keep one of them (impossible to guess which one after the sort)
 */
public class SHRINK extends ListRecursiveStackFunction {

  public SHRINK(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a size on top of the stack.");
    }
    long shrinkto = (long) top;

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) {
        if (element instanceof GeoTimeSerie) {
          GeoTimeSerie gts = (GeoTimeSerie) element;

          if (shrinkto < 0) {
            GTSHelper.sort(gts, true);
          } else {
            GTSHelper.sort(gts, false);
          }

          if (GTSHelper.nvalues(gts) > Math.abs(shrinkto)) {
            GTSHelper.shrinkTo(gts, (int) Math.abs(shrinkto));
          }
          return gts;

        } else {
          return UNHANDLED;
        }
      }
    };

  }

  @Override
  public String getUnhandledErrorMessage() {
    return getName() + " can only handle GTS and list thereof.";
  }

}
