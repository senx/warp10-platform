//
//   Copyright 2019-2021  SenX S.A.S.
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

package io.warp10.script.functions.shape;

import java.util.ArrayList;
import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Return a BOOLEAN indicating whether an input list and its nested lists sizes are coherent together to form a tensor (or multidimensional array).
 */
public class CHECKSHAPE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public CHECKSHAPE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object o = stack.pop();
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST.");
    }
    List list = (List) o;

    List<Long> candidateShape = SHAPE.candidate_shape(list);
    stack.push(recValidateShape(list, candidateShape));
    return stack;
  }

  private static Boolean hasNestedList(List list) {
    for (Object el: list) {
      if (el instanceof List) {
        return true;
      }
    }

    return false;
  }

  static Boolean recValidateShape(List list, List<Long> candidateShape) {
    List<Long> copyShape =  new ArrayList<Long>(candidateShape);

    if (list.size() != copyShape.remove(0)) {
      return false;
    }

    if (!hasNestedList(list)) {
      return 0 == copyShape.size();

    } else if (0 == copyShape.size()) {
      return false;

    } else {

      for (Object el: list) {
        if (!(el instanceof List)) {
          return false;
        }

        if (!recValidateShape((List) el, copyShape)) {
          return false;
        }
      }

      return true;
    }
  }

}
