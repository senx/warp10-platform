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
 * Return the shape of an input list if it could be a tensor (or multidimensional array), or raise an Exception.
 * - param LIST The input list
 * - param FAST If true, it does not check if the sizes of the nested lists are coherent and it returns a shape based on the first nested lists at each level. Defaults to false
 */
public class SHAPE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public SHAPE(String name) {
    super(name);
  }

  @Override
  public WarpScriptStack apply(WarpScriptStack stack) throws WarpScriptException {

    Object o = stack.pop();

    //
    // 2nd optional argument
    //

    boolean fast = false;
    if (o instanceof Boolean) {
      fast = Boolean.TRUE.equals(o);
      o = stack.pop();
    }

    //
    // 1st argument
    //

    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST.");
    }
    List list = (List) o;

    //
    // Logic
    //

    List<Long> candidateShape = candidate_shape(list);

    if (fast || CHECKSHAPE.recValidateShape(list, candidateShape)) {
      stack.push(candidateShape);
    } else {
      throw new WarpScriptException(getName() + " expects that the sizes of the nested lists are coherent together to form a tensor (or multidimensional array).");
    }
    return stack;
  }

  static List<Long> candidate_shape(List list) {
    List<Long> shape = new ArrayList<Long>();
    Object firstElement = list;

    while (firstElement instanceof List) {
      List l = (List) firstElement;
      shape.add((long) (l.size()));
      if (!l.isEmpty()) {
        firstElement = l.get(0);
      } else {
        break;
      }
    }

    return shape;
  }
}