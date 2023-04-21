//
//   Copyright 2019-2023  SenX S.A.S.
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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.GET;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Permute the dimensions of a nested LIST as if it were a tensor or a multidimensional array.
 * - param TENSOR The nested LIST for which dimensions will be permuted as if it were a tensor.
 * - param PATTERN The permutation pattern (a LIST of LONG)
 * - param FAST If true, it does not check if the sizes of the nested lists are coherent before operating. Defaults to false.
 */
public class PERMUTE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public PERMUTE(String name) {
    super(name);
  }

  @Override
  public WarpScriptStack apply(WarpScriptStack stack) throws WarpScriptException {

    Object o = stack.pop();

    //
    // 3rd optional argument
    //

    boolean fast = false;
    if (o instanceof Boolean) {
      fast = Boolean.TRUE.equals(o);
      o = stack.pop();
    }

    //
    // 2nd argument
    //

    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST of LONG as second argument.");
    }

    for (Object oo: (List) o) {
      if (!(oo instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a LIST of LONG as second argument.");
      }
    }
    List<Long> pattern = (List<Long>) o;
    o = stack.pop();

    //
    // 1st argument
    //

    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST as first argument.");
    }
    List tensor = (List) o;

    //
    // Logic
    //

    if (pattern.size() > (new HashSet<Object>(pattern)).size()){
      throw new WarpScriptException(getName() + " error: duplicate axis in permutation pattern.");
    }

    List<Long> shape = SHAPE.candidate_shape(tensor);

    if (!(fast || CHECKSHAPE.recValidateShape(tensor, shape))) {
      throw new WarpScriptException(getName() + " expects that the sizes of the nested lists are coherent together to form a tensor (or multidimensional array).");
    }

    List<Long> newShape = new ArrayList<Long>();
    for (int r = 0; r < pattern.size(); r++) {
      newShape.add(shape.get(pattern.get(r).intValue()));
    }

    List<Object> result = new ArrayList<Object>();
    recPermute(tensor, result, new ArrayList<Long>(), 0, pattern, newShape);
    stack.push(result);
    return stack;
  }

  private void recPermute(List<Object> tensor, List<Object> result, List<Long> indices, int dimension, List<Long> pattern, List<Long> newShape) throws WarpScriptException {

    for (int i = 0; i < newShape.get(dimension); i++) {
      List<Long> new_indices = new ArrayList(indices);
      new_indices.add(Long.valueOf(i));

      if (newShape.size() - 1 == dimension) {
        List<Long> permutedIndices = new ArrayList<Long>();

        for (int r = 0; r < pattern.size(); r++) {
          permutedIndices.add(new_indices.get(pattern.lastIndexOf(Long.valueOf(r))));
        }

        result.add(GET.nestedGet(tensor, permutedIndices));

      } else {

        List<Object> nested = new ArrayList<Object>();
        result.add(nested);
        recPermute(tensor, nested, new_indices, dimension + 1, pattern, newShape);
      }
    }
  }
}
