//
//   Copyright 2018  SenX S.A.S.
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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Create a new list with the elements whose indices are in the parameter list.
 * If the parameter list contains two indices [a,b] then SUBLIST
 * returns the list of elements from the lesser index to the bigger index (included).
 * If the parameter list contains more than two indices, the result of SUBLIST
 * contains all the elements at the specified indices, with possible duplicates.
 * If, instead of the parameter list, there are number, they are considered to define
 * a range. From top to bottom: step (optional), end(optional), start
 */
public class SUBLIST extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public SUBLIST(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o;
    List indices = null;
    List elements = null;
    ArrayList<Integer> intParams = new ArrayList<Integer>();

    // Get the 4 elements on top of the stack or until a list is found.
    // After this, either indices is null or longParams contains at least one long.
    for (int i = 0; i < 4; i++) {
      o = stack.pop();

      if (o instanceof List) {
        if (0 == i) { // No range defined as numbers
          indices = (List) o;
          o = stack.pop();
          if (o instanceof List) {
            elements = (List) o;
          } else {
            throw new WarpScriptException(getName() + " expects a list of indices on top of the stack and will operate on the list below it.");
          }
        } else {
          elements = (List) o;
        }
        break;
      } else if (o instanceof Number) {
        try {
          intParams.add(0, Math.toIntExact(((Number) o).longValue())); // Prepend the int parameter
        } catch (ArithmeticException ae) {
          throw new WarpScriptException(getName() + " expects number parameters to be ints.", ae);
        }
      } else {
        throw new WarpScriptException(getName() + " expects a list of indices on top of the stack or a start end step and will operate on the list below it.");
      }
    }

    // elements can be null if the 4 elements on top of the stack were numbers.
    if (null == elements) {
      throw new WarpScriptException(getName() + " expects a list of indices on top of the stack or a start end step and will operate on the list below it.");
    }

    List<Object> sublist = new ArrayList<Object>();

    if (null == indices) { // Range definition with intParams
      // If indices is null, there is at least one parameter in longParams: start.
      int start = intParams.get(0);
      int end = intParams.size() > 1 ? intParams.get(1) : -1; // Defaults to end of the list

      if (start < 0) {
        start += elements.size();
      }
      if (end < 0) {
        end += elements.size();
      }

      start = Math.max(0, Math.min(elements.size() - 1, start));
      end = Math.max(0, Math.min(elements.size() - 1, end));

      int step = 1;
      if (intParams.size() > 2) {
        step = intParams.get(2);
      } else {
        if (start > end) {
          step = -1;
        }
        // else step is already 1
      }

      if (0 == step) {
        throw new WarpScriptException(getName() + " expects the step parameter to be a strictly positive or negative integer.");
      } else if (step > 0) {
        if (start > end) {
          throw new WarpScriptException(getName() + " expects start to be before end when step is positive.");
        }
      } else {
        if (end > start) {
          throw new WarpScriptException(getName() + " expects start to be after end when step is negative.");
        }
      }

      if (step > 0) {
        for (int i = start; i <= end; i += step) {
          sublist.add(elements.get(i));
        }
      } else {
        for (int i = start; i >= end; i += step) {
          sublist.add(elements.get(i));
        }
      }
    } else if (2 == indices.size()) { // Range definition with indices
      Object a = indices.get(0);
      Object b = indices.get(1);

      if (!(a instanceof Long) || !(b instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a list of indices which are numeric integers.");
      }

      int la = ((Long) a).intValue();
      int lb = ((Long) b).intValue();

      if (la < 0) {
        la = elements.size() + la;
      }
      if (lb < 0) {
        lb = elements.size() + lb;
      }

      if (la < 0 && lb < 0) {
        la = elements.size();
        lb = elements.size();
      } else {
        la = Math.max(0, la);
        lb = Math.max(0, lb);
      }

      //
      // If at least one of the bounds is included in the list indices,
      // fix the other one
      //

      if (la < elements.size() || lb < elements.size()) {
        if (la >= elements.size()) {
          la = elements.size() - 1;
        } else if (la < -1 * elements.size()) {
          la = -1 * elements.size();
        }

        if (lb >= elements.size()) {
          lb = elements.size() - 1;
        } else if (lb < -1 * elements.size()) {
          lb = -1 * elements.size();
        }
      }

      if (la < lb) {
        if (la < elements.size()) {
          lb = Math.min(elements.size() - 1, lb);
          for (int i = la; i <= lb; i++) {
            sublist.add(elements.get(i));
          }
        }
      } else {
        if (lb < elements.size()) {
          la = Math.min(elements.size() - 1, la);
          for (int i = lb; i <= la; i++) {
            sublist.add(elements.get(i));
          }
        }
      }
    } else { // Individual elements selection
      for (Object index: indices) {
        if (!(index instanceof Long)) {
          throw new WarpScriptException(getName() + " expects a list of indices which are numeric integers.");
        }

        int idx = ((Long) index).intValue();

        if (idx >= elements.size() || (idx < -1 * elements.size())) {
          throw new WarpScriptException(getName() + " reported an out of bound index.");
        }

        if (idx >= 0) {
          sublist.add(elements.get(idx));
        } else {
          sublist.add(elements.get(elements.size() + idx));
        }
      }
    }

    stack.push(sublist);

    return stack;
  }
}
