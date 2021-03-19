//
//   Copyright 2019-2020  SenX S.A.S.
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
import io.warp10.script.WarpScriptATCException;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EmptyStackException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sorts a list or a LinkedHashMap using a comparator macro.
 *
 * In the case of a List, the comparator macro is given:
 * TOP: b
 * 2:   a
 * And should push a negative Long if a less than b, a positive one if a more than b or 0L if they are equal.
 * This Long must be within the bounds of a 32-bit integer.
 *
 * In the case of a LinkedHashMap, the comparator macro is given:
 * TOP: b.value
 * 2:   b.key
 * 2:   a.value
 * 2:   a.key
 * And should push a negative Long if a less than b, a positive one if a more than b or 0L if they are equal.
 * This Long must be within the bounds of a 32-bit integer.
 */
public class SORTWITH extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  // Unchecked exception to wrap checked ones, thus allowing Compare.compare to throw it.
  // Also nicely formats the error message for unchecked exceptions thrown during the comparison.
  private static class ComparisonException extends RuntimeException {
    public ComparisonException(Exception e) {
      super(e);
    }
  }

  public SORTWITH(String name) {
    super(name);
  }

  @Override
  public Object apply(final WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof WarpScriptStack.Macro)) {
      throw new WarpScriptException(getName() + " expects a macro on top of the stack.");
    }

    final WarpScriptStack.Macro macro = (WarpScriptStack.Macro) top;

    top = stack.pop();

    if (top instanceof List) {
      List list = (List) top;

      try {
        list.sort(new Comparator() {
          @Override
          public int compare(Object o1, Object o2) {
            try {
              stack.push(o1);
              stack.push(o2);
              stack.exec(macro);

              Object topComp = stack.pop();

              if (!(topComp instanceof Long)) {
                throw new WarpScriptException(getName() + " was given a macro which doesn't return a LONG. This LONG must be within the bounds of a 32-bit integer.");
              }

              return Math.toIntExact((Long) topComp);
            } catch (WarpScriptException | ArithmeticException | EmptyStackException e) {
              // Wrap in a unchecked exception for Comparator.compare to be able to throw it.
              throw new ComparisonException(e);
            }
          }
        });
      } catch (ComparisonException ce) {
        // Rethrow encapsulated WarpScriptATCException, which allows STOP to work, for instance.
        if (ce.getCause() instanceof WarpScriptATCException) {
          throw (WarpScriptATCException) ce.getCause();
        }
        throw new WarpScriptException(getName() + " encountered an error with comparator: " + ce.getCause().getMessage(), ce);
      }

      stack.push(list);
    } else if (top instanceof LinkedHashMap) {
      LinkedHashMap linkedHashMap = (LinkedHashMap) top;
      ArrayList<Map.Entry> entryList = new ArrayList<Map.Entry>(linkedHashMap.entrySet());

      try {
        entryList.sort(new Comparator<Map.Entry>() {
          @Override
          public int compare(Map.Entry entry1, Map.Entry entry2) {
            try {
              stack.push(entry1.getKey());
              stack.push(entry1.getValue());
              stack.push(entry2.getKey());
              stack.push(entry2.getValue());
              stack.exec(macro);

              Object topComp = stack.pop();

              if (!(topComp instanceof Long)) {
                throw new WarpScriptException(getName() + " was given a macro which doesn't return a LONG. This LONG must be within the bounds of a 32-bit integer.");
              }

              return Math.toIntExact((Long) topComp);
            } catch (WarpScriptException | ArithmeticException | EmptyStackException e) {
              // Wrap in a unchecked exception for Comparator.compare to be able to throw it.
              throw new ComparisonException(e);
            }
          }
        });
      } catch (ComparisonException ce) {
        throw new WarpScriptException(getName() + " encountered an error with comparator: " + ce.getCause().getMessage(), ce);
      }

      linkedHashMap.clear();

      for (Map.Entry entry: entryList) {
        linkedHashMap.put(entry.getKey(), entry.getValue());
      }

      stack.push(linkedHashMap);
    } else {
      throw new WarpScriptException(getName() + " operates on a list or a map created by {} or ->MAP.");
    }

    return stack;
  }

}
