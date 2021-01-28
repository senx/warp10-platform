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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.List;
import java.util.Map;

/**
 * Extracts a value from a map, list, a byte array or a String, given a key.
 */
public class GET extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public GET(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object key = stack.pop();

    Object coll = stack.pop();

    Object value;

    try {
      if (key instanceof List && coll instanceof List) {
        // The list is considered to be a kind of path. For instance [ 'a' 0 -1 ] GET is equivalent to 'a' GET 0 GET -1 GET.
        // We restrict that syntax only to top-level collections of type List because a Map can have list keys.
        // This top-level limitation allows a list of maps of strings to be addressed through this syntax.
        value = coll;
        for (Object keyElement: (List) key) {
          value = get(keyElement, value);
        }
      } else {
        value = get(key, coll);
      }
    } catch (WarpScriptException | IndexOutOfBoundsException e) {
      throw new WarpScriptException(getName() + " failed.", e);
    }

    stack.push(value);

    return stack;
  }

  /**
   * Get a value from a List, Map, byte[] or String using respectively a Long, Object, Long or Long key.
   * @param key Either an Object for a Map or a Long for List, byte[] or String. Negative indexing is possible, in that case the corresponding index is size + index.
   * @param collection Either a List, Map, byte[] or String instance.
   * @return The value at the given key.
   * @throws WarpScriptException If the collection type cannot be handled or the key is invalid for the collection type.
   */
  public static Object get(Object key, Object collection) throws WarpScriptException {
    if (collection instanceof List) {
      if (!(key instanceof Long)) {
        throw new WarpScriptException("Getting on LIST requires a LONG.");
      }

      int idx = ((Long) key).intValue();
      int size = ((List) collection).size();
      idx = computeAndCheckIndex(idx, size);
      return ((List) collection).get(idx);

    } else if (collection instanceof Map) {
      return ((Map) collection).get(key);

    } else if (collection instanceof String) {
      if (!(key instanceof Long)) {
        throw new WarpScriptException("Getting on STRING requires a LONG.");
      }

      int idx = ((Long) key).intValue();
      int size = ((String) collection).length();
      idx = computeAndCheckIndex(idx, size);
      return String.valueOf(((String) collection).charAt(idx));

    } else if (collection instanceof byte[]) {
      if (!(key instanceof Long)) {
        throw new WarpScriptException("Getting on BYTES requires a LONG.");
      }

      int idx = ((Long) key).intValue();
      int size = ((byte[]) collection).length;
      idx = computeAndCheckIndex(idx, size);
      return ((byte[]) collection)[idx] & 0xFFL;

    } else {
      throw new WarpScriptException("Invalid OBJECT to GET on.");
    }
  }

  public static int computeAndCheckIndex(int index, int size) throws WarpScriptException {
    if (index < 0) {
      index += size;
    } else if (index >= size) {
      throw new WarpScriptException("Index out of bound, " + index + " >= " + size);
    }
    if (index < 0) {
      throw new WarpScriptException("Index out of bound, " + (index - size) + " < -" + size);
    }

    return index;
  }

  public static Object nestedGet(List<Object> nestedList, List<Long> indexList) throws WarpScriptException {
    Object res = nestedList;

    for (int i = 0; i < indexList.size(); i++) {

      if (res instanceof List) {

        int idx = computeAndCheckIndex(indexList.get(i).intValue(), ((List) res).size());
        res = ((List) res).get(idx);

      } else {
        throw new WarpScriptException("Tried to get an element at a nested path that does not exist in the input list.");
      }
    }

    return res;
  }
}
