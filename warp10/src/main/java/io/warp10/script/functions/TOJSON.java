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

import io.warp10.JsonUtils;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Converts the object on top of the stack to a JSON representation
 */
public class TOJSON extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOJSON(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    //
    // Only allow the serialization of simple lists and maps, otherwise JSON might
    // expose internals
    //

    if (!validate(o)) {
      throw new WarpScriptException(getName() + " can only serialize structures containing numbers, strings, booleans, lists and maps which do not reference the same list/map multiple times.");
    }

    try {
      String json = JsonUtils.objectToJson(o, false);
      stack.push(json);
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " failed with to convert to JSON.", ioe);
    }

    return stack;
  }

  /**
   * Validate an object for serialization.
   * We only allow Number/String/Booleans and Lists/Map containing those elements or List/Map
   *
   * @return true if the object is validated, false otherwise
   */
  private static boolean validate(Object o) {

    // Allocate a list for elements, we need to ensure there are
    // no loops in the object we try to serialize so we enforce
    // a more stringent rule which forces lists/maps to not be
    // referenced several times in the structure we attempt to
    // serialize

    List<Object> elements = new ArrayList<Object>();

    int idx = 0;


    if (null == o || o instanceof Number || o instanceof String || o instanceof Boolean || o instanceof GeoTimeSerie) {
      return true;
    }

    elements.add(o);

    while (idx < elements.size()) {
      Object obj = elements.get(idx++);

      if (obj instanceof Number || obj instanceof String || obj instanceof Boolean || obj instanceof GeoTimeSerie) {
        continue;
      }

      if (obj instanceof List) {
        for (Object elt: (List) obj) {
          if (null == elt || elt instanceof Number || elt instanceof String || elt instanceof Boolean || elt instanceof GeoTimeSerie) {
            continue;
          } else if (elt instanceof List || elt instanceof Map) {
            // Check that the given list/map is not already in the structure
            if (containsElement(elements, elt)) {
              return false;
            }
            elements.add(elt);
          } else {
            return false;
          }
        }
      } else if (obj instanceof Map) {
        for (Entry<Object, Object> entry: ((Map<Object, Object>) obj).entrySet()) {
          Object elt = entry.getKey();
          if (null == elt || elt instanceof Number || elt instanceof String || elt instanceof Boolean || elt instanceof GeoTimeSerie) {
            // Ignore keys which are atomic like types
          } else if (elt instanceof List || elt instanceof Map) {
            // Check that the given list/map is not already in the structure
            if (containsElement(elements, elt)) {
              return false;
            }
            elements.add(elt);
          } else {
            return false;
          }
          elt = entry.getValue();
          if (null == elt || elt instanceof Number || elt instanceof String || elt instanceof Boolean || elt instanceof GeoTimeSerie) {
            continue;
          } else if (elt instanceof List || elt instanceof Map) {
            // Check that the given list/map is not already in the structure
            if (containsElement(elements, elt)) {
              return false;
            }
            elements.add(elt);
          } else {
            return false;
          }
        }
      } else {
        return false;
      }
    }

    return true;
  }

  /**
   * Check if an actual object reference is already in a list
   *
   * @param list
   * @param element
   * @return
   */
  private static boolean containsElement(List<Object> list, Object element) {
    for (Object o: list) {
      if (o == element) {
        return true;
      }
    }

    return false;
  }
}
