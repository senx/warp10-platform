//
//   Copyright 2016  Cityzen Data
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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.boon.json.JsonSerializer;
import org.boon.json.JsonSerializerFactory;

/**
 * Converts the object on top of the stack to a JSON representation
 */
public class TOJSON extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final JsonSerializerFactory BOON_SERIALIZER_FACTORY = new JsonSerializerFactory();

  public TOJSON(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    JsonSerializer parser = BOON_SERIALIZER_FACTORY.create();
    
    //
    // Only allow the serialization of simple lists and maps, otherwise JSON might
    // expose internals
    //
    
    if (!validate(o)) {
      throw new WarpScriptException(getName() + " can only serialize structures containing numbers, strings, booleans, lists and maps which do not reference the same list/map multiple times.");
    }

    String json = parser.serialize(o).toString();
    
    stack.push(json);
    
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
    
    
    if (o instanceof Number || o instanceof String || o instanceof Boolean) {
      return true;
    }
    
    elements.add(o);
    
    while(idx < elements.size()) {
      Object obj = elements.get(idx++);
      
      if (obj instanceof Number || obj instanceof String || obj instanceof Boolean) {
        continue;
      }

      if (obj instanceof List) {
        for (Object elt: (List) obj) {
          if (elt instanceof Number || elt instanceof String || elt instanceof Boolean) {
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
        for (Entry<Object,Object> entry: ((Map<Object,Object>) obj).entrySet()) {
          Object elt = entry.getKey();
          if (elt instanceof Number || elt instanceof String || elt instanceof Boolean) {
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
          if (elt instanceof Number || elt instanceof String || elt instanceof Boolean) {
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
