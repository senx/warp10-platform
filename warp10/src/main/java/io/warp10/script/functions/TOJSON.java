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
    
    if (o instanceof List) {
      for (Object elt: (List) o) {
        if (!(elt instanceof Number) && !(elt instanceof String) && !(elt instanceof Boolean)) {
          throw new WarpScriptException(getName() + " can only handle numeric, boolean and string types.");
        }
      }
    } else if (o instanceof Map) {
      for (Entry<Object, Object> entry: ((Map<Object,Object>) o).entrySet()) {
        Object elt = entry.getKey();
        if (!(elt instanceof Number) && !(elt instanceof String) && !(elt instanceof Boolean)) {
          throw new WarpScriptException(getName() + " can only handle numeric, boolean and string types.");
        }
        elt = entry.getValue();
        if (!(elt instanceof Number) && !(elt instanceof String) && !(elt instanceof Boolean)) {
          throw new WarpScriptException(getName() + " can only handle numeric, boolean and string types.");
        }
      }
    } else {
      throw new WarpScriptException(getName() + " can only handle simple lists/maps.");
    }
    
    String json = parser.serialize(o).toString();
    
    stack.push(json);
    
    return stack;
  }
}
