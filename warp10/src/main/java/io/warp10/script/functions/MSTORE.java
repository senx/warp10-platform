//
//   Copyright 2022  SenX S.A.S.
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

public class MSTORE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private boolean conditional;

  public MSTORE(String name, boolean conditional) {
    super(name);
    this.conditional = conditional;
  }

  public MSTORE(String name) {
    this(name, false);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    List optionalKeyList = null;
    Object o = stack.pop();

    if (o instanceof List) {
      optionalKeyList = (List) o;
      o = stack.pop();
    }

    if (!(o instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a Map as argument.");
    }

    Map<Object, Object> variables = (Map<Object, Object>) o;

    // Check that each key of the map is either a LONG or a STRING
    if (null != optionalKeyList) {
      // Check that each element of the list is either a LONG or a STRING
      for (Object symbol: optionalKeyList) {
        if (null != symbol && (!(symbol instanceof String) && !(symbol instanceof Long))) {
          throw new WarpScriptException(getName() + " expects a list of variable names or register numbers as second argument.");
        }
        Object v = variables.get(symbol);

        if (symbol instanceof Long) {
          if (!conditional || null == stack.load(((Long) symbol).intValue())) {
            stack.store(((Long) symbol).intValue(), v);
          }
        } else {
          if (!conditional || !stack.getSymbolTable().containsKey((String) symbol)) {
            stack.store((String) symbol, v);
          }
        }
      }
    } else {
      for (Object elt: variables.keySet()) {
        if (null != elt && (!(elt instanceof String) && !(elt instanceof Long))) {
          throw new WarpScriptException(getName() + " expects each key to be a STRING or a LONG.");
        }
      }

      for (Map.Entry<Object, Object> entry: variables.entrySet()) {
        Object symbol = entry.getKey();

        if (null == symbol) {
          continue;
        }

        if (symbol instanceof Long) {
          if (!conditional || null == stack.load(((Long) symbol).intValue())) {
            stack.store(((Long) symbol).intValue(), entry.getValue());
          }
        } else {
          if (!conditional || !stack.getSymbolTable().containsKey((String) symbol)) {
            stack.store((String) symbol, entry.getValue());
          }
        }
      }
    }
    return stack;
  }
}
