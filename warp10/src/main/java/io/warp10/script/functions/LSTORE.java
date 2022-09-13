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

public class LSTORE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private boolean conditional;

  public LSTORE(String name, boolean conditional) {
    super(name);
    this.conditional = conditional;
  }

  public LSTORE(String name) {
    this(name, false);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of variable names or register numbers as second argument.");
    }

    List variables = (List) o;

    // Check that each element of the list is either a LONG or a STRING
    for (Object elt: variables) {
      if (null != elt && (!(elt instanceof String) && !(elt instanceof Long))) {
        throw new WarpScriptException(getName() + " expects a list of variable names or register numbers as second argument.");
      }
    }

    o = stack.pop();
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list as first argument.");
    }

    List objects = (List) o;

    for (int i = 0; i < variables.size(); i++) {
      if (i >= objects.size()) {
        break;
      }

      Object symbol = variables.get(i);

      if (null == symbol) {
        continue;
      }

      if (symbol instanceof Long) {
        if (!conditional || null == stack.load(((Long) symbol).intValue())) {
          stack.store(((Long) symbol).intValue(), objects.get(i));
        }
      } else {
        if (!conditional || !stack.getSymbolTable().containsKey((String) symbol)) {
          stack.store((String) symbol, objects.get(i));
        }
      }
    }

    return stack;
  }
}
