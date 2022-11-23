//
//   Copyright 2018-2022  SenX S.A.S.
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

import java.util.ArrayList;
import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpFleetMacroRepository;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

public class WFSETREPOS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public WFSETREPOS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_WFSET)) {
      throw new WarpScriptException(getName() + " expects the capability '" + WarpScriptStack.CAPABILITY_WFSET + "' to be set.");
    }

    Object top = stack.pop();

    if (null != top && !(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of repository URLs on top of the stack.");
    }

    List<String> repos = null;

    if (null != top) {
      repos = new ArrayList<String>(((List<Object>) top).size());
      for (Object elt: (List<Object>) top) {
        if (!(elt instanceof String)) {
          throw new WarpScriptException(getName() + " expects a list of repository URLs on top of the stack.");
        }
        repos.add(elt.toString());
      }
    }

    WarpFleetMacroRepository.setRepos(stack, repos);

    return stack;
  }
}
