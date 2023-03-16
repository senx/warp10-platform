//
//   Copyright 2020-2021  SenX S.A.S.
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
import io.warp10.script.WarpScriptAudit;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

import java.io.IOException;
import java.util.List;

import static io.warp10.script.WarpScriptAudit.AUDIT_TASK_3_X_MIGRATION;
import static io.warp10.script.WarpScriptAudit.AUDIT_TASK_PARSING;

public class WSAUDIT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public WSAUDIT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects an audit task");
    }
    String auditTask = (String) top;

    top = stack.pop();
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a String as WarpScript input");
    }

    try {
      stack.push(WarpScriptAudit.auditWsToJon((String) top, auditTask));
    } catch (IOException e) {
      throw new WarpScriptException(e);
    }

    return stack;
  }
}
