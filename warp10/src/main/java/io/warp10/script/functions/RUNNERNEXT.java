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

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.ext.http.HttpWarpScriptExtension;
import io.warp10.warp.sdk.Capabilities;

/**
 * Extract the content of a Runner Nonce
 */
public class RUNNERNEXT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private byte[] runnerPSK;

  public RUNNERNEXT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPNAME_RUNNER_RESCHEDULE_MIN_PERIOD)) {
      throw new WarpScriptException(getName() + " requires capability " + WarpScriptStack.CAPNAME_RUNNER_RESCHEDULE_MIN_PERIOD + ".");
    }

    // rescheduling capability is defined in milliseconds (as runners subdirectories)
    Long minPeriod = Long.valueOf(Capabilities.get(stack, WarpScriptStack.CAPNAME_RUNNER_RESCHEDULE_MIN_PERIOD));

    if (minPeriod<=0) {
      throw new WarpScriptException(getName() + " requires capability " + WarpScriptStack.CAPNAME_RUNNER_RESCHEDULE_MIN_PERIOD + " strictly greater than 0ms.");
    }
    
    Object o = stack.pop();
    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a LONG period as parameter.");
    }
    // convert to milliseconds    
    Long p = ((Long) o) / Constants.TIME_UNITS_PER_MS;
    if (p < minPeriod) {
      throw new WarpScriptException(getName() + " cannot set period below " + minPeriod + "ms defined in " + WarpScriptStack.CAPNAME_RUNNER_RESCHEDULE_MIN_PERIOD + " capability.");
    }

    // store required period as a stack attribute
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_RUNNER_RESCHEDULE_PERIOD, p);

    return stack;
  }

}
