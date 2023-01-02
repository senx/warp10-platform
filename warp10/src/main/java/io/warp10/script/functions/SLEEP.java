//
//   Copyright 2023 SenX S.A.S.
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
import io.warp10.warp.sdk.Capabilities;

import java.util.concurrent.locks.LockSupport;

public class SLEEP extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public SLEEP(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_SLEEP_MAXTIME)) {
      throw new WarpScriptException(getName() + " requires capability " + WarpScriptStack.CAPABILITY_SLEEP_MAXTIME + ".");
    }

    // sleep max time capability is defined in milliseconds
    long maxSleepMs;
    try {
      maxSleepMs = Long.parseLong(Capabilities.get(stack, WarpScriptStack.CAPABILITY_SLEEP_MAXTIME));
    } catch (NumberFormatException e) {
      throw new WarpScriptException(getName() + " cannot parse capability " + WarpScriptStack.CAPABILITY_SLEEP_MAXTIME + ": '" + Capabilities.get(stack, WarpScriptStack.CAPABILITY_SLEEP_MAXTIME) + "' is not a valid LONG");
    }

    if (maxSleepMs <= 0) {
      throw new WarpScriptException(getName() + " requires capability " + WarpScriptStack.CAPABILITY_SLEEP_MAXTIME + " to be set to a value strictly greater than 0 ms.");
    }

    Object o = stack.pop();
    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a LONG period as parameter.");
    }
    long t = ((Long) o).longValue();
    // convert to milliseconds
    if ((t / Constants.TIME_UNITS_PER_MS) > maxSleepMs) {
      throw new WarpScriptException(getName() + " cannot sleep during more than " + maxSleepMs + " ms, as defined in " + WarpScriptStack.CAPABILITY_SLEEP_MAXTIME + " capability.");
    }
    // convert to nanoseconds, check for overflow
    if (t > (Long.MAX_VALUE / Constants.NS_PER_TIME_UNIT)) {
      t = Long.MAX_VALUE / Constants.NS_PER_TIME_UNIT;
    }
    
    LockSupport.parkNanos(t * Constants.NS_PER_TIME_UNIT);

    return stack;
  }

}
