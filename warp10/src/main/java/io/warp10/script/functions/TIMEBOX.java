//
//   Copyright 2018-2023  SenX S.A.S.
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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import io.warp10.script.WarpScriptStopException;
import org.joda.time.Instant;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStack.Signal;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;
import io.warp10.script.WarpScriptStack;

public class TIMEBOX extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  /**
   * Default timeboxing is 30s
   */
  private static final long DEFAULT_TIMEBOX_MAXTIME = 30000L;

  /**
   * Maximum timeboxing possible, 0 means no limit
   */
  private static final long TIMEBOX_MAXTIME;

  static {
    TIMEBOX_MAXTIME = Long.parseLong(WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_TIMEBOX_MAXTIME, Long.toString(DEFAULT_TIMEBOX_MAXTIME)));
  }

  private final Signal signal;
  private final boolean cap;
  private final boolean quiet;

  public TIMEBOX(String name) {
    super(name);
    this.signal = null;
    this.cap = true;
    this.quiet = false;
  }

  public TIMEBOX(String name, Signal signal, boolean cap, boolean quiet) {
    super(name);
    this.signal = signal;
    this.cap = cap;
    this.quiet = quiet;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a maximum execution time on top of the stack.");
    }

    // This is the requested time limit.
    long maxtimeParam = Math.max(0L, ((Number) top).longValue());

    // This is the maximum time limit which may be requested. 0 means no limit.
    long maxtime = !this.cap ? 0 : TIMEBOX_MAXTIME * Constants.TIME_UNITS_PER_MS;

    top = stack.pop();

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a macro.");
    }

    //
    // If TIMEBOX should cap the maximum execution time, check the capability
    //

    long maxtimeCapability = 0;

    if (this.cap && maxtime > 0 && null != Capabilities.get(stack, WarpScriptStack.CAPABILITY_TIMEBOX_MAXTIME)) {
      String val = Capabilities.get(stack, WarpScriptStack.CAPABILITY_TIMEBOX_MAXTIME).trim();

      if (val.startsWith("P")) {
        maxtimeCapability = DURATION.parseDuration(new Instant(), val, true, false);
      } else {
        try {
          maxtimeCapability = Long.valueOf(Capabilities.get(stack, WarpScriptStack.CAPABILITY_TIMEBOX_MAXTIME)) * Constants.TIME_UNITS_PER_MS;
        } catch (NumberFormatException nfe) {
          throw new WarpScriptException(getName() + " invalid value for capability '" + WarpScriptStack.CAPABILITY_TIMEBOX_MAXTIME + "'.");
        }
      }

      // make sure value is positive
      maxtimeCapability = Math.max(0, maxtimeCapability);

      // If the capability specified a limit, raise the maximum time limit
      if (maxtimeCapability > 0) {
        maxtime = Math.max(maxtime, maxtimeCapability);
      } else {
        maxtime = 0;
      }
    }

    if (this.cap && maxtimeParam > 0) {
      if (maxtime > 0) {
        maxtime = Math.min(maxtime, maxtimeParam);
      } else {
        // No bound was set, use the limit provided as parameter
        maxtime = maxtimeParam;
      }
    } else {
      maxtime = maxtimeParam;
    }

    final Macro macro = (Macro) top;
    final WarpScriptStack fstack = stack;

    Boolean timeboxed = Boolean.TRUE.equals(fstack.getAttribute(WarpScriptStack.ATTRIBUTE_TIMEBOXED));
    fstack.setAttribute(WarpScriptStack.ATTRIBUTE_TIMEBOXED, true);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<Object> future = executorService.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        fstack.exec(macro);
        return fstack;
      }
    });

    try {
      future.get(maxtime, Constants.timeunit);
    } catch (TimeoutException te) {
      if (null != signal) {
        stack.signal(signal);
      }
      throw new WarpScriptException(getName() + " reached the execution time limit (" + maxtime + " " + Constants.timeunit.name() + ").");
    } catch (ExecutionException ee) {
      if (ee.getCause() instanceof WarpScriptStopException) {
        // Do not rethrow, this is a STOP
      } else if (this.quiet && ee.getCause() instanceof WarpScriptException) {
        throw (WarpScriptException) ee.getCause();
      } else {
        throw new WarpScriptException(getName() + " encountered an exception while executing macro", ee.getCause());
      }
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " encountered an exception", e);
    } finally {
      try { executorService.shutdownNow(); } catch (SecurityException ignore) {}
      fstack.setAttribute(WarpScriptStack.ATTRIBUTE_TIMEBOXED, timeboxed);
      if (!executorService.isShutdown()) {
        throw new WarpScriptException(getName() + " could not be properly shut down.");
      }
    }

    return stack;
  }
}
