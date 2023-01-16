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

package io.warp10.script.ext.shm;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

public class MUTEX extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  static final String MUTEX_ATTRIBUTE = "ext.shm.mutex";

  public MUTEX(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    String cap = Capabilities.get(stack, SharedMemoryWarpScriptExtension.CAPABILITY_MUTEX);

    if (null == cap) {
      throw new WarpScriptException(getName() + " expected capability '" + SharedMemoryWarpScriptExtension.CAPABILITY_MUTEX + "' to be set.");
    }

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects the mutex name on top of the stack.");
    }

    String mutex = String.valueOf(top);

    if(null != stack.getAttribute(MUTEX_ATTRIBUTE + stack.getUUID()) && mutex != stack.getAttribute(MUTEX_ATTRIBUTE + stack.getUUID())) {
      throw new WarpScriptException(getName() + " calls can only be nested with the same mutex.");
    }

    top = stack.pop();

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects the macro to run below the mutex name.");
    }

    Macro macro = (Macro) top;

    if (!"".equals(cap) && !Pattern.matches(cap, mutex)) {
      throw new WarpScriptException(getName() + " capability does not grant access to mutex '" + mutex + "'.");
    }

    ReentrantLock lock = SharedMemoryWarpScriptExtension.getLock(mutex);

    long maxwait = SharedMemoryWarpScriptExtension.MUTEX_DEFAULT_MAXWAIT;

    if (null != Capabilities.get(stack, SharedMemoryWarpScriptExtension.CAPABILITY_MUTEX_MAXWAIT)) {
      try {
        maxwait = Long.parseLong(Capabilities.get(stack, SharedMemoryWarpScriptExtension.CAPABILITY_MUTEX_MAXWAIT));
      } catch(NumberFormatException nfe) {
        throw new WarpScriptException(getName() + " invalid '" + SharedMemoryWarpScriptExtension.CAPABILITY_MUTEX_MAXWAIT + "' capability value.");
      }
    }

    boolean locked = false;
    boolean clearAttr = null == stack.getAttribute(MUTEX_ATTRIBUTE + stack.getUUID());

    try {
      if (0 == maxwait) {
        lock.lockInterruptibly();
        locked = true;
      } else {
        locked = lock.tryLock(maxwait, TimeUnit.MILLISECONDS);

        if (!locked) {
          throw new WarpScriptException(getName() + " failed to acquire mutex within " + maxwait + " ms.");
        }
      }
      stack.setAttribute(MUTEX_ATTRIBUTE + stack.getUUID(), mutex);
      stack.exec(macro);
    } catch (WarpScriptException wse) {
      throw wse;
    } catch (Throwable t) {
      throw new WarpScriptException("Error while running mutex macro.", t);
    } finally {
      if (lock.isHeldByCurrentThread() && locked) {
        lock.unlock();
      }
      if (clearAttr) {
        stack.setAttribute(MUTEX_ATTRIBUTE + stack.getUUID(), null);
      }
    }

    return stack;
  }
}
