//
//   Copyright 2021  SenX S.A.S.
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

package io.warp10;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class CustomThreadFactory implements ThreadFactory {

  private final String name;
  private final ThreadGroup group;
  private final boolean isDaemon;
  private final int priority;

  private final AtomicInteger threadNumber = new AtomicInteger(1);

  public CustomThreadFactory(String name) {
    this(name, null, false, Thread.NORM_PRIORITY);
  }

  public CustomThreadFactory(String name, ThreadGroup group, boolean isDaemon, int priority) {
    this.name = name;

    if (null == group) {
      SecurityManager s = System.getSecurityManager();
      if (null == s) {
        this.group = Thread.currentThread().getThreadGroup();
      } else {
        this.group = s.getThreadGroup();
      }
    } else {
      this.group = group;
    }

    this.isDaemon = isDaemon;

    this.priority = priority;
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Thread t = new Thread(group, runnable, "[" + this.name + " #" + threadNumber.getAndIncrement() + "]", 0);

    if (t.isDaemon() != isDaemon) {
      t.setDaemon(isDaemon);
    }

    if (priority != t.getPriority()) {
      t.setPriority(priority);
    }

    return t;
  }

}
