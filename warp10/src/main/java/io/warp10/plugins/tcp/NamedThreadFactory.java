package io.warp10.plugins.tcp;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class NamedThreadFactory implements ThreadFactory {
  private final ThreadGroup group;
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String name;

  public NamedThreadFactory(String name) {
    this.name = name;

    SecurityManager s = System.getSecurityManager();
    if (null == s) {
      group = Thread.currentThread().getThreadGroup();
    } else {
      group = s.getThreadGroup();
    }
  }

  public Thread newThread(Runnable r) {
    Thread t = new Thread(group, r, "[" + this.name + " #" + threadNumber.getAndIncrement() + "]", 0);

    if (t.isDaemon()) {
      t.setDaemon(false);
    }

    if (Thread.NORM_PRIORITY != t.getPriority()) {
      t.setPriority(Thread.NORM_PRIORITY);
    }

    return t;
  }
}