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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptException;
import io.warp10.warp.sdk.WarpScriptExtension;

public class SharedMemoryWarpScriptExtension extends WarpScriptExtension implements Runnable {

  private static final Map<String,Object> functions;
  private static final Map<String,ReentrantLock> locks;
  private static final Map<String,Long> lockUses;

  private static final Map<String,Object> shmobjects;
  private static final Map<String,String> shmobjectLocks;
  private static final Map<String,Long> shmobjectUses;

  private static Thread janitor = null;

  private static final long ttl;

  /**
   * TTL of shared objects and locks after their last access, in ms.
   */
  private static final String CONFIG_SHM_TTL = "shm.ttl";

  /**
   * Maximum time to wait for a mutex, in ms. If 0, then unlimited waiting is
   * allowed.
   * This value can be altered by the value of the mutex.maxwait capability.
   */
  private static final String CONFIG_MUTEX_MAXWAIT = "mutex.maxwait";

  /**
   * Default timeout for MUTEX is 5 s
   */
  private static final String DEFAULT_MUTEX_MAXWAIT = Long.toString(5000L);

  /**
   * Default TTL for shared objects, 1H
   */
  private static final long DEFAULT_SHM_TTL = 1 * 3600 * 1000L;

  static final long MUTEX_DEFAULT_MAXWAIT;

  static {
    MUTEX_DEFAULT_MAXWAIT = Long.parseLong(WarpConfig.getProperty(CONFIG_MUTEX_MAXWAIT, DEFAULT_MUTEX_MAXWAIT));

    if (MUTEX_DEFAULT_MAXWAIT < 0) {
      throw new RuntimeException("Invalid value for '" + CONFIG_MUTEX_MAXWAIT + "', expected value >= 0.");
    }

    locks = new HashMap<String,ReentrantLock>();
    lockUses = new HashMap<String,Long>();

    shmobjects = new HashMap<String,Object>();
    shmobjectLocks = new HashMap<String,String>();
    shmobjectUses = new HashMap<String,Long>();

    ttl = Long.parseLong(WarpConfig.getProperty(CONFIG_SHM_TTL, String.valueOf(DEFAULT_SHM_TTL)));

    functions = new HashMap<String,Object>();

    functions.put("SHMSTORE", new SHMSTORE("SHMSTORE"));
    functions.put("SHMLOAD", new SHMLOAD("SHMLOAD"));
    functions.put("SHMDEFINED", new SHMDEFINED("SHMDEFINED"));
    functions.put("MUTEX", new MUTEX("MUTEX"));
  }

  public static final String CAPABILITY_MUTEX = "mutex";
  public static final String CAPABILITY_MUTEX_MAXWAIT = "mutex.maxwait";
  public static final String CAPABILITY_SHMLOAD = "shmload";
  public static final String CAPABILITY_SHMSTORE = "shmstore";

  public SharedMemoryWarpScriptExtension() {
    //
    // Start the janitor thread if it is not yet started
    //

    synchronized(functions) {
      if (null == janitor) {
        janitor = new Thread(this);
        janitor.setDaemon(true);
        janitor.start();
      }
    }
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }

  public synchronized static final ReentrantLock getLock(String name) {
    ReentrantLock lock = locks.get(name);

    if (null == lock) {
      lock = new ReentrantLock(true);
      locks.put(name, lock);
    }

    lockUses.put(name, System.currentTimeMillis());

    return lock;
  }

  public static final void store(String symbol, String mutex, Object o) throws WarpScriptException {

    synchronized(locks) {
      if (null == o || null == mutex) {
        shmobjects.remove(symbol);
        shmobjectLocks.remove(symbol);
        shmobjectUses.remove(symbol);
        return;
      }

      //
      // If the shared memory object is already defined, return
      //
      if (null != shmobjectLocks.get(symbol)) {
        return;
      }

      shmobjects.put(symbol, o);
      shmobjectLocks.put(symbol, mutex);
      shmobjectUses.put(symbol, System.currentTimeMillis());
    }
  }

  public static final Object load(String symbol) throws WarpScriptException {

    synchronized(locks) {
      String mutex = shmobjectLocks.get(symbol);

      if (null == mutex) {
        throw new WarpScriptException("Unknown shared memory symbol '" + symbol + "'.");
      }

      ReentrantLock lock = locks.get(mutex);

      if (!lock.isHeldByCurrentThread()) {
        throw new WarpScriptException("Invalid access to shared memory symbol '" + symbol + "', not in a mutex section with mutex '" + mutex + "' held.");
      }

      Object value = shmobjects.get(symbol);

      shmobjectUses.put(symbol, System.currentTimeMillis());
      return value;
    }
  }

  public static final Object defined(String symbol) throws WarpScriptException {

    synchronized(locks) {
      String mutex = shmobjectLocks.get(symbol);

      return null != mutex;
    }
  }

  @Override
  public void run() {
    while(true) {
      try {
        LockSupport.parkNanos(Math.min(60000000000L, ttl * 500000L));

        synchronized(locks) {
          long now = System.currentTimeMillis();

          for (String symbol: shmobjects.keySet()) {
            // If SHM Object was not used for more than ttl, clear it
            // if its mutex is not currently held
            if (now - shmobjectUses.get(symbol) > ttl) {
              String mutex = shmobjectLocks.get(symbol);
              ReentrantLock lock = locks.get(mutex);
              if (!lock.isLocked()) {
                shmobjects.remove(symbol);
                shmobjectLocks.remove(symbol);
                shmobjectUses.remove(symbol);
              }
            }
          }

          for (Map.Entry<String, ReentrantLock> mutexAndLock: locks.entrySet()) {
            String mutex = mutexAndLock.getKey();
            // If lock has not been requested for over ttl, it is not associated with any shm object
            // and it is not currently held, clear it.
            if (now - lockUses.get(mutex) > ttl && !shmobjectLocks.containsValue(mutex)) {
              if (!mutexAndLock.getValue().isLocked()) {
                locks.remove(mutex);
                lockUses.remove(mutex);
              }
            }
          }
        }
      } catch (Throwable t) {
      }
    }
  }
}
