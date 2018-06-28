//
//   Copyright 2016  Cityzen Data
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

package io.warp10.script.ext.concurrent;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptLoopBreakException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Execute a list of macros in a concurrent manner (CEVAL),
 * or the same macro with different arguments in a concurrent manner (CFOREACH)
 */
public class CEVAL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String CONCURRENT_EXECUTION_ATTRIBUTE = "concurrent.execution";
  public static final String CONCURRENT_LOCK_ATTRIBUTE = "concurrent.lock";
  private final boolean forEach;

  public CEVAL(String name, boolean forEach) {
    super(name);
    this.forEach = forEach;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    //
    // Check if the stack is already in concurrent execution mode
    //

    if (Boolean.TRUE.equals(stack.getAttribute(CONCURRENT_EXECUTION_ATTRIBUTE))) {
      throw new WarpScriptException(getName() + " cannot be called from within a concurrent execution.");
    }

    //
    // Parallelism level
    //
    Object top = stack.pop();
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a parallelism level on top of the stack.");
    }

    int parallelism = ((Number) top).intValue();
    if (parallelism < 1) {
      throw new WarpScriptException(getName() + " parallelism level cannot be less than 1.");
    }

    //
    // CEVAL expects a list of macro
    // CFOREACH as the same signature as FOREACH under the parallelism level
    //
    int ntasks = 0;
    Object iterableobj = null;
    top = stack.pop();
    if (this.forEach) {
      // CFOREACH parameters
      if (!(top instanceof Macro)) {
        throw new WarpScriptException(getName() + " expects a macro below the parallelism level.");
      }
      iterableobj = stack.pop();
      if (!(iterableobj instanceof List) && !(iterableobj instanceof Map) && !(iterableobj instanceof Iterator) && !(iterableobj instanceof Iterable)) {
        throw new WarpScriptException(getName() + " operates on a list, map, iterator or iterable.");
      }
      if (iterableobj instanceof List) {
        ntasks = ((List) iterableobj).size();
      } else if (iterableobj instanceof Map) {
        ntasks = ((Map) iterableobj).size();
      } else {
        Iterator<Object> iter = iterableobj instanceof Iterator ? (Iterator<Object>) iterableobj : ((Iterable<Object>) iterableobj).iterator();
        while (iter.hasNext()) {
          iter.next();
          ntasks++;
        }
      }
    } else {
      // CEVAL parameters
      if (!(top instanceof List)) {
        throw new WarpScriptException(getName() + " expects a list of macros below the parallelism level.");
      }
      //
      // Check that all elements of the list are macros
      //
      for (Object o : (List) top) {
        if (!(o instanceof Macro)) {
          throw new WarpScriptException(getName() + " expects a list of macros below the parallelism level.");
        }
      }
      ntasks = ((List) top).size();
    }

    //
    // CEVAL :
    //  -top = a list of macro
    //  -iterableobj = null
    // CFOREACH :
    //  -top = a macro
    //  -iterableobj can be a List, a Map, an Iterator or Iterable.
    //

    //
    // Limit parallelism to number of macros to run or size of list or map
    //
    if (parallelism > ntasks) {
      parallelism = ntasks;
    }

    ExecutorService executor = null;

    try {
      //
      // Create a Reentrant lock for optional synchronization
      //
      ReentrantLock lock = new ReentrantLock();

      stack.setAttribute(CONCURRENT_EXECUTION_ATTRIBUTE, true);
      stack.setAttribute(CONCURRENT_LOCK_ATTRIBUTE, lock);

      BlockingQueue<Runnable> queue = new LinkedBlockingDeque<Runnable>(ntasks);
      executor = new ThreadPoolExecutor(parallelism, parallelism, 30, TimeUnit.SECONDS, queue);

      //
      // Copy the current stack context
      //
      stack.save();
      StackContext context = (StackContext) stack.pop();

      //
      // Define the tasks common atomic variables and common output.
      //
      List<Future<List<Object>>> futures = new ArrayList<Future<List<Object>>>();
      final AtomicBoolean aborted = new AtomicBoolean(false);
      final AtomicInteger pending = new AtomicInteger(0);
      final AtomicBoolean stopped = new AtomicBoolean(false);

      //
      // The task creation are slightly different between CEVAL and CFOREACH
      //
      if (this.forEach) {
        if (iterableobj instanceof List) {
          for (Object o : ((List<Object>) iterableobj)) {
            List newstackcontent = new ArrayList<Object>(1);
            newstackcontent.add(o);
            CreateTaskAndStackContext((Macro) top, stack, context, aborted, pending, stopped, newstackcontent, executor, futures);
          }
        } else if (iterableobj instanceof Map) {
          for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) iterableobj).entrySet()) {
            List newstackcontent = new ArrayList<Object>(2);
            newstackcontent.add(entry.getKey());
            newstackcontent.add(entry.getValue());
            CreateTaskAndStackContext((Macro) top, stack, context, aborted, pending, stopped, newstackcontent, executor, futures);
          }
        } else if (iterableobj instanceof Iterator || iterableobj instanceof Iterable) {
          Iterator<Object> iter = iterableobj instanceof Iterator ? (Iterator<Object>) iterableobj : ((Iterable<Object>) iterableobj).iterator();
          while (iter.hasNext()) {
            Object o = iter.next();
            List newstackcontent = new ArrayList<Object>(1);
            newstackcontent.add(o);
            CreateTaskAndStackContext((Macro) top, stack, context, aborted, pending, stopped, newstackcontent, executor, futures);
          }
        }
      } else {
        int idx = 0;
        for (Object o : (List) top) {
          idx++;
          final Macro macro = (Macro) o;
          List newstackcontent = new ArrayList<Object>(1);
          newstackcontent.add(idx);
          CreateTaskAndStackContext(macro, stack, context, aborted, pending, stopped, newstackcontent, executor, futures);
        }
      }


      //
      // Wait until all tasks have completed or they were aborted or stopped
      //
      while (!aborted.get() && !stopped.get() && pending.get() > 0) {
        LockSupport.parkNanos(100000000L);
      }

      //
      // Abort the executor abruptly if one of the jobs has failed or if a BREAK occured.
      //
      if (aborted.get() || stopped.get()) {
        try {
          executor.shutdownNow();
          executor = null;
        } catch (Throwable t) {
        }
      }

      // Collect task results.
      List<Object> results = new ArrayList<Object>();
      for (Future<List<Object>> future : futures) {
        try {
          if (future.isDone()) {
            results.add(future.get());
          } else {
            results.add(null);
          }
        } catch (Exception e) {
          if (e.getCause() instanceof WarpScriptException) {
            throw (WarpScriptException) e.getCause();
          } else {
            throw new WarpScriptException(e.getCause());
          }
        }
      }
      stack.push(results);

    } finally {
      if (null != executor) {
        executor.shutdownNow();
      }
      stack.setAttribute(CONCURRENT_EXECUTION_ATTRIBUTE, false);
      stack.setAttribute(CONCURRENT_LOCK_ATTRIBUTE, null);
    }

    return stack;
  }


  /**
   * -Create a brand new stack from existing one and copy the context
   * -Copy stackelements on the stack
   * -Create the task to execute the macro
   *
   * @param macro         The macro to execute in the task
   * @param stack         The existing stack (will be cloned as a new one for the task)
   * @param context       The context of the existing stack, will be applied to the new one.
   * @param aborted       Set if the macro raise an exception
   * @param pending       Increased at each task launch, decreased at each task end.
   * @param stopped       Set if the macro raise a WarpScriptLoopBreakException
   * @param stackelements Elements to push on the stack before macro execution
   * @param executor      The executor service
   * @param futures       The list of each output stack
   * @throws WarpScriptException
   */
  private void CreateTaskAndStackContext(final Macro macro,
                                         WarpScriptStack stack,
                                         StackContext context,
                                         final AtomicBoolean aborted,
                                         final AtomicInteger pending,
                                         final AtomicBoolean stopped,
                                         final List stackelements,
                                         ExecutorService executor,
                                         List<Future<List<Object>>> futures) throws WarpScriptException {

    final MemoryWarpScriptStack newstack = ((MemoryWarpScriptStack) stack).getSubStack();

    newstack.push(context);
    newstack.restore();

    Callable task = new Callable<List<Object>>() {
      @Override
      public List<Object> call() throws Exception {
        boolean breakexception = false;
        try {
          if (aborted.get()) {
            throw new WarpScriptException("Early abort.");
          }
          if (!stopped.get()) {
            for (Object o : ((List<Object>) stackelements)) {
              final Object fo = o;
              newstack.push(fo);
            }
            newstack.exec(macro);
          }
          List<Object> results = new ArrayList<Object>();
          while (newstack.depth() > 0) {
            results.add(newstack.pop());
          }
          return results;
        } catch (WarpScriptLoopBreakException e) {
          List<Object> results = new ArrayList<Object>();
          while (newstack.depth() > 0) {
            results.add(newstack.pop());
          }
          breakexception = true;
          return results;  //BREAK set the stopped flag and return its stack after a BREAK.
        } catch (Exception e) {
          aborted.set(true);
          if (e instanceof WarpScriptException) {
            throw e;       //any other exception set the aborted flag and raise an exception.
          } else {
            throw new WarpScriptException(e);
          }
        } finally {
          if (breakexception) {
            stopped.set(true);
          }
          pending.addAndGet(-1);
        }
      }
    };

    pending.addAndGet(1);
    Future<List<Object>> future = executor.submit(task);
    futures.add(future);
  }

}
