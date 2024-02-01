//
//   Copyright 2024  SenX S.A.S.
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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WrappedStatement;
import io.warp10.script.WrappedStatementFactory;
import io.warp10.script.functions.SNAPSHOT.Snapshotable;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

/**
 * Turn on the tracking of statement positions
 */
public class STMTPOS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static class WrappedWarpScriptStackFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction, WrappedStatement {

    public WrappedWarpScriptStackFunction(String name) {
      super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      return null;
    }

    @Override
    public Object statement() {
      return null;
    }
  }

  public static WrappedStatementFactory NUMBERING_WRAPPED_STATEMENT_FACTORY = new WrappedStatementFactory() {

    @Override
    public Object wrap(Object obj, long lineno, int start, int end) throws WarpScriptException {
      final Object o = obj;
      final long flineno = lineno;
      final int fstart = start;
      final int fend = end;

      if (o instanceof NamedWarpScriptFunction) {
        return new WrappedWarpScriptStackFunction(((NamedWarpScriptFunction) o).getName()) {
          @Override
          public Object apply(WarpScriptStack stack) throws WarpScriptException {
            stack.setAttribute(WarpScriptStack.ATTRIBUTE_LAST_STMTPOS, flineno + ":" + fstart + ":" + fend);
            return ((WarpScriptStackFunction) o).apply(stack);
          }
          @Override
          public Object statement() { return o; }
          @Override
          public String toString() { if (o instanceof Snapshotable) { return ((Snapshotable) o).snapshot(); } else { return o.toString(); } }
        };
      } else if (o instanceof WarpScriptStackFunction) {
        return new WrappedWarpScriptStackFunction("") {
          @Override
          public Object apply(WarpScriptStack stack) throws WarpScriptException {
            stack.setAttribute(WarpScriptStack.ATTRIBUTE_LAST_STMTPOS, flineno + ":" + fstart + ":" + fend);
            return ((WarpScriptStackFunction) o).apply(stack);
          }
          public Object statement() { return o; }
          @Override
          public String toString() { if (o instanceof Snapshotable) { return ((Snapshotable) o).snapshot(); } else { return o.toString(); } }
        };
      } else if (o instanceof Macro) {
        // Leave Macro instances unwrapped
        return o;
      } else {
        return new WrappedWarpScriptStackFunction("") {
          @Override
          public Object apply(WarpScriptStack stack) throws WarpScriptException {
            stack.setAttribute(WarpScriptStack.ATTRIBUTE_LAST_STMTPOS, flineno + ":" + fstart + ":" + fend);
            stack.push(o);
            return stack;
          }
          public Object statement() { return o; }
          @Override
          public String toString() { if (o instanceof Snapshotable) { return ((Snapshotable) o).snapshot(); } else { return o.toString(); } }
        };
      }
    }
  };

  public STMTPOS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (Boolean.TRUE.equals(top)) {
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_WRAPPED_STATEMENT_FACTORY, NUMBERING_WRAPPED_STATEMENT_FACTORY);
    } else if (Boolean.FALSE.equals(top)) {
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_WRAPPED_STATEMENT_FACTORY, null);
    } else {
      throw new WarpScriptException(getName() + " expects a BOOLEAN.");
    }

    return stack;
  }

}
