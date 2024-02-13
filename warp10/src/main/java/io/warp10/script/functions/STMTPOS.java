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

  public static final String POS_LINENO = "pos.lineno";
  public static final String POS_START = "pos.start";
  public static final String POS_END = "pos.end";

  public static class PositionedWrappedWarpScriptStackFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction, WrappedStatement, Snapshotable {

    private final long lineno;
    private final long start;
    private final long end;

    public PositionedWrappedWarpScriptStackFunction(String name, long lineno, long start, long end) {
      super(name);
      this.lineno = lineno;
      this.start = start;
      this.end = end;
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      return null;
    }

    @Override
    public Object statement() {
      return null;
    }

    @Override
    public String snapshot() {
      return null;
    }

    public long getLine() {
      return lineno;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }
  }

  public static WrappedStatementFactory NUMBERING_WRAPPED_STATEMENT_FACTORY = new WrappedStatementFactory() {

    @Override
    public Object wrap(Object obj, long lineno, long start, long end) throws WarpScriptException {
      final Object o = obj;

      if (o instanceof NamedWarpScriptFunction && o instanceof WarpScriptStackFunction) {
        return new PositionedWrappedWarpScriptStackFunction(((NamedWarpScriptFunction) o).getName(), lineno, start, end) {
          @Override
          public Object apply(WarpScriptStack stack) throws WarpScriptException {
            Object ret = null;
            try {
              ret = ((WarpScriptStackFunction) o).apply(stack);
            } catch (Throwable t) {
              stack.setAttribute(WarpScriptStack.ATTRIBUTE_LAST_ERRORPOS, lineno + ":" + start + ":" + end);
              throw t;
            }
            stack.setAttribute(WarpScriptStack.ATTRIBUTE_LAST_STMTPOS, lineno + ":" + start + ":" + end);
            return ret;
          }
          @Override
          public Object statement() { return o; }
          @Override
          public String snapshot() { if (o instanceof Snapshotable) { return ((Snapshotable) o).snapshot(); } else { return o.toString(); } }
          @Override
          public String toString() { return o.toString(); }
        };
      } else if (o instanceof WarpScriptStackFunction) {
        return new PositionedWrappedWarpScriptStackFunction("", lineno, start, end) {
          @Override
          public Object apply(WarpScriptStack stack) throws WarpScriptException {
            Object ret = null;
            try {
              ret = ((WarpScriptStackFunction) o).apply(stack);
            } catch (Throwable t) {
              stack.setAttribute(WarpScriptStack.ATTRIBUTE_LAST_ERRORPOS, lineno + ":" + start + ":" + end);
              throw t;
            }
            stack.setAttribute(WarpScriptStack.ATTRIBUTE_LAST_STMTPOS, lineno + ":" + start + ":" + end);
            return ret;
          }
          public Object statement() { return o; }
          @Override
          public String snapshot() { if (o instanceof Snapshotable) { return ((Snapshotable) o).snapshot(); } else { return o.toString(); } }
          @Override
          public String toString() { return o.toString(); }
        };
      } else if (o instanceof Macro) {
        // Leave Macro instances unwrapped but set their position
        ((Macro) o).setAttribute(POS_LINENO, lineno);
        ((Macro) o).setAttribute(POS_START, start);
        ((Macro) o).setAttribute(POS_END, end);
        return o;
      } else {
        return new PositionedWrappedWarpScriptStackFunction("", lineno, start, end) {
          @Override
          public Object apply(WarpScriptStack stack) throws WarpScriptException {
            try {
              stack.push(o);
              stack.setAttribute(WarpScriptStack.ATTRIBUTE_LAST_STMTPOS, lineno + ":" + start + ":" + end);
            } catch (Throwable t) {
              stack.setAttribute(WarpScriptStack.ATTRIBUTE_LAST_ERRORPOS, lineno + ":" + start + ":" + end);
              throw t;
            }
            return stack;
          }
          public Object statement() { return o; }
          @Override
          public String snapshot() { if (o instanceof Snapshotable) { return ((Snapshotable) o).snapshot(); } else { return o.toString(); } }
          @Override
          public String toString() { return o.toString(); }
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
