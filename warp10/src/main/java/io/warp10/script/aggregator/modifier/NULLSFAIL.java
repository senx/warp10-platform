//
//   Copyright 2023  SenX S.A.S.
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

package io.warp10.script.aggregator.modifier;

import io.warp10.continuum.gts.Aggregate;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFailIfAnyNull;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptReducer;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.SNAPSHOT;

public class NULLSFAIL extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public NULLSFAIL(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof WarpScriptReducer)) {
      throw new WarpScriptException(getName() + " expects a reducer");
    }

    if (!(o instanceof WarpScriptAggregatorFailIfAnyNull)) {
      throw new WarpScriptException(getName() + " cannot be be applied to this AGGREGATOR");
    }

    stack.push(new FailIfAnyNullDecorator(getName(), (WarpScriptAggregatorFailIfAnyNull) o));

    return stack;
  }

  private static final class FailIfAnyNullDecorator extends NamedWarpScriptFunction implements WarpScriptReducer, WarpScriptAggregatorFailIfAnyNull, SNAPSHOT.Snapshotable {

    private final WarpScriptAggregatorFailIfAnyNull aggregator;

    public FailIfAnyNullDecorator(String name, WarpScriptAggregatorFailIfAnyNull aggregator) {
      super(name);
      this.aggregator = aggregator;
    }

    @Override
    public Object apply(Aggregate aggregate) throws WarpScriptException {
      WarpScriptAggregatorFailIfAnyNull.failIfAnyNull(aggregate);
      return aggregator.apply(aggregate);
    }

    @Override
    public String snapshot() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(aggregator.toString());
      sb.append(" ");
      sb.append(getName());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      return sb.toString();
    }
  }
}
