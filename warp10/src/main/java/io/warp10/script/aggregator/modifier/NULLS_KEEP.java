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
import io.warp10.script.WarpScriptAggregatorKeepNulls;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptReducer;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.SNAPSHOT;

public class NULLS_KEEP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public NULLS_KEEP(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof WarpScriptReducer)) {
      throw new WarpScriptException(getName() + " expects a reducer");
    }

    if (!(o instanceof WarpScriptAggregatorKeepNulls)) {
      throw new WarpScriptException(getName() + " can not be be applied to this AGGREGATOR");
    }

    WarpScriptAggregatorKeepNulls aggregator = (WarpScriptAggregatorKeepNulls) o;

    if (aggregator.actionOnNullsIsSet()) {
      throw new WarpScriptException(getName() + " can not be applied on an AGGREGATOR that already has an action on null values");
    }

    stack.push(new ModifiedAggregator(getName(), aggregator));

    return stack;
  }

  private static final class ModifiedAggregator extends NamedWarpScriptFunction implements WarpScriptReducer, WarpScriptAggregatorKeepNulls, SNAPSHOT.Snapshotable {

    private final WarpScriptAggregatorKeepNulls aggregator;

    public ModifiedAggregator(String name, WarpScriptAggregatorKeepNulls aggregator) {
      super(name);
      this.aggregator = aggregator;
    }

    @Override
    public Object apply(Aggregate aggregate) throws WarpScriptException {
      return aggregator.apply(WarpScriptAggregatorKeepNulls.keepNulls(aggregate));
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