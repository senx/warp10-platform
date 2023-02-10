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
import io.warp10.continuum.gts.COWTAggregate;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorKeepNulls;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptReducer;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.SNAPSHOT;

public class NULLSKEEP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public NULLSKEEP(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof WarpScriptReducer)) {
      throw new WarpScriptException(getName() + " expects a reducer");
    }

    stack.push(new KeepNullsDecorator(getName(), (WarpScriptReducer) o));

    return stack;
  }

  private static final class KeepNullsDecorator extends NamedWarpScriptFunction implements WarpScriptReducer, SNAPSHOT.Snapshotable {

    private final WarpScriptReducer aggregator;

    public KeepNullsDecorator(String name, WarpScriptReducer aggregator) {
      super(name);
      this.aggregator = aggregator;
    }

    @Override
    public Object apply(Aggregate aggregate) throws WarpScriptException {
      Aggregate agg;

      if (aggregator instanceof WarpScriptAggregatorKeepNulls) {
        agg = ((WarpScriptAggregatorKeepNulls) (aggregator)).keepNulls(aggregate);
      } else {
        agg = keepNulls(aggregate);
      }

      return aggregator.apply(agg);
    }

    private Aggregate keepNulls(Aggregate aggregate) throws WarpScriptException {
      if (null == aggregate.getValues()) {
        return aggregate;
      }

      if (aggregate instanceof COWTAggregate) {
        ((COWTAggregate) aggregate).keepNulls();
        return aggregate;
      } else {
        throw new WarpScriptException(getName() + " cannot expose null values from this aggregate.");
      }
    }

    @Override
    public String snapshot() {
      StringBuilder sb = new StringBuilder();
      try {
        SNAPSHOT.addElement(sb, aggregator);
      }
      catch (WarpScriptException wse) {
        throw new RuntimeException(wse);
      }
      sb.append(" ");
      sb.append(getName());
      return sb.toString();
    }
  }
}
