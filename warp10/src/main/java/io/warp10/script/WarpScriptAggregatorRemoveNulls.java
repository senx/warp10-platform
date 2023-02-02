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

package io.warp10.script;

import io.warp10.continuum.gts.AggregateList;
import io.warp10.continuum.gts.MultivariateAggregateCOWList;
import io.warp10.continuum.gts.UnivariateAggregateCOWList;

import java.util.ArrayList;
import java.util.List;

public interface WarpScriptAggregatorRemoveNulls extends WarpScriptAggregatorHandleNulls {
  static public AggregateList removeNulls(AggregateList aggregateList) throws WarpScriptException {
    if (!aggregateList.hasValues()) {
      return aggregateList;
    }

    // First case : COWList are backed by arrays of primitive objects that cannot contain any null value
    if (aggregateList instanceof UnivariateAggregateCOWList) {
      return aggregateList;
    }

    // Second case : this is the usual case from REDUCE or APPLY framework
    if (aggregateList instanceof MultivariateAggregateCOWList) {

      ((MultivariateAggregateCOWList) aggregateList).removeNulls();
      return aggregateList;
    }

    // Third case : this covers all remaining cases for full compatibility with the interface but should not happen for optimised aggregators
    List values = aggregateList.getValues();
    List<Integer> skippedIndices= new ArrayList<Integer>(values.size() / 2);

    for (int i = 0; i < values.size(); i++) {
      if (null == values.get(i)) {
        skippedIndices.add(i);
      }
    }

    if (0 == skippedIndices.size()) {
      return aggregateList;
    }

    if (1 == values.size()) {
      for (int i = 1; i < 8; i++) {
        aggregateList.set(i, new ArrayList<Object>(0));
      }
      return aggregateList;
    }

    for (int i = 1; i < 8; i++) {

      List field = (List) aggregateList.get(i);
      if (field.size() > 1) {

        List newField = new ArrayList(field.size() - skippedIndices.size());

        for (int j = 0; j < field.size(); j++) {
          if (!skippedIndices.contains(j)) {
            newField.add(field.get(j));
          }
        }
      }
    }

    return aggregateList;
  }
}
