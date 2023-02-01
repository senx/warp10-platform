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

package io.warp10.continuum.gts;

import java.util.List;

/**
 *
 * Draft: this class is a WIP
 *
 * This aggregate class can be used for REDUCE and APPLY
 */
public class MultivariateAggregateCOWList extends AggregateList {

  /**
   * For each input gts, a datapoint is aggregated if the tick at the corresponding index is equal to the reference tick.
   * In this case this index is incremented.
   */
  public MultivariateAggregateCOWList(List<GeoTimeSerie> gtsList, int[] idx, long reference) {
    super(8);

    if (gtsList.size() != idx.length) {
      throw new RuntimeException("Size mismatch");
    }

    //todo

  }

  public void skipNulls() {
    // todo
  }

}
