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
 * This is the base class for the aggregate that is given as input to BUCKETIZE, MAP, REDUCE and APPLY frameworks.
 */
public abstract class Aggregate {
  protected long referenceTick;
  final protected List[] lists = new List[7];

  public boolean isEmpty() {
    return 0 == lists.length;
  }

  public Long getReferenceTick() {
    return referenceTick;
  }

  public List[] getLists() {
    return lists;
  }

  public List getClassnames() {
    return lists[0];
  }
  public List getLabels() {
    return lists[1];
  }
  public List getTicks() {
    return lists[2];
  }
  public List getLocations() {
    return lists[3];
  }
  public List getElevations() {
    return lists[4];
  }
  public List getValues() {
    return lists[5];
  }
  public List getAdditionalParams() {
    return lists[6];
  }

  public void setReferenceTick(long referenceTick) {
    this.referenceTick = referenceTick;
  }
  public void setClassnames(List classnames) {
    lists[0] = classnames;
  }
  public void setLabels(List labels) {
    lists[1] = labels;
  }
  public void setTicks(List ticks) {
    lists[2] = ticks;
  }
  public void setLocations(List locations) {
    lists[3] = locations;
  }
  public void setElevations(List elevations) {
    lists[4] = elevations;
  }
  public void setValues(List values) {
    lists[5] = values;
  }
  public void setAdditionalParams(List additionalParams) {
    lists[6] = additionalParams;
  }

  /**
   * Convert into a List (to be used by MACROMAPPER, MACROREDUCER)
   * @return a list that can be pushed on a WarpScript stack
   */
  abstract public List<Object> toList();

  public List<Object> toListWithAdditionalParams() {
    List res = toList();

    res.add(getAdditionalParams());
    return res;
  }
}
