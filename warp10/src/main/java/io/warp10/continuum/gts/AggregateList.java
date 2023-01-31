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

import java.util.ArrayList;
import java.util.List;

/**
 * This is the base class for the aggregate that is given as input to BUCKETIZE, MAP, REDUCE and APPLY frameworks.
 * It is treated as a LIST on the warpscript stack.
 */
public class AggregateList extends ArrayList {

  public boolean isEmpty() {
    return size() < 1;
  }
  public boolean hasClassnames() {
    return size() > 1;
  }
  public boolean hasLabels() {
    return size() > 2;
  }
  public boolean hasTicks() {
    return size() > 3;
  }
  public boolean hasLatitudes() {
    return size() > 4;
  }
  public boolean hasLongitudes() {
    return size() > 5;
  }
  public boolean hasElevations() {
    return size() > 6;
  }
  public boolean hasValues() {
    return size() > 7;
  }
  public boolean hasAdditionalParams() {
    return size() > 8;
  }

  public Long getReferenceTick() {
    return isEmpty() ? null : (Long) get(0);
  }
  public List getClassnames() {
    return hasClassnames() ? (List) get(1) : null;
  }
  public List getLabels() {
    return hasLabels() ? (List) get(2) : null;
  }
  public List getTicks() {
    return hasTicks() ? (List) get(3) : null;
  }
  public List getLatitudes() {
    return hasLatitudes() ? (List) get(4) : null;
  }
  public List getLongitudes() {
    return hasLongitudes() ? (List) get(5) : null;
  }
  public List getElevations() {
    return hasElevations() ? (List) get(6) : null;
  }
  public List getValues() {
    return hasValues() ? (List) get(7) : null;
  }
  public List getAdditionalParams() {
    return hasAdditionalParams() ? (List) get(8) : null;
  }
}
