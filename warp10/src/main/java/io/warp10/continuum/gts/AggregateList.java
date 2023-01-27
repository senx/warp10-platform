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

import io.warp10.script.WarpScriptException;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the base class for the aggregate that is given as input to BUCKETIZE, MAP, REDUCE and APPLY frameworks.
 * It is treated as a LIST on the warpscript stack.
 */
public class AggregateList extends ArrayList {

  public Long getReferenceTick() throws WarpScriptException {
    if (size() < 1) {
      throw new WarpScriptException("Aggregate list does not contain reference tick.");
    }

    return (Long) get(0);
  }

  public List getClassnames() throws WarpScriptException {
    if (size() < 2) {
      throw new WarpScriptException("Aggregate list does not contain classnames.");
    }

    return (List) get(1);
  }

  public List getLabels() throws WarpScriptException {
    if (size() < 3) {
      throw new WarpScriptException("Aggregate list does not contain labels.");
    }

    return (List) get(2);
  }

  public List getTicks() throws WarpScriptException {
    if (size() < 4) {
      throw new WarpScriptException("Aggregate list does not contain ticks.");
    }

    return (List) get(3);
  }

  public List getLatitudes() throws WarpScriptException {
    if (size() < 5) {
      throw new WarpScriptException("Aggregate list does not contain latitudes.");
    }

    return (List) get(4);
  }

  public List getLongitudes() throws WarpScriptException {
    if (size() < 6) {
      throw new WarpScriptException("Aggregate list does not contain longitudes.");
    }

    return (List) get(5);
  }

  public List getElevations() throws WarpScriptException {
    if (size() < 7) {
      throw new WarpScriptException("Aggregate list does not contain elevations.");
    }

    return (List) get(6);
  }

  public List getValues() throws WarpScriptException {
    if (size() < 8) {
      throw new WarpScriptException("Aggregate list does not contain values.");
    }

    return (List) get(7);
  }

  public List getAdditionalParams() throws WarpScriptException {
    if (size() < 9) {
      throw new WarpScriptException("Aggregate list does not contain additional parameters.");
    }

    return (List) get(8);
  }
}
