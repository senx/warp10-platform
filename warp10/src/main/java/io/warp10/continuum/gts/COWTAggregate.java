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
 * Copy On Write Transversal Aggregate
 *
 * This aggregate class can be used for REDUCE and APPLY, it contains COWTLists
 */
public class COWTAggregate extends Aggregate {

  public COWTAggregate() {}

  public void setDataPoints(List<GeoTimeSerie> gtsList, int[] indices, List<Integer> skippedGTS, long referenceTick) {
    setTicks(new ReadOnlyConstantList(indices.length, referenceTick)); // note: break previous REDUCE convention that had MIN_LONG when no value

    boolean hasLoc = false;
    boolean hasElev = false;
    for (GeoTimeSerie gts: gtsList) {
      if (!hasLoc && gts.hasLocations()) {
        hasLoc = true;
      }
      if (!hasElev && gts.hasElevations()) {
        hasElev = true;
      }
      if (hasLoc && hasElev) {
        break;
      }
    }
    setLocations(hasLoc ? new COWTList(gtsList, indices, skippedGTS, COWTList.TYPE.LOCATIONS) : null);
    setElevations(hasElev ? new COWTList(gtsList, indices, skippedGTS, COWTList.TYPE.ELEVATIONS) : null);
    setValues(new COWTList(gtsList, indices, skippedGTS, COWTList.TYPE.VALUES));
  }

  public void removeNulls() {
    List locations = getLocations();
    if (null != locations && locations instanceof COWTList) {
      ((COWTList) locations).setExposeNullValues(false);
    }

    List elevations = getElevations();
    if (null != elevations && elevations instanceof COWTList) {
      ((COWTList) elevations).setExposeNullValues(false);
    }

    List values = getValues();
    if (null != values && values instanceof COWTList) {
      ((COWTList) values).setExposeNullValues(false);
    }
  }

  public void keepNulls() {
    List locations = getLocations();
    if (null != locations && locations instanceof COWTList) {
      ((COWTList) locations).setExposeNullValues(true);
    }

    List elevations = getElevations();
    if (null != elevations && elevations instanceof COWTList) {
      ((COWTList) elevations).setExposeNullValues(true);
    }

    List values = getValues();
    if (null != values && values instanceof COWTList) {
      ((COWTList) values).setExposeNullValues(true);
    }
  }
}
