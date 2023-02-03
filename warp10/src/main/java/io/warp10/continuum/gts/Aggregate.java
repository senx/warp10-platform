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

import com.geoxp.GeoXPLib;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the base class for the aggregate that is given as input to BUCKETIZE, MAP, REDUCE and APPLY frameworks.
 */
public class Aggregate {
  protected long referenceTick;
  final protected List[] lists = new List[7];

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
  public List<Object> toList() {
    List<Object> params = new ArrayList<>(8);

    params.add(getReferenceTick());
    params.add(getClassnames());
    params.add(getLabels());
    params.add(getTicks());

    // locations need to be converted
    if (null != getLocations()) {
      List locations = getLocations();

      ArrayList<Double> lats = new ArrayList<Double>(locations.size());
      ArrayList<Double> lons = new ArrayList<Double>(locations.size());

      for (int i = 0; i < locations.size(); i++) {
        Long location = (Long) locations.get(i);
        if (GeoTimeSerie.NO_LOCATION == location) {
          lats.add(Double.NaN);
          lons.add(Double.NaN);
        } else {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          lats.add(latlon[0]);
          lons.add(latlon[1]);
        }
      }
      params.add(lats);
      params.add(lons);

    } else {
      // in this case, it is a readOnlyConstantList with value Double.NaN
      Object o = new ReadOnlyConstantList(getTicks().size(), Double.NaN);
      params.add(o);
      params.add(o);
    }

    if (null != getElevations()) {
      List elevations = getElevations();

      ArrayList<Object> elevs = new ArrayList<Object>(elevations.size());
      for (int i = 0; i < elevations.size(); i++) {
        if (GeoTimeSerie.NO_ELEVATION == (Long) elevations.get(i)) {
          elevs.add(Double.NaN);
        } else {
          elevs.add(elevations.get(i));
        }
      }
      params.add(elevs);

    } else {
      // in this case, it is a readOnlyConstantList with value Double.NaN
      params.add(new ReadOnlyConstantList(getTicks().size(), Double.NaN));
    }

    params.add(getValues());

    return params;
  }
}
