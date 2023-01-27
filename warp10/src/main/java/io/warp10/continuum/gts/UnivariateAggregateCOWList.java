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
 * This aggregate class can be used for BUCKETIZE and MAP
 */
public class UnivariateAggregateCOWList extends AggregateList {

  public UnivariateAggregateCOWList(GeoTimeSerie gts, int startIdx, int length, Long reference, List additionalParams) {

    // tick of computation
    this.add(reference);

    // classnames
    List classnames = new ArrayList(1);
    classnames.add(gts.getName());
    this.add(classnames);

    // labels
    List labels = new ArrayList(1);
    labels.add(gts.getLabels());
    this.add(labels);

    // ticks
    this.add(new COWList(gts.ticks, startIdx, length));

    // locations need to be converted
    if (gts.hasLocations()) {

      ArrayList<Double> lats = new ArrayList<Double>(gts.locations.length);
      ArrayList<Double> lons = new ArrayList<Double>(gts.locations.length);

      for (int i = 0; i < gts.locations.length; i++) {
        Long location = gts.locations[i];
        if (GeoTimeSerie.NO_LOCATION == location) {
          lats.add(Double.NaN);
          lons.add(Double.NaN);
        } else {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          lats.add(latlon[0]);
          lons.add(latlon[1]);
        }
      }
      this.add(lats);
      this.add(lons);

    } else {
      // in this case, it is a readOnlyConstantList with value Double.NaN
      Object o = new ReadOnlyConstantList(gts.size(), Double.NaN);
      this.add(o);
      this.add(o);
    }

    // elevations
    if (null != gts.elevations) {
      ArrayList<Object> elevs = new ArrayList<Object>(gts.elevations.length);
      for (int i = 0; i < gts.elevations.length; i++) {
        if (GeoTimeSerie.NO_ELEVATION == gts.elevations[i]) {
          elevs.add(Double.NaN);
        } else {
          elevs.add(gts.elevations[i]);
        }
      }
      this.add(elevs);

    } else {
      // in this case, it is a readOnlyConstantList with value Double.NaN
      this.add(new ReadOnlyConstantList(gts.size(), Double.NaN));
    }

    // values
    switch (gts.type) {
      case LONG:
        this.add(new COWList(gts.longValues, startIdx, length));
        break;
      case DOUBLE:
        this.add(new COWList(gts.doubleValues, startIdx, length));
        break;
      case STRING:
        this.add(new COWList(gts.booleanValues, startIdx, length));
        break;
      case BOOLEAN:
        this.add(new COWList(gts.stringValues, startIdx, length));
        break;
      default:
        throw new RuntimeException("Undefined GeoTimeSeries Type.");
    }

    // additional parameters
    this.add(additionalParams);
  }
}
