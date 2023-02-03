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
public class UnivariateAggregateCOWList extends Aggregate {

  public UnivariateAggregateCOWList() {}

  public UnivariateAggregateCOWList(GeoTimeSerie gts, int startIdx, int length, Long reference) {
    this(gts, startIdx, length, reference, null);
  }

  public UnivariateAggregateCOWList(GeoTimeSerie gts, int startIdx, int length, Long reference, List additionalParams) {
    // tick of computation
    setReferenceTick(reference);

    // metadata
    setMetaData(gts);

    // data points
    setDataPoints(gts, startIdx, length);

    // additional parameters
    setAdditionalParams(additionalParams);
  }

  public void setMetaData(GeoTimeSerie gts) {
    // classnames
    List classnames = new ArrayList(1);
    classnames.add(gts.getName());
    setClassnames(classnames);

    // labels
    List labels = new ArrayList(1);
    labels.add(gts.getLabels());
    setLabels(labels);
  }

  public void setDataPoints(GeoTimeSerie gts, int startIdx, int length) {
    // ticks
    setTicks(new COWList(gts.ticks, startIdx, length));

    // locations
    if (gts.hasLocations()) {
      setLocations(new COWList(gts.locations, startIdx, length));
    } else {
      setLocations(null);
    }

    // elevations
    if (gts.hasElevations()) {
      setElevations(new COWList(gts.elevations, startIdx, length));
    } else {
      setElevations(null);
    }

    // values
    switch (gts.type) {
      case LONG:
        setValues(new COWList(gts.longValues, startIdx, length));
        break;
      case DOUBLE:
        setValues(new COWList(gts.doubleValues, startIdx, length));
        break;
      case STRING:
        setValues(new COWList(gts.stringValues, startIdx, length));
        break;
      case BOOLEAN:
        setValues(new COWList(gts.booleanValues, startIdx, length));
        break;
      default:
        throw new RuntimeException("Undefined GeoTimeSeries Type.");
    }
  }

  @Override
  public List<Object> toList() {
    List<Object> params = new ArrayList<>(8);

    params.add(getReferenceTick());
    params.add(getClassnames());
    params.add(getLabels());
    params.add(getTicks());

    // locations need to be converted
    if (null != getLocations()) {
      COWList locations = (COWList) getLocations();

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
      COWList elevations = (COWList) getElevations();

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
