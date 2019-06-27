package io.warp10.continuum.gts;

import com.geoxp.GeoXPLib;
import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Pickler for Geo Time Series
 */
public class GTSPickler implements IObjectPickler {

  public void pickle(Object o, OutputStream out, Pickler currentPickler) throws PickleException, IOException {
    GeoTimeSerie gts = (GeoTimeSerie) o;

    HashMap<String, Object> gtsAsMap = new HashMap<>();
    gtsAsMap.put("c", gts.getName());
    gtsAsMap.put("l", gts.getMetadata().getAttributes());

    List<Long> ticks = new ArrayList<Long>(gts.values);
    for (int i = 0; i < gts.values; i++) {
      ticks.add(gts.ticks[i]);
    }
    gtsAsMap.put("t", ticks);

    if (0 == gts.values) {
      gtsAsMap.put("v", new long[0]);
    } else {
      List<Object> values = new ArrayList<Object>(gts.values);

      for (int i = 0; i < gts.values; i++) {
        values.add(GTSHelper.valueAtIndex(gts, i));
      }
      gtsAsMap.put("v", values);
    }

    if (gts.hasLocations()) {
      long[] locations = gts.locations;
      List<Double> lats = new ArrayList<Double>();
      List<Double> lons = new ArrayList<Double>();

      for (int i = 0; i < gts.values; i++) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(locations[i]);
        lats.add(latlon[0]);
        lons.add(latlon[1]);
      }

      gtsAsMap.put("lat", lats);
      gtsAsMap.put("lon", lons);
    }

    if (gts.hasElevations()) {
      List<Long> elevs = new ArrayList<Long>(gts.values);
      for (int i = 0; i < gts.values; i++) {
        elevs.add(gts.elevations[i]);
      }
      gtsAsMap.put("elev", elevs);
    }

    currentPickler.save(gtsAsMap);
  }
}
