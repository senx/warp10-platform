//
//   Copyright 2019  SenX S.A.S.
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
package io.warp10.script.functions;

import java.util.ArrayList;
import java.util.List;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.WarpScriptException;

/**
 * Split a GTS according to motion
 */
public class MOTIONSPLIT {

  /**
   * Split a Geo Time Series according to motion.
   * 
   * The criteria for splitting are:
   * 
   * - time between datapoints, above a certain threshold a split will happen
   * - distance between datapoints, above a certain threshold a split will happen
   * - time not moving, if datapoints stay within a certain area and no distance is traveled for a certain time, a split will happen
   * 
   * @param gts GTS to split
   * @param label Name of label which will contain the start/end of the split
   * @param timeThreshold If the delay between two ticks goes beyond this value, force a split
   * @param distanceThreshold If we traveled more than distanceThreshold between two ticks, force a split
   * @param proximityZoneRadius Radius of the proximity zone in meters
   * @param proximityZoneTime If we spent more than that much time in the proximity zone without moving and traveled slower than proximityZoneSpeed while in the proximityZone force a split
   * @param proximityZoneSpeed Minimum speed in the proxmityZone to prevent a split, in m/s
   * 
   * @return
   * @throws WarpScriptException
   */
  public static List<GeoTimeSerie> motionSplit(GeoTimeSerie gts, String label, long timeThreshold, long distanceThreshold, double proximityZoneRadius, double proximityZoneSpeed, long proximityZoneTime) throws WarpScriptException {
    //
    // Sort GTS according to timestamps
    //
    
    GTSHelper.sort(gts, false);

    GeoTimeSerie split = null;
    
    List<GeoTimeSerie> splits = new ArrayList<GeoTimeSerie>();
    
    int splitStart = 0;
    int idx = 0;
    
    int n = GTSHelper.nvalues(gts);
    
    boolean mustSplit = true;

    long refLocation = GeoTimeSerie.NO_LOCATION;
    long refTimestamp = Long.MIN_VALUE;
    
    long previousTimestamp = Long.MIN_VALUE;
    long previousLocation = GeoTimeSerie.NO_LOCATION;
    long proximityZoneTraveledDistance = 0;
    
    while (idx < n) {
      long timestamp = GTSHelper.tickAtIndex(gts, idx);
      long location = GTSHelper.locationAtIndex(gts, idx);
      long elevation = GTSHelper.elevationAtIndex(gts, idx);
      Object value = GTSHelper.valueAtIndex(gts, idx);
    
      //
      // If the previous tick was more than 'timeThreashold' ago, split now
      //
      
      if (timestamp - previousTimestamp > timeThreshold) {
        mustSplit = true;
      }
      
      //
      // If the distance to the previous location is above distanceThreshold, split
      //
      
      if (GeoTimeSerie.NO_LOCATION != previousLocation && GeoTimeSerie.NO_LOCATION != location && GeoXPLib.orthodromicDistance(location, previousLocation) > distanceThreshold) {
        mustSplit = true;
      }
      
      //
      // If the current point is farther away from the reference point than 'proximityDistance'
      // change the reference point and reset the traveled distance.
      // If the distance traveled in the proximity zone is less than the proximityTrip and the time spent in the zone is above maxProximityTime force a split
      //
      
      if (GeoTimeSerie.NO_LOCATION != refLocation && GeoTimeSerie.NO_LOCATION != location && GeoXPLib.orthodromicDistance(refLocation, location) > proximityZoneRadius) {
        if (timestamp - refTimestamp > proximityZoneTime && proximityZoneTraveledDistance / ((timestamp - refTimestamp) / Constants.TIME_UNITS_PER_S) < proximityZoneSpeed) {
          mustSplit = true;
        }
        refLocation = location;
        refTimestamp = timestamp;
        proximityZoneTraveledDistance = 0;
      }
      
      if (GeoTimeSerie.NO_LOCATION != refLocation && GeoTimeSerie.NO_LOCATION != location && GeoXPLib.orthodromicDistance(refLocation, location) <= proximityZoneRadius) {
        if (GeoTimeSerie.NO_LOCATION != previousLocation) {
          proximityZoneTraveledDistance += GeoXPLib.orthodromicDistance(previousLocation, location);
        }
      }
      
      if (mustSplit) {
        split = gts.cloneEmpty();
        
        gts.getLabels().put(label, Long.toString(timestamp));
        
        splits.add(split);
        mustSplit = false;
        
        if (GeoTimeSerie.NO_LOCATION != location) {
          refLocation = location;
          refTimestamp = timestamp;
          proximityZoneTraveledDistance = 0;
        } else {
          refLocation = GeoTimeSerie.NO_LOCATION;
          refTimestamp = timestamp;
          proximityZoneTraveledDistance = 0;
        }        
      }
      
      GTSHelper.setValue(split, timestamp, location, elevation, value, false);

      previousTimestamp = timestamp;
      previousLocation = location;
      
      idx++;
    }
            
    return splits;
  }
}
