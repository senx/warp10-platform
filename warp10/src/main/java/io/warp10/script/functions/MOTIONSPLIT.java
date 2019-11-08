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
import java.util.Map;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Split a GTS according to motion
 */
public class MOTIONSPLIT extends ElementOrListStackFunction {

  public static final String PARAM_TIMESPLIT = "timesplit";
  public static final String PARAM_START_LABEL = "label.split.start";
  public static final String PARAM_PROXIMITY_IN_ZONE_TIME_LABEL = "label.stopped.time";
  public static final String PARAM_SPLIT_TYPE_LABEL = "label.split.type";
  public static final String PARAM_DISTANCETHRESHOLD = "distance.split";
  public static final String PARAM_PROXIMITY_ZONE_TIME = "stopped.min.time";
  public static final String PARAM_PROXIMITY_ZONE_SPEED = "stopped.max.speed";
  public static final String PARAM_PROXIMITY_ZONE_RADIUS = "stopped.max.radius";
  public static final String PARAM_PROXIMITY_IN_ZONE_MAX_SPEED = "stopped.max.mean.speed";

  public static final String SPLIT_TYPE_TIME = "timesplit";
  public static final String SPLIT_TYPE_DISTANCE = "distancesplit";
  public static final String SPLIT_TYPE_END = "end";
  public static final String SPLIT_TYPE_STOPPED = "stopped";


  public MOTIONSPLIT(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {

    Map<String, Object> params = null;
    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a parameter MAP on top of the stack.");
    }

    params = (Map) top;

    long timeThreshold = Long.MAX_VALUE;
    if (params.containsKey(PARAM_TIMESPLIT)) {
      Object o = params.get((PARAM_TIMESPLIT));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_TIMESPLIT + " must be a number.");
      }
      timeThreshold = ((Number) o).longValue();
    }

    String startLabel = null;
    if (params.containsKey(PARAM_START_LABEL)) {
      Object o = params.get((PARAM_START_LABEL));
      if (!(o instanceof String)) {
        throw new WarpScriptException(getName() + " " + PARAM_START_LABEL + " must be a string.");
      }
      startLabel = o.toString();
    }

    String splitTypeLabel = null;
    if (params.containsKey(PARAM_SPLIT_TYPE_LABEL)) {
      Object o = params.get((PARAM_SPLIT_TYPE_LABEL));
      if (!(o instanceof String)) {
        throw new WarpScriptException(getName() + " " + PARAM_SPLIT_TYPE_LABEL + " must be a string.");
      }
      splitTypeLabel = o.toString();
    }

    String stoppedTimeLabel = null;
    if (params.containsKey(PARAM_PROXIMITY_IN_ZONE_TIME_LABEL)) {
      Object o = params.get((PARAM_PROXIMITY_IN_ZONE_TIME_LABEL));
      if (!(o instanceof String)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_IN_ZONE_TIME_LABEL + " must be a string.");
      }
      stoppedTimeLabel = o.toString();
    }

    double distanceThreshold = Double.MAX_VALUE;
    if (params.containsKey(PARAM_DISTANCETHRESHOLD)) {
      Object o = params.get((PARAM_DISTANCETHRESHOLD));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_DISTANCETHRESHOLD + " must be a number.");
      }
      distanceThreshold = ((Number) o).doubleValue();
    }

    double proximityZoneMaxSpeed = Double.MAX_VALUE;
    if (params.containsKey(PARAM_PROXIMITY_ZONE_SPEED)) {
      Object o = params.get((PARAM_PROXIMITY_ZONE_SPEED));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_ZONE_SPEED + " must be a number: speed in m/s.");
      }
      proximityZoneMaxSpeed = ((Number) o).doubleValue();
    }

    double proximityInZoneMaxMeanSpeed = Double.MAX_VALUE;
    if (params.containsKey(PARAM_PROXIMITY_IN_ZONE_MAX_SPEED)) {
      Object o = params.get((PARAM_PROXIMITY_IN_ZONE_MAX_SPEED));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_IN_ZONE_MAX_SPEED + " must be a number: speed in m/s.");
      }
      proximityInZoneMaxMeanSpeed = ((Number) o).doubleValue();
    }

    long proximityZoneTime = Long.MAX_VALUE;
    if (params.containsKey(PARAM_PROXIMITY_ZONE_TIME)) {
      Object o = params.get((PARAM_PROXIMITY_ZONE_TIME));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_ZONE_TIME + " must be a number: time in platform time unit");
      }
      proximityZoneTime = ((Number) o).longValue();
    }

    double proximityZoneRadius = Double.MAX_VALUE;
    if (params.containsKey(PARAM_PROXIMITY_ZONE_RADIUS)) {
      Object o = params.get((PARAM_PROXIMITY_ZONE_RADIUS));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_ZONE_RADIUS + " must be a number: radius in meters.");
      }
      proximityZoneRadius = ((Number) o).doubleValue();
    }

    final String fstartLabel = startLabel;
    final long ftimeThreshold = timeThreshold;
    final double fdistanceThreshold = distanceThreshold;
    final double fproximityZoneRadius = proximityZoneRadius;
    final double fproximityZoneMaxSpeed = proximityZoneMaxSpeed;
    final long fproximityZoneTime = proximityZoneTime;
    final double fproximityInZoneMaxMeanSpeed = proximityInZoneMaxMeanSpeed;
    final String fsplitTypeLabel = splitTypeLabel;
    final String fstoppedTimeLabel = stoppedTimeLabel;

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (!(element instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " can only be applied on Geo Time Series.");
        }

        GeoTimeSerie gts = (GeoTimeSerie) element;

        return motionSplit(gts, fstartLabel, ftimeThreshold, fdistanceThreshold, fproximityZoneRadius, fproximityZoneMaxSpeed, fproximityZoneTime, fproximityInZoneMaxMeanSpeed, fsplitTypeLabel, fstoppedTimeLabel);
      }
    };
  }

  /**
   * Split a Geo Time Series according to motion.
   * <p>
   * The criteria for splitting are:
   * <p>
   * - time between datapoints, above a certain threshold a split will happen
   * - distance between datapoints, above a certain threshold a split will happen
   * - time not moving, if datapoints stay within a certain area and no distance is traveled for a certain time, a split will happen
   *
   * @param gts                         GTS to split
   * @param startLabel                  If set, add a label which will contain the start of the split
   * @param timeThreshold               If the delay between two ticks goes beyond this value, force a split
   * @param distanceThreshold           If we traveled more than distanceThreshold between two ticks, force a split
   * @param proximityZoneRadius         Radius of the proximity zone in meters.
   * @param proximityZoneTime           If we spent more than that much time in the proximity zone without moving and traveled slower than proximityInZoneMaxMeanSpeed while in the proximityZone force a split
   * @param proximityZoneMaxSpeed       Maximum instant speed to consider you are in the same proximity zone
   * @param proximityInZoneMaxMeanSpeed Minimum mean speed in the proximityZone to prevent a split, in m/s
   * @param splitTypeLabel              If set, add a label with the reason of the split
   * @param stoppedTimeLabel            If set, add a label with the time in the stop state
   * @return
   * @throws WarpScriptException
   */
  private static List<GeoTimeSerie> motionSplit(GeoTimeSerie gts, String startLabel, long timeThreshold, double distanceThreshold, double proximityZoneRadius, double proximityZoneMaxSpeed,
                                                long proximityZoneTime, double proximityInZoneMaxMeanSpeed, String splitTypeLabel, String stoppedTimeLabel) throws WarpScriptException {
    //
    // Sort GTS according to timestamps
    //

    GTSHelper.sort(gts, false);

    GeoTimeSerie split = null;

    List<GeoTimeSerie> splits = new ArrayList<GeoTimeSerie>();

    int idx = 0;

    int n = GTSHelper.nvalues(gts);

    boolean mustSplit = true;

    long refLocation = GeoTimeSerie.NO_LOCATION;
    long refTimestamp = Long.MIN_VALUE;

    long previousTimestamp = Long.MIN_VALUE;
    long previousLocation = GeoTimeSerie.NO_LOCATION;
    double proximityZoneTraveledDistance = 0.0D;
    String splitReason = SPLIT_TYPE_END;

    while (idx < n) {
      long timestamp = GTSHelper.tickAtIndex(gts, idx);
      long location = GTSHelper.locationAtIndex(gts, idx);
      long elevation = GTSHelper.elevationAtIndex(gts, idx);
      Object value = GTSHelper.valueAtIndex(gts, idx);

      //
      // Initialise refLocation as soon as there is a valid position
      //
      if (GeoTimeSerie.NO_LOCATION == refLocation && GeoTimeSerie.NO_LOCATION != location) {
        refLocation = location;
        refTimestamp = timestamp;
      }

      //
      // If the previous tick was more than 'timeThreshold' ago, split now
      //
      if (timestamp - previousTimestamp > timeThreshold) {
        splitReason = SPLIT_TYPE_TIME;
        mustSplit = true;
      }

      //
      // If the distance to the previous location is above distanceThreshold, split
      //
      if (GeoTimeSerie.NO_LOCATION != previousLocation && GeoTimeSerie.NO_LOCATION != location && GeoXPLib.orthodromicDistance(location, previousLocation) > distanceThreshold) {
        splitReason = SPLIT_TYPE_DISTANCE;
        mustSplit = true;
      }

      //
      // If the current point is farther away from the reference point than 'proximityZoneRadius', or the instant speed is greater than proximityZoneSpeed
      // change the reference point and reset the traveled distance.
      // If the distance traveled in the proximity zone was traveled at less than proximityInZoneMaxMeanSpeed and the time spent in the zone is above proximityZoneTime, force a split
      //
      long timeStopped = previousTimestamp - refTimestamp;
      if (GeoTimeSerie.NO_LOCATION != refLocation && GeoTimeSerie.NO_LOCATION != location ) {

        double currentSpeed = 0.0D;
        if (GeoTimeSerie.NO_LOCATION != previousLocation && timestamp != previousTimestamp) {
          currentSpeed = GeoXPLib.orthodromicDistance(previousLocation, location) / ((double) (timestamp - previousTimestamp) / Constants.TIME_UNITS_PER_S);
        }

        // quit the radius, or speed greater than max stopped speed.
        if (GeoXPLib.orthodromicDistance(refLocation, location) > proximityZoneRadius || currentSpeed > proximityZoneMaxSpeed) {
          double zoneMeanSpeed = 0.0D;
          if (previousTimestamp != refTimestamp) {
            zoneMeanSpeed = proximityZoneTraveledDistance / ((double) (previousTimestamp - refTimestamp) / Constants.TIME_UNITS_PER_S);
          }
          if (timeStopped > proximityZoneTime && zoneMeanSpeed < proximityInZoneMaxMeanSpeed) {
            splitReason = SPLIT_TYPE_STOPPED;
            mustSplit = true;
            if (null != stoppedTimeLabel) {
              split.getMetadata().putToLabels(stoppedTimeLabel, Long.toString(timeStopped));
            }
          }
          refLocation = location;
          refTimestamp = timestamp;
          proximityZoneTraveledDistance = 0.0D;
        } else { // inside the radius
          if (GeoTimeSerie.NO_LOCATION != previousLocation) {
            proximityZoneTraveledDistance += GeoXPLib.orthodromicDistance(previousLocation, location);
          }
        }
      }

      //
      // mustSplit is also true on the first iteration
      //
      if (mustSplit) {
        //
        // End the previous split (could be null on the first iteration), add optional labels.
        //
        if (null != splitTypeLabel && null != split) {
          split.getMetadata().putToLabels(splitTypeLabel, splitReason);
        }
        split = gts.cloneEmpty();
        //
        // Start of the split, add optional labels.
        //
        if (null != startLabel) {
          split.getMetadata().putToLabels(startLabel, Long.toString(timestamp));
        }
        splits.add(split);
        mustSplit = false;

        refLocation = location;
        refTimestamp = timestamp;
        proximityZoneTraveledDistance = 0.0D;
      }

      GTSHelper.setValue(split, timestamp, location, elevation, value, false);

      //
      // On the last iteration, also manage the split type label (end or stopped)
      //
      if (idx == n - 1) {
        splitReason = SPLIT_TYPE_END;
        if ((previousTimestamp - refTimestamp) > proximityZoneTime) {
          splitReason = SPLIT_TYPE_STOPPED;
          if (null != stoppedTimeLabel) {
            split.getMetadata().putToLabels(stoppedTimeLabel, Long.toString(timeStopped));
          }
        }
        if (null != splitTypeLabel) {
          split.getMetadata().putToLabels(splitTypeLabel, splitReason);
        }
      }

      previousTimestamp = timestamp;
      previousLocation = location;

      idx++;
    }

    return splits;
  }
}
