//
//   Copyright 2018-2020  SenX S.A.S.
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

import com.geoxp.GeoXPLib;
import io.warp10.DoubleUtils;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.Arrays;

/**
 * Perform Dynamic Time Warping pseudo-distance computation between two GTSs.
 * It can be done on values, locations, elevations or timestamps.
 */
public class DTW extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  // Constants to specify on which characteristic of the GTSs we want to do the DTW.
  // Define as power of 2 in case we want to combine them.
  public static final int TIMESTAMPS = 1;
  public static final int LOCATIONS = 2;
  public static final int ELEVATIONS = 4;
  public static final int VALUES = 8;

  /**
   * Should we normalize?
   */
  private final boolean normalize;

  /**
   * Should we do Z-Normalization or 0-1 normalization?
   */
  private final boolean znormalize;

  public DTW(String name, boolean normalize, boolean znormalize) {
    super(name);
    this.normalize = normalize;
    this.znormalize = znormalize;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    int type = VALUES;
    DTWDistance distance = DTW::manhattan;

    // Optional GTS characteristic to apply DTW on.
    if (o instanceof String) {
      String charac = ((String) o).toLowerCase();
      switch (charac) {
        case "values":
          type = VALUES;
          break;
        case "locations":
          type = LOCATIONS;
          break;
        case "elevations":
          type = ELEVATIONS;
          break;
        case "timestamps":
          type = TIMESTAMPS;
          break;
        default:
          throw new WarpScriptException(getName() + " expects the characteristic of the GTS to compute the DTW on to be values, locations, elevations or timestamps.");
      }

      o = stack.pop();
    }

    // Optional distance spec
    if (o instanceof String) {
      String dist = ((String) o).toLowerCase();

      switch (dist) {
        case "manhattan":
          distance = DTW::manhattan;
          break;
        case "euclidean":
          distance = DTW::euclidean;
          break;
        case "squaredeuclidean":
          distance = DTW::squaredEuclidean;
          break;
        case "loxodromic":
          distance = DTW::loxodromic;
          break;
        case "orthodromic":
          distance = DTW::orthodromic;
          break;
        default:
          throw new WarpScriptException(getName() + " expects the distance to use in the DTW to be manhattan, euclidean, loxodromic or orthodromic.");
      }

      o = stack.pop();
    }

    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a numeric threshold on top of the stack.");
    }

    double threshold = ((Number) o).doubleValue();

    // If the threshold is not strictly positive, consider there is no threshold.
    if (threshold <= 0.0D) {
      threshold = Double.POSITIVE_INFINITY;
    }

    o = stack.pop();

    // Optional window parameter.
    int window = Integer.MAX_VALUE;

    if (o instanceof Long) {
      window = (int) Math.min(Integer.MAX_VALUE, (Long) o);

      // If the window is negative, consider there is no window.
      if (window < 0) {
        window = Integer.MAX_VALUE;
      }

      o = stack.pop();
    }

    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects two Geo Time Series below the threshold.");
    }

    GeoTimeSerie gts1 = (GeoTimeSerie) o;

    o = stack.pop();

    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects two Geo Time Series below the threshold.");
    }

    GeoTimeSerie gts2 = (GeoTimeSerie) o;

    //
    // Compute DTW and push it to the stack
    //

    double d;
    try {
      d = compute(gts1, gts2, window, threshold, type, distance);
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " failed.", wse);
    }

    stack.push(d);

    return stack;
  }

  /**
   * Compute the DTW pseudo-distance on two GTS.
   * @param gts1 One of the two GTSs to compare to the other.
   * @param gts2 One of the two GTSs to compare to the other.
   * @param window The window defining th maximum index difference when matching the GTSs. Integer.MAX_VALUE for no window constraint.
   * @param threshold The maximum value of DTW. If the computed pseudo-distance is over this value, it is aborted and returns -1;
   * @param type The characteristic of the GTSs to run the DTW on.
   * @param distance The distance to use to compare GTS values.
   * @return The pseudo-distance, -1 if the value would be over threshold.
   * @throws WarpScriptException in case the DTW cannot be computed given the parameters.
   */
  public final double compute(GeoTimeSerie gts1, GeoTimeSerie gts2, int window, double threshold, int type, DTWDistance distance) throws WarpScriptException {

    //
    // Check that the type of the GTS is numerical
    //
    if (VALUES == type) {
      if (TYPE.LONG != gts1.getType() && TYPE.DOUBLE != gts1.getType()) {
        throw new WarpScriptException(getName() + " can only operate on numerical Geo Time Series.");
      }

      if (TYPE.LONG != gts2.getType() && TYPE.DOUBLE != gts2.getType()) {
        throw new WarpScriptException(getName() + " can only operate on numerical Geo Time Series.");
      }
    }

    //
    // Sort GTS in chronological order
    //

    GTSHelper.sort(gts1);
    GTSHelper.sort(gts2);

    //
    // Extract values, compute min/max and quantize values (x - max/(max - min))
    // Values are multi-dimensional, with first index being the dimension and second index the element index. See
    // DTW.compute(double[][], int, int, double[][], int, int, int, double, io.warp10.script.functions.DTW.DTWDistance)
    // for more details.
    //
    double[][] values1;
    double[][] values2;

    if (VALUES == type) {
      values1 = new double[][] {GTSHelper.getValuesAsDouble(gts1)};
      values2 = new double[][] {GTSHelper.getValuesAsDouble(gts2)};
    } else if (LOCATIONS == type) {
      long[] locations1 = GTSHelper.getOriginalLocations(gts1);
      if (null == locations1) {
        throw new WarpScriptException(getName() + " expects GTSs to have locations when DTW is applied to them.");
      }

      values1 = new double[2][locations1.length];
      for (int i = 0; i < locations1.length; i++) {
        if (GeoTimeSerie.NO_LOCATION == locations1[i]) {
          throw new WarpScriptException(getName() + " expects GTSs to have locations when DTW is applied to them.");
        }
        double[] latLon = GeoXPLib.fromGeoXPPoint(locations1[i]);
        values1[0][i] = latLon[0];
        values1[1][i] = latLon[1];
      }

      long[] locations2 = GTSHelper.getOriginalLocations(gts2);
      if (null == locations2) {
        throw new WarpScriptException(getName() + " expects GTSs to have locations when DTW is applied to them.");
      }

      values2 = new double[2][locations2.length];
      for (int i = 0; i < locations2.length; i++) {
        if (GeoTimeSerie.NO_LOCATION == locations2[i]) {
          throw new WarpScriptException(getName() + " expects GTSs to have locations when DTW is applied to them.");
        }
        double[] latLon = GeoXPLib.fromGeoXPPoint(locations2[i]);
        values2[0][i] = latLon[0];
        values2[1][i] = latLon[1];
      }
    } else if (ELEVATIONS == type) {
      long[] elev1 = GTSHelper.getOriginalElevations(gts1);
      if (null == elev1) {
        throw new WarpScriptException(getName() + " expects GTSs to have elevations when DTW is applied to them.");
      }

      values1 = new double[1][elev1.length];
      for (int i = 0; i < elev1.length; i++) {
        if (GeoTimeSerie.NO_ELEVATION == elev1[i]) {
          throw new WarpScriptException(getName() + " expects GTSs to have elevations when DTW is applied to them.");
        }
        values1[0][i] = elev1[i];
      }

      long[] elev2 = GTSHelper.getOriginalElevations(gts2);
      if (null == elev2) {
        throw new WarpScriptException(getName() + " expects GTSs to have elevations when DTW is applied to them.");
      }

      values2 = new double[1][elev2.length];
      for (int i = 0; i < elev1.length; i++) {
        if (GeoTimeSerie.NO_ELEVATION == elev2[i]) {
          throw new WarpScriptException(getName() + " expects GTSs to have elevations when DTW is applied to them.");
        }
        values2[0][i] = elev2[i];
      }
    } else if (TIMESTAMPS == type) {
      double[] timestamps1 = Arrays.stream(GTSHelper.getTicks(gts1)).asDoubleStream().toArray();
      double[] timestamps2 = Arrays.stream(GTSHelper.getTicks(gts2)).asDoubleStream().toArray();
      values1 = new double[][] {timestamps1};
      values2 = new double[][] {timestamps2};
    } else {
      throw new IllegalArgumentException("DTW type is unknown: " + type);
    }

    if (this.normalize) {
      //
      // Perform normalization of values1 and values2, on each dimension.
      //
      for (int dimension = 0; dimension < values1.length; dimension++) {

        double[] dimVal1 = values1[dimension];
        double[] dimVal2 = values2[dimension];
        if (this.znormalize) {
          double[] musigma = DoubleUtils.musigma(dimVal1, true);

          for (int i = 0; i < dimVal1.length; i++) {
            dimVal1[i] = (dimVal1[i] - musigma[0]) / musigma[1];
          }

          musigma = DoubleUtils.muvar(dimVal2);
          for (int i = 0; i < dimVal2.length; i++) {
            dimVal2[i] = (dimVal2[i] - musigma[0]) / musigma[1];
          }
        } else {
          double min = Double.POSITIVE_INFINITY;
          double max = Double.NEGATIVE_INFINITY;

          for (int i = 0; i < dimVal1.length; i++) {
            if (dimVal1[i] < min) {
              min = dimVal1[i];
            }
            if (dimVal1[i] > max) {
              max = dimVal1[i];
            }
          }

          double range = max - min;

          if (0.0D == range) {
            throw new WarpScriptException(getName() + " cannot normalize a constant GTS.");
          }

          for (int i = 0; i < dimVal1.length; i++) {
            dimVal1[i] = (dimVal1[i] - min) / range;
          }

          min = Double.POSITIVE_INFINITY;
          max = Double.NEGATIVE_INFINITY;

          for (int i = 0; i < dimVal2.length; i++) {
            if (dimVal2[i] < min) {
              min = dimVal2[i];
            }
            if (dimVal2[i] > max) {
              max = dimVal2[i];
            }
          }

          range = max - min;

          if (0.0D == range) {
            throw new WarpScriptException(getName() + " cannot normalize a constant GTS.");
          }

          for (int i = 0; i < dimVal2.length; i++) {
            dimVal2[i] = (dimVal2[i] - min) / range;
          }
        }
      }
    }

    int len1 = values1[0].length;
    int len2 = values2[0].length;

    return compute(values1, 0, len1, values2, 0, len2, window, threshold, distance);
  }

  /**
   * Compute the DTW pseudo-distance on two multi-dimensional data.
   * @param values1 One of the two series to compare to the other. First index is the dimension, second is the element index. This way, less arrays are allocated because usually dimensions << number of elements.
   * @param offset1 The start index from which to consider the data in values1.
   * @param len1 Number of elements to consider in values1.
   * @param values2 One of the two series to compare to the other. First index is the dimension, second is the element index.
   * @param offset1 The start index from which to consider the data in values2.
   * @param len2 Number of elements to consider in values2.
   * @param window The window defining th maximum index difference when matching the data. Integer.MAX_VALUE for no window constraint.
   * @param threshold The maximum value of DTW. If the computed pseudo-distance is over this value, it is aborted and returns -1;
   * @param distance The distance to use to compare GTS values.
   * @return The pseudo-distance, -1 if the value would be over threshold.
   * @throws WarpScriptException in case the DTW cannot be computed given the parameters.
   */
  public static double compute(double[][] values1, int offset1, int len1, double[][] values2, int offset2, int len2, int window, double threshold, DTWDistance distance) throws WarpScriptException {
    // Make sure the values have the same dimension
    if (values1.length != values2.length) {
      throw new IllegalArgumentException("values must have the same dimension.");
    }

    // Make sure each dimension has the same number of points
    for (int dimension = 1; dimension < values1.length; dimension++) {
      if (values1[0].length != values1[dimension].length || values2[0].length != values2[dimension].length) {
        throw new WarpScriptException("values must have the same number of element per dimension.");
      }
    }

    //
    // Make sure value1 is the shortest array
    // This ensure the a and b arrays created after are as small as possible.
    //

    if (len1 > len2) {
      double[][] tmp = values1;
      values1 = values2;
      values2 = tmp;
      int tmpint = offset1;
      offset1 = offset2;
      offset2 = tmpint;
      tmpint = len1;
      len1 = len2;
      len2 = tmpint;
    }

    //
    // Now run DTW.
    // We allocate two columns so we can run DTW, only allocating.
    // To better visualize the algorithm, and understand the terminology used below, imagine the problem like this,
    // values1 and values2 are 1-dimension:
    //
    //         \    .
    //          |   .
    //          \   .
    //           \  .
    // values1    | 9
    // (turned    / 7
    //   90°)    |  5 ?
    //           |  3 7
    //          /   2 4
    //              _ / ¯ \ _           / ¯ ¯ ¯ ¯ \ _ _ _ _ _
    //                        \ _ _ _ /
    //                          values2
    //              a b
    // When computing "?" we will compute the distance between values1[0][2] and values2[0][1] then add it to the lowest
    // distance between the left one (5), the bottom one (7) or the bottom-left (3) one. When b is filled, a is of
    // no use anymore, so b is stored in a, and a will be used as the new b, discarding its values.
    // All the values taken by a and b are called the matching matrix in the following.
    //

    double[] a = new double[len1];
    double[] b = new double[len1];

    // Tell if any value in the current column is below the threshold.
    boolean belowThreshold = false;

    // There's no need for the window to be greater than len2-1 and cannot be smaller than len2-len1, else it won't
    // be able to match the last point of values1 to the last point of values2.
    window = Math.max(Math.min(window, len2 - 1), len2 - len1);

    for (int i = offset2; i < len2; i++) {
      // i - window cannot overflow but i + window + 1 can.
      // Check for an overflow, knowing that i + 1 cannot overflow.
      int maxWindow;
      try {
        maxWindow = Math.addExact(i + 1, window);
      } catch (ArithmeticException ae) {
        maxWindow = Integer.MAX_VALUE;
      }
      for (int j = Math.max(offset1, i - window); j < Math.min(len1, maxWindow); j++) {

        //
        // Extract surrounding values
        //

        double bestPreviousTotalDistance;

        if (0 == i && 0 == j) {
          // At the bottom-left of the matching matrix, so there is not previous distance.
          bestPreviousTotalDistance = 0D;
        } else {
          // If at the first element of values2, the value at the left in not valid because it is outside the matching matrix.
          // If at the end of the window, the value at the left in not valid because it was outside the previous window.
          double left = (i > 0 && (i + window) != j) ? a[j] : Double.POSITIVE_INFINITY;
          // If at the bottom of the matrix, bottom values does not exist.
          // If at the start of the window, the value at the bottom in not valid because it was not computed.
          double bottom = (j > 0 && (i - window) != j) ? b[j - 1] : Double.POSITIVE_INFINITY;
          // If at the bottom of the matrix, bottom-left values does not exist.
          double bottomLeft = j > 0 ? a[j - 1] : Double.POSITIVE_INFINITY;

          bestPreviousTotalDistance = Math.min(left, Math.min(bottom, bottomLeft));
        }

        // Avoid computing the distance if the best total previous distance is already over the threshold.
        if (threshold >= bestPreviousTotalDistance) {
          //
          // Compute distance.
          // DTW simply considers the delta in values, not the delta in indices
          //

          double d = distance.measure(values1, j, values2, i);

          b[j] = d + bestPreviousTotalDistance;

          if (!belowThreshold && b[j] <= threshold) {
            belowThreshold = true;
          }
        } else {
          b[j] = Double.POSITIVE_INFINITY;
        }
      }

      // Exit if no value is below threshold
      if (!belowThreshold) {
        return -1.0D;
      }

      // Shift b into a
      double[] tmp = a;
      a = b;
      b = tmp;
    }

    // Result is the top right value of the match matrix.
    // It may be over the threshold if a[k] <= threshold for some k<(len1-1);
    if (a[len1 - 1] <= threshold) {
      return a[len1 - 1];
    } else {
      return -1;
    }
  }

  // Functional interface defining a distance function used by DTW.
  @FunctionalInterface
  public interface DTWDistance {
    double measure(double[][] values1, int index1, double[][] values2, int index2) throws WarpScriptException;
  }

  public static double manhattan(double[][] values1, int index1, double[][] values2, int index2) throws WarpScriptException {
    double d = 0D;
    for (int dimension = 0; dimension < values1.length; dimension++) {
      d += Math.abs(values1[dimension][index1] - values2[dimension][index2]);
    }
    return d;
  }

  public static double euclidean(double[][] values1, int index1, double[][] values2, int index2) throws WarpScriptException {
    double d = 0D;
    for (int dimension = 0; dimension < values1.length; dimension++) {
      d += Math.pow(values1[dimension][index1] - values2[dimension][index2], 2.0D);
    }
    return Math.sqrt(d);
  }

  public static double squaredEuclidean(double[][] values1, int index1, double[][] values2, int index2) throws WarpScriptException {
    double d = 0D;
    for (int dimension = 0; dimension < values1.length; dimension++) {
      d += Math.pow(values1[dimension][index1] - values2[dimension][index2], 2.0D);
    }
    return d;
  }

  public static double loxodromic(double[][] values1, int index1, double[][] values2, int index2) throws WarpScriptException {
    if (2 != values1.length) {
      throw new WarpScriptException("Loxodromic distance must be given two dimensions: lat, lon.");
    }

    double lat1 = Math.toRadians(values1[0][index1]);
    double lon1 = Math.toRadians(values1[1][index1]);
    double lat2 = Math.toRadians(values2[0][index2]);
    double lon2 = Math.toRadians(values2[1][index2]);


    double x = (lon1 - lon2) * Math.cos((lat1 + lat2) / 2.0D);
    double y = lat1 - lat2;

    return (180.0D / Math.PI) * (1852.0D * 60.0D) * Math.sqrt(x * x + y * y);
  }

  public static double orthodromic(double[][] values1, int index1, double[][] values2, int index2) throws WarpScriptException {
    if (2 != values1.length) {
      throw new WarpScriptException("Orthodromic distance must be given two dimensions: lat, lon.");
    }

    double lat1 = Math.toRadians(values1[0][index1]);
    double lon1 = Math.toRadians(values1[1][index1]);
    double lat2 = Math.toRadians(values2[0][index2]);
    double lon2 = Math.toRadians(values2[1][index2]);

    // Return result in meters
    return (180.0D / Math.PI) * (1852.0D * 60.0D) * Math.acos(Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos((lon2 - lon1)));
  }
}
