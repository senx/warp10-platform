//
//   Copyright 2018-2023  SenX S.A.S.
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

public class GTSOpsHelper {

  public static interface GTSUnaryOp {
    public Object op(GeoTimeSerie gts, int idx);
  }

  public static interface GTSMixedOp {
    public Object op(GeoTimeSerie gts, int idx, Object param);
  }

  public static interface GTSBinaryOp {
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb);
  }

  /**
   * Apply a unary operator to the values of a GTS, resulting in another GTS. Location and elevation info are copied to the result GTS.
   * @param result The resulting GTS, for each tick of gts, result[tick]=op(gts[tick]).
   * @param gts The GTS from where to take the values from.
   * @param op The operator to apply to the values.
   */
  public static void applyUnaryOp(GeoTimeSerie result, GeoTimeSerie gts, GTSUnaryOp op) {
    int n = GTSHelper.nvalues(gts);

    for (int i = 0; i < n; i++) {
      Object value = op.op(gts, i);
      GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
    }
  }

  /**
   * Apply a 1-parameter unary operator to the values of a GTS, resulting in another GTS. Location and elevation info are copied to the result GTS.
   * @param result The resulting GTS, for each tick of gts, result[tick]=op(gts[tick]).
   * @param gts The GTS from where to take the values from.
   * @param op The operator to apply to the values.
   * @param param1 The parameter for the application of the operator.
   */
  public static void applyUnaryOp(GeoTimeSerie result, GeoTimeSerie gts, GTSMixedOp op, Object param1) {
    int n = GTSHelper.nvalues(gts);

    for (int i = 0; i < n; i++) {
      Object value = op.op(gts, i, param1);
      GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
    }
  }

  /**
   * Set the bucketize parameters of the result GTS according to those of gts1 and gts2.
   * If both GTSs have the same bucket span and buckets are aligned, the result covers the common buckets of gts1 and gts2.
   * In all the other case, the result is not bucketized.
   * @param result The GTS whose bucketize parameters must be set (or not).
   * @param gts1 One of the two GTSs to get the bucketize parameters from.
   * @param gts2 One of the two GTSs to get the bucketize parameters from.
   */
  public static void handleBucketization(GeoTimeSerie result, GeoTimeSerie gts1, GeoTimeSerie gts2) {
    if (GTSHelper.isBucketized(gts1) && GTSHelper.isBucketized(gts2)) {
      if (GTSHelper.getBucketSpan(gts1) == GTSHelper.getBucketSpan(gts2)) {
        // Both GTS have the same bucket span, check their lastbucket to see if they have the
        // same remainder modulo the bucketspan
        long bucketspan = GTSHelper.getBucketSpan(gts1);
        if (GTSHelper.getLastBucket(gts1) % bucketspan == GTSHelper.getLastBucket(gts2) % bucketspan) {
          GTSHelper.setBucketSpan(result, bucketspan);
          GTSHelper.setLastBucket(result, Math.max(GTSHelper.getLastBucket(gts1), GTSHelper.getLastBucket(gts2)));
          // Compute the number of bucket counts
          long firstbucket = Math.min(GTSHelper.getLastBucket(gts1) - (GTSHelper.getBucketCount(gts1) - 1) * bucketspan, GTSHelper.getLastBucket(gts2) - (GTSHelper.getBucketCount(gts2) - 1) * bucketspan);
          int bucketcount = (int) ((GTSHelper.getLastBucket(result) - firstbucket) / bucketspan) + 1;
          GTSHelper.setBucketCount(result, bucketcount);
        }
      }
    }
  }

  public static void applyBinaryOp(GeoTimeSerie result, GeoTimeSerie gts1, GeoTimeSerie gts2, GTSBinaryOp op) {
    applyBinaryOp(result, gts1, gts2, op, false);
  }

  /**
   * Apply a binary operator to the values of two GTSs, resulting in a single GTS. Only common ticks are considered.
   * @param result GTS containing all the results. ie for each common tick to gts1 and gts2, result[tick]=op(gts1[tick], gts2[tick]).
   * @param gts1 First GTS to take values from.
   * @param gts2 Second GTS to take values from.
   * @param op Operator which will compute the value to be put in result.
   * @param copyGts1Location Whether to copy the location and elevation of gts1 to result. If false, points in result have no location and no elevation.
   */
  public static void applyBinaryOp(GeoTimeSerie result, GeoTimeSerie gts1, GeoTimeSerie gts2, GTSBinaryOp op, boolean copyGts1Location) {
    // Determine if result should be bucketized or not
    handleBucketization(result, gts1, gts2);
    
    // Sort GTS
    GTSHelper.sort(gts1);
    GTSHelper.sort(gts2);
    
    // Sweeping line over the timestamps
    int idxa = 0;
    int idxb = 0;
    
    int na = GTSHelper.nvalues(gts1);
    int nb = GTSHelper.nvalues(gts2);
    
    Long tsa = null;
    Long tsb = null;

    if (idxa < na) {
      tsa = GTSHelper.tickAtIndex(gts1, idxa);
    }
    if (idxb < na) {
      tsb = GTSHelper.tickAtIndex(gts2, idxb);
    }
    
    while (idxa < na || idxb < nb) {
      if (idxa >= na) {
        tsa = null;
      }
      if (idxb >= nb) {
        tsb = null;
      }
      if (null != tsa && null != tsb) {
        // We have values at the current index for both GTS
        if (0 == tsa.compareTo(tsb)) {
          // Both indices indicate the same timestamp
          if (copyGts1Location) {
            GTSHelper.setValue(result, tsa, GTSHelper.locationAtIndex(gts1, idxa), GTSHelper.elevationAtIndex(gts1, idxa), op.op(gts1, gts2, idxa, idxb), false);
          } else {
            GTSHelper.setValue(result, tsa, op.op(gts1, gts2, idxa, idxb));
          }
          // Advance both indices
          idxa++;
          idxb++;
        } else if (tsa < tsb) {
          // Timestamp at index A is lower than timestamp at index B
          // Advance index for GTS A
          idxa++;
        } else {
          // Timestamp at index B is >= timestamp at index B
          // Advance index for GTS B
          idxb++;
        }
      } else if (null == tsa && null != tsb) {
        // Index A has reached the end of GTS A, GTS B still has values to scan
        idxb++;
      } else if (null == tsb && null != tsa) {
        // Index B has reached the end of GTS B, GTS A still has values to scan
        idxa++;
      }
      if (idxa < na) {
        tsa = GTSHelper.tickAtIndex(gts1, idxa);
      }
      if (idxb < nb) {
        tsb = GTSHelper.tickAtIndex(gts2, idxb);
      }
    }
  }
}
