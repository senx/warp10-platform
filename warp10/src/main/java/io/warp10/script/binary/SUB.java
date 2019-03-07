//
//   Copyright 2018  SenX S.A.S.
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

package io.warp10.script.binary;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Subtracts the two operands on top of the stack
 */
public class SUB extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final String typeCheckErrorMsg;

  public SUB(String name) {
    super(name);
    typeCheckErrorMsg = getName() + " can only operate on numeric values, vectors, matrices and numeric Geo Time Series.";
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op2 instanceof Number && op1 instanceof Number) {
      if (op1 instanceof Double || op2 instanceof Double) {
        stack.push(((Number) op1).doubleValue() - ((Number) op2).doubleValue());
      } else {
        stack.push(((Number) op1).longValue() - ((Number) op2).longValue());        
      }
    } else if (op1 instanceof RealMatrix && op2 instanceof RealMatrix) {
      stack.push(((RealMatrix) op1).subtract((RealMatrix) op2));
    } else if (op1 instanceof RealVector && op2 instanceof RealVector) {
      stack.push(((RealVector) op1).subtract((RealVector) op2));
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
      GeoTimeSerie gts1 = (GeoTimeSerie) op1;
      GeoTimeSerie gts2 = (GeoTimeSerie) op2;

      if (!(gts1.getType() == TYPE.DOUBLE || gts1.getType() == TYPE.LONG) || !(gts2.getType() == TYPE.DOUBLE || gts2.getType() == TYPE.LONG)) {
        throw new WarpScriptException(typeCheckErrorMsg);
      }

      // The result will be of type DOUBLE
      GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
      
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
      result.setType(TYPE.DOUBLE);
      
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

      while(idxa < na || idxb < nb) {
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
            GTSHelper.setValue(result, tsa, ((Number) GTSHelper.valueAtIndex(gts1, idxa)).doubleValue() - ((Number) GTSHelper.valueAtIndex(gts2, idxb)).doubleValue());
            // Advance both indices
            idxa++;
            idxb++;
          } else if (tsa < tsb) {
            // Timestamp at index A is lower than timestamp at index B
            //GTSHelper.setValue(result, tsa, ((Number) GTSHelper.valueAtIndex(gts1, idxa)).doubleValue());
            // Advance index for GTS A
            idxa++;
          } else {
            // Timestamp at index B is >= timestamp at index B
            //GTSHelper.setValue(result, tsb, ((Number) GTSHelper.valueAtIndex(gts2, idxb)).doubleValue());
            // Advance index for GTS B
            idxb++;
          }
        } else if (null == tsa && null != tsb) {
          // Index A has reached the end of GTS A, GTS B still has values to scan
          //GTSHelper.setValue(result, tsb, ((Number) GTSHelper.valueAtIndex(gts2, idxb)).doubleValue());          
          idxb++;
        } else if (null == tsb && null != tsa) {
          // Index B has reached the end of GTS B, GTS A still has values to scan
          //GTSHelper.setValue(result, tsa, ((Number) GTSHelper.valueAtIndex(gts1, idxa)).doubleValue());          
          idxa++;
        }
        if (idxa < na) {
          tsa = GTSHelper.tickAtIndex(gts1, idxa);
        }
        if (idxb < nb) {
          tsb = GTSHelper.tickAtIndex(gts2, idxb);
        }
      }

      stack.push(result);
    } else if ((op1 instanceof GeoTimeSerie && op2 instanceof Number) || (op1 instanceof Number && op2 instanceof GeoTimeSerie)) {
      boolean op1gts = op1 instanceof GeoTimeSerie;
      
      int n = op1gts ? GTSHelper.nvalues((GeoTimeSerie) op1) : GTSHelper.nvalues((GeoTimeSerie) op2);
      
      GeoTimeSerie result = op1gts ? ((GeoTimeSerie) op1).cloneEmpty(n) : ((GeoTimeSerie) op2).cloneEmpty();
      GeoTimeSerie gts = op1gts ? (GeoTimeSerie) op1 : (GeoTimeSerie) op2;

      if (!(gts.getType() == TYPE.LONG || gts.getType() == TYPE.DOUBLE)) {
        throw new WarpScriptException(typeCheckErrorMsg);
      }
      
      double op = op1gts ? ((Number) op2).doubleValue() : ((Number) op1).doubleValue();
      
      for (int i = 0; i < n; i++) {
       double value;
       if (op1gts) {
         value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() - op;
       } else {
         value = op - ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
       }
       GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
      }      

      stack.push(result);      
    } else {
      throw new WarpScriptException(typeCheckErrorMsg);
    }
    
    return stack;
  }
}
