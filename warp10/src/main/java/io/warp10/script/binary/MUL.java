//
//   Copyright 2020  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSOpsHelper;
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
 * Multiply the two operands on top of the stack
 */
public class MUL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final String typeCheckErrorMsg;

  public MUL(String name) {
    super(name);
    typeCheckErrorMsg = getName() + " can only operate on numeric values, vectors, matrices and numeric Geo Time Series.";
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op2 instanceof Number && op1 instanceof Number) {
      if (op1 instanceof Double || op2 instanceof Double) {
        stack.push(((Number) op1).doubleValue() * ((Number) op2).doubleValue());
      } else {
        stack.push(((Number) op1).longValue() * ((Number) op2).longValue());        
      }
    } else if (op2 instanceof RealMatrix && op1 instanceof RealMatrix) {
      stack.push(((RealMatrix) op1).multiply((RealMatrix) op2));
    } else if (op1 instanceof RealMatrix && op2 instanceof Number) {
      stack.push(((RealMatrix) op1).scalarMultiply(((Number) op2).doubleValue()));
    } else if (op2 instanceof RealMatrix && op1 instanceof Number) {
      stack.push(((RealMatrix) op2).scalarMultiply(((Number) op1).doubleValue()));
    } else if (op2 instanceof RealMatrix && op1 instanceof RealVector) {
      stack.push(((RealMatrix) op2).preMultiply((RealVector) op1));
    } else if (op1 instanceof RealMatrix && op2 instanceof RealVector) {
      stack.push(((RealMatrix) op1).operate((RealVector) op2));
    } else if (op1 instanceof RealVector && op2 instanceof Number) {
      stack.push(((RealVector) op1).mapMultiply(((Number) op2).doubleValue()));
    } else if (op2 instanceof RealVector && op1 instanceof Number) {
      stack.push(((RealVector) op2).mapMultiply(((Number) op1).doubleValue()));
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
      GeoTimeSerie gts1 = (GeoTimeSerie) op1;
      GeoTimeSerie gts2 = (GeoTimeSerie) op2;

      // Only only numeric and empty GTSs.
      if (!(gts1.getType() == TYPE.DOUBLE || gts1.getType() == TYPE.LONG || 0 == GTSHelper.nvalues(gts1))
          || !(gts2.getType() == TYPE.DOUBLE || gts2.getType() == TYPE.LONG || 0 == GTSHelper.nvalues(gts2))) {
        throw new WarpScriptException(typeCheckErrorMsg);
      }

      // The result type is LONG if both inputs are LONG.
      GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
      result.setType((gts1.getType() == TYPE.LONG && gts2.getType() == TYPE.LONG) ? TYPE.LONG : TYPE.DOUBLE);

      GTSOpsHelper.GTSBinaryOp op = new GTSOpsHelper.GTSBinaryOp() {
        @Override
        public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
          return ((Number) GTSHelper.valueAtIndex(gtsa, idxa)).doubleValue() * ((Number) GTSHelper.valueAtIndex(gtsb, idxb)).doubleValue();
        }
      };

      GTSOpsHelper.applyBinaryOp(result, gts1, gts2, op);

      // If result is empty, set type and sizehint to default.
      if (0 == result.size()) {
        result = result.cloneEmpty();
      }
      stack.push(result);
    } else if ((op1 instanceof GeoTimeSerie && op2 instanceof Number) || (op1 instanceof Number && op2 instanceof GeoTimeSerie)) {
      boolean op1gts = op1 instanceof GeoTimeSerie;
      
      int n = op1gts ? GTSHelper.nvalues((GeoTimeSerie) op1) : GTSHelper.nvalues((GeoTimeSerie) op2);
      
      GeoTimeSerie result = op1gts ? ((GeoTimeSerie) op1).cloneEmpty(n) : ((GeoTimeSerie) op2).cloneEmpty();
      GeoTimeSerie gts = op1gts ? (GeoTimeSerie) op1 : (GeoTimeSerie) op2;

      // Returns immediately a new clone if gts is empty.
      if (0 == n) {
        stack.push(result);
        return stack;
      }

      if (!(gts.getType() == TYPE.LONG || gts.getType() == TYPE.DOUBLE)) {
        throw new WarpScriptException(typeCheckErrorMsg);
      }

      Number op = op1gts ? (Number) op2 : (Number) op1;

      if (op instanceof Double || gts.getType() == TYPE.DOUBLE) {
        double opDouble = op.doubleValue();
        for (int i = 0; i < n; i++) {
          double value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() * opDouble;
          GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
        }
      } else {
        long opLong = op.longValue();
        for (int i = 0; i < n; i++) {
          long value = ((Number) GTSHelper.valueAtIndex(gts, i)).longValue() * opLong;
          GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
        }
      }

      stack.push(result);                   
    } else {
      throw new WarpScriptException(typeCheckErrorMsg);
    }
    
    return stack;
  }
}
