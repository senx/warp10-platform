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

package io.warp10.script.binary;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSOpsHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public abstract class ComparisonOperation extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public abstract boolean operator(int op1, int op2);
  
  public ComparisonOperation(String name) {
    super(name);
  }
  
  private final GTSOpsHelper.GTSBinaryOp stringOp = new GTSOpsHelper.GTSBinaryOp() {
    @Override
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
      boolean ComparisonResult = operator((GTSHelper.valueAtIndex(gtsa, idxa)).toString().compareTo((GTSHelper.valueAtIndex(gtsb, idxb)).toString()), 0);
      return ComparisonResult ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
    }
  };
  
  private final GTSOpsHelper.GTSBinaryOp numberOp = new GTSOpsHelper.GTSBinaryOp() {
    @Override
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
      if (GTSHelper.valueAtIndex(gtsa, idxa) instanceof Double && Double.isNaN((Double) GTSHelper.valueAtIndex(gtsa, idxa))) {
        // if one is NaN, result of comparison is always false, return null.
        return null;
      } else if (GTSHelper.valueAtIndex(gtsb, idxb) instanceof Double && Double.isNaN((Double) GTSHelper.valueAtIndex(gtsb, idxb))) {
        // if one is NaN, result of comparison is always false, return null.
        return null;
      } else {
        // both inputs are numbers
        boolean ComparisonResult = operator(EQ.compare((Number) (GTSHelper.valueAtIndex(gtsa, idxa)), (Number) (GTSHelper.valueAtIndex(gtsb, idxb))), 0);
        return ComparisonResult ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
      }
    }
  };
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    String exceptionMessage = getName() + "  can only operate on homogeneous numeric or string types.";
    
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op1 instanceof Double && Double.isNaN((Double) op1) && !(op2 instanceof GeoTimeSerie)) { // Do we have only one NaN ?
      stack.push(false);
    } else if (op2 instanceof Double && Double.isNaN((Double) op2) && !(op1 instanceof GeoTimeSerie)) { // Do we have only one NaN ?
      stack.push(false);
    } else if (op2 instanceof Number && op1 instanceof Number) {
      stack.push(operator(EQ.compare((Number) op1, (Number) op2), 0));
    } else if (op2 instanceof String && op1 instanceof String) {
      stack.push(operator(op1.toString().compareTo(op2.toString()), 0));
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
      GeoTimeSerie gts1 = (GeoTimeSerie) op1;
      GeoTimeSerie gts2 = (GeoTimeSerie) op2;
      if (GeoTimeSerie.TYPE.UNDEFINED == gts1.getType() || GeoTimeSerie.TYPE.UNDEFINED == gts2.getType()) {
        // gts1 or gts2 empty, return an empty gts
        stack.push(new GeoTimeSerie());
      } else if (GeoTimeSerie.TYPE.STRING == gts1.getType() && GeoTimeSerie.TYPE.STRING == gts2.getType()) {
        // both strings, compare lexicographically
        GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
        result.setType(GeoTimeSerie.TYPE.STRING);
        GTSOpsHelper.applyBinaryOp(result, gts1, gts2, stringOp);
        stack.push(result);
      } else if ((GeoTimeSerie.TYPE.LONG == gts1.getType() || GeoTimeSerie.TYPE.DOUBLE == gts1.getType())
          && (GeoTimeSerie.TYPE.LONG == gts2.getType() || GeoTimeSerie.TYPE.DOUBLE == gts2.getType())) {
        // both are numbers
        GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
        if (GeoTimeSerie.TYPE.DOUBLE == gts1.getType() || GeoTimeSerie.TYPE.DOUBLE == gts2.getType()) {
          //one input gts is double
          result.setType(GeoTimeSerie.TYPE.DOUBLE);
        } else {
          result.setType(GeoTimeSerie.TYPE.LONG);
        }
        GTSOpsHelper.applyBinaryOp(result, gts1, gts2, numberOp);
        stack.push(result);
      } else {
        throw new WarpScriptException(exceptionMessage);
      }
    } else if (op1 instanceof GeoTimeSerie && GeoTimeSerie.TYPE.UNDEFINED == ((GeoTimeSerie) op1).getType() && (op2 instanceof String || op1 instanceof Number)) {
      // empty gts compared to a string or a number
      stack.push(((GeoTimeSerie) op1).cloneEmpty());
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof String && GeoTimeSerie.TYPE.STRING == ((GeoTimeSerie) op1).getType()) {
      // one string gts compared to a string
      GeoTimeSerie gts = (GeoTimeSerie) op1;
      GeoTimeSerie result = gts.cloneEmpty();
      result.setType(GeoTimeSerie.TYPE.STRING);
      for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
        GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
            operator((GTSHelper.valueAtIndex(gts, i)).toString().compareTo(op2.toString()), 0) ? GTSHelper.valueAtIndex(gts, i) : null, false);
      }
      stack.push(result);
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof Number && GeoTimeSerie.TYPE.DOUBLE == ((GeoTimeSerie) op1).getType()) {
      // one double gts compared to number
      GeoTimeSerie gts = (GeoTimeSerie) op1;
      GeoTimeSerie result = gts.cloneEmpty();
      result.setType(GeoTimeSerie.TYPE.DOUBLE);
      if (op2 instanceof Double && Double.isNaN((Double) op2)) {
        // nothing is comparable to NaN
        stack.push(result);
      } else {
        for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
          if (!Double.isNaN((Double) GTSHelper.valueAtIndex(gts, i))) {
            //exclude NaN inputs
            GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
                operator(EQ.compare((Number) GTSHelper.valueAtIndex(gts, i), (Number) op2), 0) ? GTSHelper.valueAtIndex(gts, i) : null, false);
          }
        }
        stack.push(result);
      }
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof Number && GeoTimeSerie.TYPE.LONG == ((GeoTimeSerie) op1).getType()) {
      // one long gts compared to number
      GeoTimeSerie gts = (GeoTimeSerie) op1;
      GeoTimeSerie result = gts.cloneEmpty();
      result.setType(GeoTimeSerie.TYPE.LONG);
      if (op2 instanceof Double && Double.isNaN((Double) op2)) {
        // nothing is comparable to NaN
        stack.push(result);
      } else {
        for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
          GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
              operator(EQ.compare((Number) GTSHelper.valueAtIndex(gts, i), (Number) op2), 0) ? GTSHelper.valueAtIndex(gts, i) : null, false);
        }
        stack.push(result);
      }
    } else {
      throw new WarpScriptException(exceptionMessage);
    }
    
    return stack;
  }
}
