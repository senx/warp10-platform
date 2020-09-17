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

package io.warp10.script.binary;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSOpsHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Raise the first operand to the power of the second.
 */
public class POW extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final String typeCheckErrorMsg;

  public POW(String name) {
    super(name);
    typeCheckErrorMsg = getName() + " can only operate on numeric values, vectors and numeric Geo Time Series.";
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();

    if (op2 instanceof Number && op1 instanceof Number) {
      double pow = Math.pow(((Number) op1).doubleValue(), ((Number) op2).doubleValue());
      if (op1 instanceof Double || op2 instanceof Double) {
        stack.push(pow);
      } else {
        stack.push((long) pow);
      }
    } else if ((op1 instanceof Number && op2 instanceof List) || (op1 instanceof List && op2 instanceof Number)) {
      List list = op1 instanceof List ? (List) op1 : (List) op2;
      Number operand = op1 instanceof Number ? (Number) op1 : (Number) op2;
      double operandD = operand.doubleValue();

      ArrayList<Object> result = new ArrayList<Object>(list.size());

      for (Object element: list) {
        if (!(element instanceof Number)) {
          throw new WarpScriptException(getName() + " expects lists to contain numerical values.");
        }

        // Compute the power, taking into account parameter order.
        double pow;
        if (op1 instanceof List) {
          pow = Math.pow(((Number) element).doubleValue(), operandD);
        } else {
          pow = Math.pow(operandD, ((Number) element).doubleValue());
        }

        // Cast result to long if both operands are not floating-point values.
        if (element instanceof Double || operand instanceof Double) {
          result.add(pow);
        } else {
          result.add((long) pow);
        }
      }

      stack.push(result);
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
      GeoTimeSerie gts1 = (GeoTimeSerie) op1;
      GeoTimeSerie gts2 = (GeoTimeSerie) op2;

      if (!(gts1.getType() == GeoTimeSerie.TYPE.DOUBLE || gts1.getType() == GeoTimeSerie.TYPE.LONG) || !(gts2.getType() == GeoTimeSerie.TYPE.DOUBLE || gts2.getType() == GeoTimeSerie.TYPE.LONG)) {
        throw new WarpScriptException(typeCheckErrorMsg);
      }

      // The result type is LONG if both inputs are LONG.
      GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
      GeoTimeSerie.TYPE type = (gts1.getType() == GeoTimeSerie.TYPE.LONG && gts2.getType() == GeoTimeSerie.TYPE.LONG) ? GeoTimeSerie.TYPE.LONG : GeoTimeSerie.TYPE.DOUBLE;
      result.setType(type);

      GTSOpsHelper.GTSBinaryOp op = new GTSOpsHelper.GTSBinaryOp() {
        @Override
        public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
          return Math.pow(((Number) GTSHelper.valueAtIndex(gtsa, idxa)).doubleValue(), ((Number) GTSHelper.valueAtIndex(gtsb, idxb)).doubleValue());
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

      if (!(gts.getType() == GeoTimeSerie.TYPE.LONG || gts.getType() == GeoTimeSerie.TYPE.DOUBLE)) {
        throw new WarpScriptException(typeCheckErrorMsg);
      }

      Number op = op1gts ? (Number) op2 : (Number) op1;

      // The result type is LONG if both inputs are LONG.
      GeoTimeSerie.TYPE type = (gts.getType() == GeoTimeSerie.TYPE.LONG && op instanceof Long) ? GeoTimeSerie.TYPE.LONG : GeoTimeSerie.TYPE.DOUBLE;
      result.setType(type);

      double opDouble = op.doubleValue();
      for (int i = 0; i < n; i++) {
        double value;
        if (op1gts) {
          value = Math.pow(((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue(), opDouble);
        } else {
          value = Math.pow(opDouble, ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue());
        }
        GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
      }

      stack.push(result);
    } else {
      throw new WarpScriptException(getName() + " can only operate on numeric values.");
    }

    return stack;
  }
}
