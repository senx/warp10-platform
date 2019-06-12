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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;

import java.util.List;

/**
 * Conditional boolean operation taking either:
 * - two operands on top of the stack.
 * - a list of booleans or boolean-returning-macros.
 * This class implements short-circuit evaluation (https://en.wikipedia.org/wiki/Short-circuit_evaluation).
 */
public abstract class CondShortCircuit extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  protected final boolean triggerValue;

  public abstract boolean operator(boolean bool1, boolean bool2);

  public CondShortCircuit(String name, boolean triggerValue) {
    super(name);
    this.triggerValue = triggerValue;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    String exceptionMessage = getName() + " can only operate on two boolean values, or two GTS, or a GTS and a boolean, or a list of booleans or macros, each macro putting a single boolean on top of the stack.";

    Object top = stack.pop();

    if (top instanceof List) {
      // List version: check each element, one after another, exiting when triggerValue() is found. In case of early
      // exiting, triggerValue() is putted on top of the stack.
      for (Object operand : (List) top) {
        // If a macro is found, execute it and use the result as the operand.
        if (WarpScriptLib.isMacro(operand)) {
          stack.exec((WarpScriptStack.Macro) operand);
          operand = stack.pop();
        }

        if (operand instanceof Boolean) {
          // Short-circuit evaluation: found an early exit case.
          if (triggerValue == (boolean) operand) {
            stack.push(triggerValue);
            return stack;
          }
        } else {
          throw new WarpScriptException(exceptionMessage);
        }
      }

      // No short-circuit found
      stack.push(!triggerValue);
      return stack;
    } else {
      // two operands
      Object op2 = top;
      Object op1 = stack.pop();
      if (op2 instanceof Boolean && op1 instanceof Boolean) {
        // Simple case: both operands are booleans, do a || and push to the stack.
        stack.push(operator((boolean) op1, (boolean) op2));
        return stack;
      } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
        // logical operator between boolean GTS values
        GeoTimeSerie gts1 = (GeoTimeSerie) op1;
        GeoTimeSerie gts2 = (GeoTimeSerie) op2;
        if (GeoTimeSerie.TYPE.BOOLEAN == gts1.getType() && GeoTimeSerie.TYPE.BOOLEAN == gts2.getType()) {
          GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
          result.setType(GeoTimeSerie.TYPE.BOOLEAN);
          GTSOpsHelper.GTSBinaryOp op = new GTSOpsHelper.GTSBinaryOp() {
            @Override
            public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
              return operator(((Boolean) GTSHelper.valueAtIndex(gtsa, idxa)), ((Boolean) GTSHelper.valueAtIndex(gtsb, idxb)));
            }
          };
          GTSOpsHelper.applyBinaryOp(result, gts1, gts2, op);
          stack.push(result);
          return stack;
        } else {
          throw new WarpScriptException(getName() + " can only operate on long values or long GTS.");
        }
      } else if (op1 instanceof GeoTimeSerie || op2 instanceof GeoTimeSerie) {
        // logical operator between a boolean GTS value and a constant (might be usefull to set values to true or false)
        GeoTimeSerie gts;
        boolean mask;
        if (op1 instanceof GeoTimeSerie && op2 instanceof Boolean) {
          gts = (GeoTimeSerie) op1;
          mask = (boolean) op2;
        } else if (op2 instanceof GeoTimeSerie && op1 instanceof Boolean) {
          gts = (GeoTimeSerie) op2;
          mask = (boolean) op1;
        } else {
          throw new WarpScriptException(getName() + " can only operate two GTS or one GTS and a long value.");
        }
        GeoTimeSerie result = gts.cloneEmpty();
        result.setType(GeoTimeSerie.TYPE.BOOLEAN);
        for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
          GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
                  operator(mask, (boolean) GTSHelper.valueAtIndex(gts, i)), false);
        }
        stack.push(result);
        return stack;
      } else {
        throw new WarpScriptException(exceptionMessage);
      }
    }
  }
}
