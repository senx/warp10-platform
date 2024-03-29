//
//   Copyright 2020-2023  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSOpsHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ListRecursiveStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.math.BigDecimal;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * Apply a double or long unary operator to one value, a list of values, a nested lists of values, a numerical GTS or
 * nested lists of numerical GTSs.
 * If only the long operator is defined, all numbers are converted to long.
 * If only the double operator is defined, all numbers are converted to double.
 * If both long and double operator are defined, all numbers are converted to long until a Double or BigDecimal is found,
 * then they are all converted to double.
 */
public class NumericalUnaryFunction extends ListRecursiveStackFunction {

  final ListRecursiveStackFunction.ElementStackFunction func;
  final GTSOpsHelper.GTSUnaryOp GTSopL;
  final GTSOpsHelper.GTSUnaryOp GTSopD;
  final GTSOpsHelper.GTSUnaryOp GTSopDL;

  public NumericalUnaryFunction(String name, LongUnaryOperator opL, DoubleUnaryOperator opD) {
    this(name, opL, opD, null);
  }

  public NumericalUnaryFunction(String name, LongUnaryOperator opL, DoubleUnaryOperator opD, DoubleToLongFunction opDL) {
    super(name);

    if (null != opD && null != opDL) {
      throw new RuntimeException("Incoherent instantiation parameters for numerical function: " + name);
    }

    GTSopL = null == opL || LongUnaryOperator.identity() == opL ? null : new GTSOpsHelper.GTSUnaryOp() {
      @Override
      public Object op(GeoTimeSerie gts, int idx) {
        return opL.applyAsLong(((Number) GTSHelper.valueAtIndex(gts, idx)).longValue());
      }
    };
    GTSopD = null == opD || DoubleUnaryOperator.identity() == opD ? null : new GTSOpsHelper.GTSUnaryOp() {
      @Override
      public Object op(GeoTimeSerie gts, int idx) {
        return opD.applyAsDouble(((Number) GTSHelper.valueAtIndex(gts, idx)).doubleValue());
      }
    };
    GTSopDL = null == opDL ? null : new GTSOpsHelper.GTSUnaryOp() {
      @Override
      public Object op(GeoTimeSerie gts, int idx) {
        return opDL.applyAsLong(((Number) GTSHelper.valueAtIndex(gts, idx)).doubleValue());
      }
    };

    func = new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof Number) {
          if (null != opD && (null == opL || element instanceof Double || element instanceof BigDecimal)) {
            return opD.applyAsDouble(((Number) element).doubleValue());
          } else if (null != opDL && (null == opL || element instanceof Double || element instanceof BigDecimal)) {
            return opDL.applyAsLong(((Number) element).doubleValue());
          } else {
            return opL.applyAsLong(((Number) element).longValue());
          }
        } else if (element instanceof GeoTimeSerie) {
          GeoTimeSerie gts = (GeoTimeSerie) element;

          GeoTimeSerie.TYPE type = gts.getType();

          // Only numerical and empty GTSs are allowed.
          if (GeoTimeSerie.TYPE.LONG != type && GeoTimeSerie.TYPE.DOUBLE != type && GeoTimeSerie.TYPE.UNDEFINED != type) {
            throw new WarpScriptException(getName() + " can only operate on LONG, DOUBLE or empty GTSs.");
          }

          GeoTimeSerie result = null;

          // Choose the operator depending on which ones are defined and the GTS type.
          if (null != opD && (null == opL || gts.getType() == GeoTimeSerie.TYPE.DOUBLE)) {
            // Consider all values as doubles because only the double operator is defined or the GTS is of type DOUBLE.
            if (DoubleUnaryOperator.identity() == opD) { // only case where GTSopD is null and opD is not
              result = gts.clone();
            } else {
              // Apply the operator on all the values of gts, storing the result in result.
              result = gts.cloneEmpty(gts.size());
              GTSOpsHelper.applyUnaryOp(result, gts, GTSopD);
            }
          } else if (null != opDL && (null == opL || gts.getType() == GeoTimeSerie.TYPE.DOUBLE)) {
            // Consider all values as doubles because only the double operator is defined or the GTS is of type DOUBLE.
            // Apply the operator on all the values of gts, storing the result in result.
            result = gts.cloneEmpty(gts.size());
            GTSOpsHelper.applyUnaryOp(result, gts, GTSopDL);
          } else {
            // Consider all values as longs because either the double operator is not defined or the GTS is of LONG type.
            if (LongUnaryOperator.identity() == opL) { // only case where GTSopL is null and opL is not
              result = gts.clone();
            } else {
              // Apply the operator on all the values of gts, storing the result in result.
              result = gts.cloneEmpty(gts.size());
              GTSOpsHelper.applyUnaryOp(result, gts, GTSopL);
            }
          }

          return result;
        } else {
          return UNHANDLED;
        }
      }
    };
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    return func;
  }

  @Override
  public String getUnhandledErrorMessage() {
    return getName() + " can only operate on numerical values, list of numerical values or numerical GTSs.";
  }
}
