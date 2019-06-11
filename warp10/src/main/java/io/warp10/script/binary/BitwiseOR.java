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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Bitwise OR of the two operands on top of the stack
 */
public class BitwiseOR extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public BitwiseOR(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op2 instanceof Long && op1 instanceof Long) {
      stack.push(((Long) op1).longValue() | ((Long) op2).longValue());
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
      GeoTimeSerie gts1 = (GeoTimeSerie) op1;
      GeoTimeSerie gts2 = (GeoTimeSerie) op2;
      if (GeoTimeSerie.TYPE.BOOLEAN == gts1.getType() && GeoTimeSerie.TYPE.BOOLEAN == gts2.getType()) {
        GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
        result.setType(GeoTimeSerie.TYPE.BOOLEAN);
        GTSOpsHelper.GTSBinaryOp op = new GTSOpsHelper.GTSBinaryOp() {
          @Override
          public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
            return ((Boolean) GTSHelper.valueAtIndex(gtsa, idxa)) || ((Boolean) GTSHelper.valueAtIndex(gtsb, idxb));
          }
        };
        GTSOpsHelper.applyBinaryOp(result, gts1, gts2, op);
        stack.push(result);
      } else {
        throw new WarpScriptException(getName() + " can only operate on long values or boolean GTS.");
      }
    } else {
      throw new WarpScriptException(getName() + " can only operate on long values.");
    }
    
    return stack;
  }
}
