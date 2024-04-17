//
//   Copyright 2024  SenX S.A.S.
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

import java.math.BigDecimal;
import java.math.RoundingMode;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class BDROUND extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final BigDecimal MINUS_ZERO_POINT_FIVE = BigDecimal.valueOf(-0.5D);

  public BDROUND(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    BigDecimal bd1 = TOBD.toBigDecimal(getName(), o);

    // If the discarding part is exactly 0.5 and the number is negative, use rounding mode HALF_DOWN
    if (bd1.signum() < 0) {
      BigDecimal nonfractional = new BigDecimal(bd1.toBigInteger());
      if (MINUS_ZERO_POINT_FIVE.equals(bd1.subtract(nonfractional))) {
        stack.push(bd1.setScale(0, RoundingMode.HALF_DOWN));
      } else {
        stack.push(bd1.setScale(0, RoundingMode.HALF_UP));
      }
    } else {
      stack.push(bd1.setScale(0, RoundingMode.HALF_UP));
    }

    return stack;
  }
}
