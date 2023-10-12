//
//   Copyright 2023  SenX S.A.S.
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
import java.math.BigInteger;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class BDTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public BDTO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    boolean tobytes = false;

    if (o instanceof Boolean) {
      tobytes = Boolean.TRUE.equals(o);
      o = stack.pop();
    }

    if (!(o instanceof BigDecimal)) {
      throw new WarpScriptException(getName() + " operates on a BIGDECIMAL.");
    }

    if (tobytes) {
      try {
        BigInteger bi = ((BigDecimal) o).toBigIntegerExact();
        stack.push(bi.toByteArray());
      } catch (ArithmeticException ae) {
        throw new WarpScriptException(getName() + " cannot convert a BIGDECIMAL to BYTES if its scale is not 0.");
      }
    } else {
      stack.push(((BigDecimal) o).toPlainString());
    }

    return stack;
  }
}
