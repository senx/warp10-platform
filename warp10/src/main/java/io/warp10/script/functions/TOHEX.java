//
//   Copyright 2018-2024  SenX S.A.S.
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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import org.bouncycastle.util.encoders.Hex;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Encode a String, byte[] or Long in hexadecimal
 */
public class TOHEX extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOHEX(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (o instanceof String) {
      stack.push(new String(Hex.encode(o.toString().getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));
    } else if (o instanceof byte[]) {
      stack.push(new String(Hex.encode((byte[]) o), StandardCharsets.UTF_8));
    } else if (o instanceof Long) {
      StringBuilder sb = new StringBuilder("0000000000000000");
      sb.append(Long.toHexString(((Number) o).longValue()));
      stack.push(sb.substring(sb.length() - 16));
    } else if (o instanceof BigDecimal) {
      try {
        BigInteger bi = ((BigDecimal) o).toBigIntegerExact();

        if (bi.signum() < 0) {
          stack.push("-0x" + bi.negate().toString(16));
        } else {
          stack.push("0x" + bi.toString(16));
        }
      } catch (ArithmeticException ae) {
        throw new WarpScriptException(getName() + " can only operate on a BIGDECIMAL with no fractional part.");
      }
    } else {
      throw new WarpScriptException(getName() + " operates on a STRING, BYTES or LONG.");
    }

    return stack;
  }
}
