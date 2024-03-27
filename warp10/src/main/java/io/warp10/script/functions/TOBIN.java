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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

/**
 * Encode a String, byte[], Long or BitSet in binary
 */
public class TOBIN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String[] NIBBLES = new String[]{
    "0000", "0001", "0010", "0011",
    "0100", "0101", "0110", "0111",
    "1000", "1001", "1010", "1011",
    "1100", "1101", "1110", "1111",
  };

  public TOBIN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    StringBuilder sb = new StringBuilder();

    if (o instanceof String || o instanceof byte[]) {
      byte[] data;

      if (o instanceof String) {
        data = ((String) o).getBytes(StandardCharsets.UTF_8);
      } else {
        data = (byte[]) o;
      }

      for (byte datum: data) {
        int nibble = (datum >>> 4) & 0xF;
        sb.append(NIBBLES[nibble]);
        nibble = (datum & 0xF);
        sb.append(NIBBLES[nibble]);
      }

      stack.push(sb.toString());
    } else if (o instanceof Long) {
      sb.append("0000000000000000000000000000000000000000000000000000000000000000");

      sb.append(Long.toBinaryString(((Number) o).longValue()));
      stack.push(sb.substring(sb.length() - 64));
    } else if (o instanceof BitSet) {
      BitSet bitset = (BitSet) o;

      for (int i = 0; i < bitset.length(); i++) {
        sb.append(bitset.get(i) ? '1' : '0');
      }

      stack.push(sb.toString());
    } else if (o instanceof BigDecimal) {
      try {
        BigInteger bi = ((BigDecimal) o).toBigIntegerExact();

        if (bi.signum() < 0) {
          stack.push("-0b" + bi.negate().toString(2));
        } else {
          stack.push("0b" + bi.toString(2));
        }
      } catch (ArithmeticException ae) {
        throw new WarpScriptException(getName() + " can only operate on a BIGDECIMAL with no fractional part.");
      }

    } else {
      throw new WarpScriptException(getName() + " operates on a STRING, BYTES, LONG, BIGDECIMAL or BITSET.");
    }

    return stack;
  }
}
