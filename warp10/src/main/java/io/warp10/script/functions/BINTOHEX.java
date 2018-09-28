//
//   Copyright 2016  Cityzen Data
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

import io.warp10.continuum.gts.UnsafeString;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Decode a String in binary and immediately encode it as hexadecimal
 */
public class BINTOHEX extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public BINTOHEX(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a String.");
    }

    String bin = o.toString();

    if (bin.length() % 8 != 0) {
      bin = "0000000".substring(7 - (bin.length() % 8)) + bin;
    }

    char[] chars = UnsafeString.getChars(bin);

    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < bin.length(); i += 4) {
      String nibble = new String(chars, i, 4);

      if ("0000".equals(nibble)) {
        sb.append("0");
      } else if ("0001".equals(nibble)) {
        sb.append("1");
      } else if ("0010".equals(nibble)) {
        sb.append("2");
      } else if ("0011".equals(nibble)) {
        sb.append("3");
      } else if ("0100".equals(nibble)) {
        sb.append("4");
      } else if ("0101".equals(nibble)) {
        sb.append("5");
      } else if ("0110".equals(nibble)) {
        sb.append("6");
      } else if ("0111".equals(nibble)) {
        sb.append("7");
      } else if ("1000".equals(nibble)) {
        sb.append("8");
      } else if ("1001".equals(nibble)) {
        sb.append("9");
      } else if ("1010".equals(nibble)) {
        sb.append("A");
      } else if ("1011".equals(nibble)) {
        sb.append("B");
      } else if ("1100".equals(nibble)) {
        sb.append("C");
      } else if ("1101".equals(nibble)) {
        sb.append("D");
      } else if ("1110".equals(nibble)) {
        sb.append("E");
      } else if ("1111".equals(nibble)) {
        sb.append("F");
      }
    }

    stack.push(sb.toString());

    return stack;
  }
}
