//
//   Copyright 2018-2023  SenX S.A.S.
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

/**
 * Decode a String in hexadecimal and immediately encode it as binary
 */
public class HEXTOBIN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public HEXTOBIN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a String.");
    }

    StringBuilder sb = new StringBuilder();

    String s = o.toString();

    for (int i = 0; i < s.length(); i++) {
      switch(s.charAt(i)) {
        case '0':
          sb.append("0000");
          break;
        case '1':
          sb.append("0001");
          break;
        case '2':
          sb.append("0010");
          break;
        case '3':
          sb.append("0011");
          break;
        case '4':
          sb.append("0100");
          break;
        case '5':
          sb.append("0101");
          break;
        case '6':
          sb.append("0110");
          break;
        case '7':
          sb.append("0111");
          break;
        case '8':
          sb.append("1000");
          break;
        case '9':
          sb.append("1001");
          break;
        case 'a':
        case 'A':
          sb.append("1010");
          break;
        case 'b':
        case 'B':
          sb.append("1011");
          break;
        case 'c':
        case 'C':
          sb.append("1100");
          break;
        case 'd':
        case 'D':
          sb.append("1101");
          break;
        case 'e':
        case 'E':
          sb.append("1110");
          break;
        case 'f':
        case 'F':
          sb.append("1111");
          break;
        default:
          throw new WarpScriptException(getName() + " encountered an invalid hex character '" + s.charAt(i) + "'.");
      }
    }

    stack.push(sb.toString());

    return stack;
  }
}
