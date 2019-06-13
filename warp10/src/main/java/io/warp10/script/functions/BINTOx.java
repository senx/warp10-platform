//
//   Copyright 2018  SenX S.A.S.
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

/**
 * Decode a String in binary
 */
public abstract class BINTOx extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public BINTOx(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a String.");
    }

    String bin = (String) o;

    // Compute the number of missing bits to have a string length multiple of 8
    int missingBitsLength = (8 - bin.length() % 8) % 8;

    // Initialize the data structure
    Object data = initData(missingBitsLength + bin.length());

    int curByte = 0;
    char bit;
    // For all the characters in the given string
    for (int i = 0; i < bin.length(); i++) {
      curByte = curByte << 1;

      bit = bin.charAt(i);

      if ('1' == bit) {
        curByte = curByte | 1;
      } else if ('0' != bit) {
        throw new WarpScriptException(getName() + " expects a valid binary representation. \"" + bin + "\" is invalid.");
      }

      // When at the end of a byte, update the data structure and reset the current byte value
      if (7 == ((i + missingBitsLength) % 8)) {
        updateData(data, (i + missingBitsLength) / 8, (byte) curByte);
        curByte = 0;
      }
    }

    stack.push(generateResult(data));

    return stack;
  }

  public abstract Object initData(int numberOfBits);
  public abstract void updateData(Object data, int byteIndex, byte currentByte);
  public abstract Object generateResult(Object data);
}
