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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Converts a string into its bytes given a charset
 */
public class TOBYTES extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOBYTES(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (o instanceof String) {
      String charset = o.toString();

      o = stack.pop();

      if (!(o instanceof String)) {
        throw new WarpScriptException(getName() + " operates on a String.");
      }
      String str = o.toString();

      try {
        byte[] bytes = str.getBytes(charset);
        stack.push(bytes);
      } catch (UnsupportedEncodingException uee) {
        throw new WarpScriptException(getName() + " unsupported encoding '" + charset + "'", uee);
      }
    } else if (o instanceof Long) {
      Long nbBytes = (Long) o;
      o = stack.pop();
      if (o instanceof Long && nbBytes > 0 && nbBytes <= 8) {
        ByteBuffer b = ByteBuffer.allocate(8);
        b.putLong((Long) o);
        //truncate the result to nbBytes
        stack.push(Arrays.copyOfRange(b.array(), 8 - nbBytes.intValue(), 8));
      } else if (o instanceof Double && 4 == nbBytes) {
        //IEEE 754 simple precision conversion
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putFloat(((Double) o).floatValue());
        stack.push(b.array());
      } else if (o instanceof Double && 8 == nbBytes) {
        //IEEE 754 double precision conversion
        ByteBuffer b = ByteBuffer.allocate(8);
        b.putDouble((Double) o);
        stack.push(b.array());
      } else {
        throw new WarpScriptException(getName() + " could convert Long to an array of 1 to 8 bytes, or Double to an array of 4 or 8 bytes.");
      }
    } else {
      throw new WarpScriptException(getName() + " operates on a String and expects a charset name on top of the stack, or operates on a number and expects a number of bytes on top of the stack.");
    }

    return stack;
  }
}
