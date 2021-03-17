//
//   Copyright 2021  SenX S.A.S.
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

import java.util.List;

import com.geoxp.oss.CryptoHelper;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TOSSSS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOSSSS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    // Flag indicating whether or not to ensure randomness by
    // keeping points with 0 as their X coordinate

    boolean ensureRandomness = true;

    if (top instanceof Boolean) {
      ensureRandomness = Boolean.TRUE.equals(top);
      top = stack.pop();
    }

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects the number of splits needed to reconstruct the input.");
    }

    int k = Math.toIntExact(((Long) top).longValue());

    if (k < 2 || k > 255) {
      throw new WarpScriptException(getName() + " expects the number of needed splits to be between 2 and 255.");
    }

    top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects the number of splits to generate.");
    }

    int n = Math.toIntExact(((Long) top).longValue());

    if (n > 255) {
      throw new WarpScriptException(getName() + " the number of splits must be strictly less than 256.");
    }

    if (n < k) {
      throw new WarpScriptException(getName() + " the number of splits to generate must be at least equal to the number of needed splits.");
    }

    top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }

    byte[] bytes = (byte[]) top;

    List<byte[]> splits = CryptoHelper.SSSSSplit(bytes, n, k);

    // If we do not want to ensure randomness, iterate over the splits and
    // remove any point with a 0 X coordinate

    if (!ensureRandomness) {
      for (int i = 0; i < splits.size(); i++) {
        if (splits.get(i).length > bytes.length * 2) {
          byte[] split = splits.get(i);
          byte[] newsplit = new byte[bytes.length * 2];
          int idx = 0;
          for (int j = 0; j < split.length / 2; j++) {
            if ((byte) 0x00 == split[j * 2]) {
              continue;
            }
            newsplit[idx++] = split[j * 2];
            newsplit[idx++] = split[j * 2 + 1];
          }
          splits.set(i, newsplit);
        }
      }
    }

    stack.push(splits);

    return stack;
  }
}
