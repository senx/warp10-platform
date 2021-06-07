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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TORLP extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TORLP(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    try {
      stack.push(encode(top));
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " encountered an error while encoding RLP.", e);
    }
    return stack;
  }

  public static byte[] encode(Object o) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    if (o instanceof List) {
      long size = 0;

      for (Object item: (List) o) {
        byte[] encoded = encode(item);
        size += encoded.length;
        out.write(encoded);
      }

      if (size <= 55) {
        return Bytes.concat(new byte[] { (byte) (0xc0 + size) }, out.toByteArray());
      } else {
        byte[] len = Longs.toByteArray(size);

        int idx = 0;
        while(0x00 == len[idx]) {
          idx++;
        }
        return Bytes.concat(
            Bytes.concat(new byte[] { (byte) (0xf7 + 8 - idx) }, Arrays.copyOfRange(len, idx, 8)),
            out.toByteArray()
            );
      }
    } else if (o instanceof byte[] || o instanceof String) {
      byte[] data;
      if (o instanceof String) {
        data = ((String) o).getBytes(StandardCharsets.UTF_8);
      } else {
        data = (byte[]) o;
      }

      if (1 == data.length && (data[0] & 0xFF) <= 0x7f) {
        return data;
      } else if (data.length <= 55) {
        return Bytes.concat(new byte[] { (byte) (0x80 + data.length) }, data);
      } else {
        byte[] len = Longs.toByteArray(data.length);

        int idx = 0;
        while(0x00 == len[idx]) {
          idx++;
        }

        return Bytes.concat(
            Bytes.concat(new byte[] { (byte) (0xb7 + 8 - idx) }, Arrays.copyOfRange(len, idx, 8)),
            data
            );
      }
    } else if (o instanceof Long) {
      long l = ((Long) o).longValue();

      if (0L == l) {
        return encode(new byte[0]);
      }

      byte[] data = Longs.toByteArray(l);

      if (l > 0) {
        int idx = 0;
        while (0x00 == data[idx]) {
          idx++;
        }
        data = Arrays.copyOfRange(data, idx, data.length);
      }

      return encode(data);
    } else {
      throw new WarpScriptException("Invalid item type, only LIST, LONG, BYTES and STRING are supported.");
    }

  }
}
