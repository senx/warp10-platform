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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class RLPTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public RLPTO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }

    try {
      stack.push(decode((byte[]) top));
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " encountered an error while decoding RLP.", e);
    }

    return stack;
  }

  public static Object decode(byte[] data) throws Exception {
    return decode(data, new AtomicInteger(0), data.length);
  }

  public static Object decode(byte[] data, AtomicInteger offset, int len) throws Exception {

    int idx = offset.get();

    if ((int) (data[idx] & 0xFF) > 0xf7) { // List with explicit size
      int sizebytes = (data[idx] & 0xFF) - 0xf7;
      idx++;
      long size = 0;
      if (sizebytes > 4) {
        throw new WarpScriptException("Unable to support size over 2**32.");
      }
      offset.addAndGet(1);
      while(sizebytes > 0) {
        size <<= 8;
        size |= data[idx++] & 0xFF;
        sizebytes--;
        offset.addAndGet(1);
      }

      int end = offset.get() + (int) size;
      List<Object> list = new ArrayList<Object>();

      while(offset.get() < end) {
        list.add(decode(data, offset, end - offset.get()));
      }

      return list;
    } else if ((int) (data[idx] & 0xFF) >= 0xc0) { // List with size 0-55
      int size = (data[idx] & 0xFF) - 0xc0;
      List<Object> list = new ArrayList<Object>();

      offset.addAndGet(1);
      int end = offset.get() + size;

      while(offset.get() < end) {
        list.add(decode(data, offset, end - offset.get()));
      }

      return list;
    } else if ((int) (data[idx] & 0xFF) > 0xb7) { // String with explicit size
      int sizebytes = (data[idx] & 0xFF) - 0xb7;
      idx++;
      long size = 0;
      if (sizebytes > 4) {
        throw new WarpScriptException("Unable to support size over 2**32.");
      }
      offset.addAndGet(1);
      while(sizebytes > 0) {
        size <<= 8;
        size |= data[idx++] & 0xFF;
        sizebytes--;
        offset.addAndGet(1);
      }
      offset.addAndGet((int) size);
      return Arrays.copyOfRange(data, idx, idx + (int) size);
    } else if ((int) (data[idx] & 0xFF) >= 0x80) { // String with size 0-55
      int size = (data[idx] & 0xFF) - 0x80;
      offset.addAndGet(size + 1);
      idx++;
      return Arrays.copyOfRange(data, idx, idx + size);
    } else { // Single byte
      offset.addAndGet(1);
      return Arrays.copyOfRange(data, idx, idx + 1);
    }
  }
}
