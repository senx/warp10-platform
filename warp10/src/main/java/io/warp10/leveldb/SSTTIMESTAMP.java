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

package io.warp10.leveldb;

import org.bouncycastle.util.encoders.Hex;

import io.warp10.BytesUtils;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

/**
 * Extract the timestamp from an SST key
 */
public class SSTTIMESTAMP extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public SSTTIMESTAMP(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB) && null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_TIMESTAMP)) {
      throw new WarpScriptException(getName() + " missing capability '" + WarpScriptStack.CAPABILITY_LEVELDB + "' or '" + WarpScriptStack.CAPABILITY_LEVELDB_TIMESTAMP + "'.");
    }

    Object top = stack.pop();

    if (!(top instanceof byte[]) && !(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a byte array or hex string.");
    }

    byte[] key = top instanceof byte[] ? (byte[]) top : Hex.decode(top.toString());

    // 128BITS
    if (key.length < Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8) {
      throw new WarpScriptException(getName() + " encountered an invalid key length.");
    }

    if (0 != BytesUtils.compareTo(key, 0, Constants.FDB_RAW_DATA_KEY_PREFIX.length, Constants.FDB_RAW_DATA_KEY_PREFIX, 0, Constants.FDB_RAW_DATA_KEY_PREFIX.length)) {
      throw new WarpScriptException(getName() + " invalid key prefix.");
    }

    // Extract the reversed timestamp
    long rts = 0L;

    for (int i = 8; i > 0; i--){
      rts <<= 8;
      rts |= ((long) ((byte) key[key.length - i]) & 0xFFL);
    }

    long ts = Long.MAX_VALUE - rts;

    stack.push(ts);

    return stack;
  }
}
