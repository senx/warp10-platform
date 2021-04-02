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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import org.bouncycastle.crypto.digests.SHA256Digest;

import com.google.common.primitives.Bytes;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Encode a String into Base58 or Base58Check
 * See https://tools.ietf.org/html/draft-msporny-base58-03
 */
public class TOB58 extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

  public static final BigInteger FIFTY_EIGHT = new BigInteger("58");

  private final boolean check;

  public TOB58(String name, boolean check) {
    super(name);
    this.check = check;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    byte[] payload = null;
    byte[] prefix = null;

    if (check) {
      if (!(top instanceof byte[])) {
        throw new WarpScriptException(getName() + " expects a byte array prefix.");
      }
      prefix = (byte[]) top;
      top = stack.pop();
    }

    if (top instanceof byte[]) {
      payload = (byte[]) top;
    } else if (top instanceof String) {
      payload = ((String) top).getBytes(StandardCharsets.UTF_8);
    } else {
      throw new WarpScriptException(getName() + " operates on STRING or a byte array.");
    }

    if (check) {
      SHA256Digest digest = new SHA256Digest();
      digest.update(prefix, 0, prefix.length);
      digest.update(payload, 0, payload.length);
      byte[] hash = new byte[digest.getDigestSize()];
      digest.doFinal(hash, 0);
      digest.reset();
      digest.update(hash, 0, hash.length);
      digest.doFinal(hash, 0);
      byte[] data = new byte[prefix.length + payload.length + 4];
      System.arraycopy(prefix,  0,  data,  0,  prefix.length);
      System.arraycopy(payload, 0, data, prefix.length, payload.length);
      System.arraycopy(hash, 0, data,  prefix.length + payload.length, 4);
      payload = data;
    }

    stack.push(encode(payload));
    return stack;
  }

  public static String encode(byte[] data) {
    //
    // See https://tools.ietf.org/id/draft-msporny-base58-01.html
    //

    int zero_counter = 0;

    StringBuilder b58_encoding = new StringBuilder();

    while(zero_counter < data.length && 0x00 == data[zero_counter]) {
      b58_encoding.append("1");
      zero_counter++;
    }

    // Add a leading 0x00 byte if the first byte is above 128 as otherwise
    // the number would be considered a negative one
    if (0x00 != (data[0] & 0x80)) {
      data = Bytes.concat(new byte[] { 0x00 }, data);
    }

    BigInteger n = new BigInteger(data);

    while(!BigInteger.ZERO.equals(n)) {
      int r = n.mod(FIFTY_EIGHT).intValue();
      n = n.divide(FIFTY_EIGHT);
      b58_encoding.insert(zero_counter, ALPHABET.charAt(r));
    }

    return b58_encoding.toString();
  }
}
