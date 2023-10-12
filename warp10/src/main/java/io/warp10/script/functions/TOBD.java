//
//   Copyright 2023  SenX S.A.S.
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

import com.google.common.io.BaseEncoding;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Encode a String in base64
 */
public class TOBD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOBD(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    BigDecimal bd = toBigDecimal(getName(), o);

    stack.push(bd);

    return stack;
  }

  public static BigDecimal toBigDecimal(String name, Object o) throws WarpScriptException {

    BigDecimal bd = null;

    if (o instanceof String) {
      bd = new BigDecimal((String) o);
    } else if (o instanceof Long) {
      bd = BigDecimal.valueOf(((Long) o).longValue());
    } else if (o instanceof Double) {
      bd = BigDecimal.valueOf(((Double) o).doubleValue());
    } else if (o instanceof byte[]) {
      BigInteger bi = new BigInteger((byte[]) o);
      bd = new BigDecimal(bi);
    } else {
      throw new WarpScriptException(name + " can only be applied to a STRING, BYTES, LONG or DOUBLE argument.");
    }

    return bd;
  }
}
