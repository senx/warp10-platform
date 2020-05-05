//
//   Copyright 2020  SenX S.A.S.
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
 * Converts a geocell long to a String HHCode prefix whose resolution is implicitly given by its length.
 * It cannot convert to a byte[] representation because they are limited to resolutions multiple of 4.
 */
public class GEOCELLTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public GEOCELLTO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a " + TYPEOF.TYPE_LONG + " geocell.");
    }

    long geocell = ((Long) top).longValue();

    stack.push(geocellToHHCodePrefix(geocell));

    return stack;
  }

  /**
   * Converts a geocell (4 bits resolution + 60 bits hhcode) to an HHCode prefix String whose resolution is implicitly
   * given by the length of the String.
   *
   * @param geocell The geocell to convert.
   * @return An HHCode prefix String whose resolution is implicitly given by the length of the String. Thus its length is
   * between 1 and 15, inclusive.
   */
  public static String geocellToHHCodePrefix(long geocell) {
    return Long.toHexString(geocell).substring(1, (int) (geocell >>> 60) + 1);
  }
}
