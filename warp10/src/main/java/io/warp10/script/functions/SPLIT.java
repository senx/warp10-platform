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

import java.util.ArrayList;
import java.util.List;

/**
 * Split a String in segments given a delimiter.
 * <p>
 * The limit parameter controls the number of times the pattern is applied and therefore affects the length of the
 * resulting array. If the limit n is greater than zero then the pattern will be applied at most n - 1 times, the
 * array's length will be no greater than n, and the array's last entry will contain all input beyond the last matched
 * delimiter.
 */
public class SPLIT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public SPLIT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    int limit = Integer.MAX_VALUE;

    if (o instanceof Long) {
      limit = Math.toIntExact((Long) o);

      if (limit <= 0) {
        throw new WarpScriptException(getName() + " expects the limit to be a strictly positive integer.");
      }

      o = stack.pop();
    }

    if (!(o instanceof String) || 1 != ((String) o).length()) {
      throw new WarpScriptException(getName() + " expects a string delimiter of length 1.");
    }

    char delimiter = ((String) o).charAt(0);

    o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a String.");
    }

    stack.push(split((String) o, delimiter, limit));

    return stack;
  }

  /**
   * Split a string using a delimiter and returning a List of "limit" maximum length.
   * @param input The String instance to be split.
   * @param delim The delimiter to use for the split.
   * @param limit The returned List maximum size. For limit <= 0 the List contains the input.
   * @return A List of splits.
   */
  public static List<String> split(String input, char delim, int limit) {
    ArrayList<String> l = new ArrayList<String>();
    int offset = 0;
    int splits = 1;

    while (splits < limit) {
      int index = input.indexOf(delim, offset);
      if (-1 == index) {
        l.add(input.substring(offset));
        break;
      } else {
        l.add(input.substring(offset, index));

        offset = index + 1;
        splits++;
      }
    }

    if (splits >= limit) {
      l.add(input.substring(offset));
    }

    return l;
  }
}
