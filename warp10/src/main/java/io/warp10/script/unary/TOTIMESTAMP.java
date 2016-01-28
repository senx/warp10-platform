//
//   Copyright 2016  Cityzen Data
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

package io.warp10.script.unary;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Convert Date in format ISO8601 into a Timestamp in µs
 *
 * TOTIMESTAMP expects a date in ISO8601 on the top of the stack
 */
public class TOTIMESTAMP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  private static final String DATE = "date";

  public TOTIMESTAMP(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();


    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects an ISO8601 timestamp on top of the stack.");
    }
    else {
      long timestamp = fmt.parseDateTime((String) top).getMillis() * Constants.TIME_UNITS_PER_MS;
      stack.push(timestamp);
    }

    return stack;
  }
}