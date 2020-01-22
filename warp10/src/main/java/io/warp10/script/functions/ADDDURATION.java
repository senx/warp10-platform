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

import io.warp10.continuum.gts.UnsafeString;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutablePeriod;
import org.joda.time.ReadWritablePeriod;
import org.joda.time.format.ISOPeriodFormat;

import java.util.List;
import java.util.Locale;

public class ADDDURATION extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ADDDURATION(String name) {
    super(name);
  }
  final private static WarpScriptStackFunction TSELEMENTS = new TSELEMENTS(WarpScriptLib.TSELEMENTS);
  final private static WarpScriptStackFunction FROMTSELEMENTS = new FROMTSELEMENTS(WarpScriptLib.TSELEMENTSTO);

  @Override
  public WarpScriptStack apply(WarpScriptStack stack) throws WarpScriptException {

    //
    // Retrieve arguments
    //

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects an ISO8601 duration (a string) on top of the stack. See http://en.wikipedia.org/wiki/ISO_8601#Durations.");
    }

    String duration = top.toString();

    String tz = null;

    if (stack.peek() instanceof String) {
      tz = stack.pop().toString();
      if (!(stack.peek() instanceof Long)) {
        throw new WarpScriptException(getName() + " operates on a tselements list, timestamp, or timestamp and timezone.");
      }
    } else if (!(stack.peek() instanceof List) && !(stack.peek() instanceof Long)) {
      throw new WarpScriptException(getName() + " operates on a tselements list, timestamp, or timestamp and timezone.");
    }

    //
    // Handle duration
    //

    // Separate seconds from  digits below second precision
    String[] tokens = UnsafeString.split(duration, '.');

    long offset = 0;
    if (tokens.length > 2) {
      throw new WarpScriptException(getName() + "received an invalid ISO8601 duration.");
    }

    if (2 == tokens.length) {
      duration = tokens[0].concat("S");
      String tmp = tokens[1].substring(0, tokens[1].length() - 1);
      Double d_offset = Double.valueOf(tmp) * new Double(Constants.TIME_UNITS_PER_S);
      offset = d_offset.longValue();
    }

    ReadWritablePeriod period = new MutablePeriod();
    ISOPeriodFormat.standard().getParser().parseInto(period, duration, 0, Locale.US);

    //
    // Handle time zone
    //

    if (null == tz) {
      tz = "UTC";
    }
    DateTimeZone dtz = DateTimeZone.forID(tz);

    //
    // Do the computation
    //

    boolean tselements = false;
    if (stack.peek() instanceof List) {
      FROMTSELEMENTS.apply(stack);
      tselements = true;
    }

    long instant = ((Number) stack.pop()).longValue();
    DateTime dt = new DateTime(instant / Constants.TIME_UNITS_PER_MS, dtz);

    dt.plus(period);
    long ts = dt.getMillis() * Constants.TIME_UNITS_PER_MS;
    ts += (instant % Constants.TIME_UNITS_PER_MS);
    ts += offset;

    stack.push(ts);
    if (tselements) {
      TSELEMENTS.apply(stack);
    }

    return stack;
  }
}
