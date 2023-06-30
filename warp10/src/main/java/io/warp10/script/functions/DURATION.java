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

package io.warp10.script.functions;

import io.warp10.continuum.gts.UnsafeString;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutablePeriod;
import org.joda.time.Period;
import org.joda.time.ReadWritablePeriod;
import org.joda.time.format.ISOPeriodFormat;

/**
 * Pushes onto the stack the number of time units described by the ISO8601 duration
 * on top of the stack (String).
 *
 * @see <a href="http://en.wikipedia.org/wiki/ISO_8601#Durations">http://en.wikipedia.org/wiki/ISO_8601#Durations</a>
 */
public class DURATION extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  final private static Double STU = Double.valueOf(Constants.TIME_UNITS_PER_S);

  final private static Pattern NEGATIVE_ZERO_SECONDS_PATTERN = Pattern.compile(".*-0+$");

  public DURATION(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects an ISO8601 duration (a string) on top of the stack. See http://en.wikipedia.org/wiki/ISO_8601#Durations");
    }

    String duration_string = o.toString();

    try {
      long duration = parseDuration(new Instant(), duration_string, false, false);
      stack.push(duration);
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " encountered an error while parsing duration.", wse);
    }

    return stack;
  }

  public static long parseDuration(Instant ref, String duration_string, boolean allowAmbiguous, boolean toRef) throws WarpScriptException {
    // Handle "-PTxxx" which is valid but not handled by Joda.
    long globalSignFactor = 1;
    if (duration_string.startsWith("-")) {
      duration_string = duration_string.substring(1);
      globalSignFactor = -1;
    }

    // Separate seconds from digits below second precision
    String[] tokens = UnsafeString.split(duration_string, '.');

    long subseconds = 0;

    if (tokens.length > 2) {
      throw new WarpScriptException("invalid ISO8601 duration.");
    }

    if (2 == tokens.length) {
      duration_string = tokens[0].concat("S");
      String tmp = tokens[1].substring(0, tokens[1].length() - 1);
      Double d_offset = Double.valueOf("0." + tmp) * STU;
      subseconds = d_offset.longValue();
    }

    ReadWritablePeriod period = new MutablePeriod();

    ISOPeriodFormat.standard().getParser().parseInto(period, duration_string, 0, Locale.US);

    Period p = period.toPeriod();

    if (!allowAmbiguous && (p.getMonths() != 0 || p.getYears() != 0)) {
      throw new WarpScriptException("no support for ambiguous durations containing years or months, please convert those to days.");
    }

    Duration duration = toRef ? p.toDurationTo(ref) : p.toDurationFrom(ref);

    // Find out if subseconds are positive or negative.
    boolean negativeSubseconds = false;
    if (p.getSeconds() < 0) {
      // Seconds are negative, so as subseconds (example: -1.234).
      negativeSubseconds = true;
    } else if (0 == p.getSeconds() && 0 != subseconds) {
      // There aren't whole seconds and subseconds are defined (example: 0.123).
      // Check if there is a minus sign before any number of 0s defining seconds.
      Matcher negativeZeroSecondMatcher = NEGATIVE_ZERO_SECONDS_PATTERN.matcher(tokens[0]);
      negativeSubseconds = negativeZeroSecondMatcher.matches();
    }

    if (negativeSubseconds) {
      subseconds = -subseconds;
    }

    return globalSignFactor * (duration.getMillis() * Constants.TIME_UNITS_PER_MS + subseconds);
  }
}
