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

package io.warp10.script.functions;

import java.util.ArrayList;
import java.util.List;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Adds a year to a timestamp
 */
public class ADDYEARS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ADDYEARS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
  
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of years on top of the stack.");
    }
    
    int years = ((Number) top).intValue();
    
    top = stack.pop();
    
    String tz = null;
    
    if (top instanceof String) {
      tz = top.toString();
      top = stack.pop();
      if (!(top instanceof Long)) {
        throw new WarpScriptException(getName() + " operates on a tselements list, timestamp, or timestamp and timezone.");
      }
    } else if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a tselements list, timestamp, or timestamp and timezone.");
    }
    
    if (top instanceof Long) {
      long instant = ((Number) top).longValue();
      
      if (null == tz) {
        tz = "UTC";
      }
      
      DateTimeZone dtz = DateTimeZone.forID(null == tz ? "UTC" : tz);
      
      DateTime dt = new DateTime(instant / Constants.TIME_UNITS_PER_MS, dtz);
      
      dt = dt.plusYears(years);
      
      long ts = dt.getMillis() * Constants.TIME_UNITS_PER_MS + (instant % Constants.TIME_UNITS_PER_MS);
      
      stack.push(ts);      
    } else {
      List<Object> elts = new ArrayList<Object>((List<Object>) top);
      
      int year = ((Number) elts.get(0)).intValue();
      year += years;
      
      elts.set(0, (long) year);
      
      // Now check if we are in ferbuary and if this is coherent with
      // a possibly non leap year
      
      if (elts.size() > 2) {
        int month = ((Number) elts.get(1)).intValue();
        int day = ((Number) elts.get(2)).intValue();
        
        if (2 == month && day > 28) {
          if ((0 != year % 4) || (0 == year % 100)) {
            elts.set(2, (long) 28);
          }
        }
      }
     
      stack.push(elts);
    }
        
    return stack;
  }
}
