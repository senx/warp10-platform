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

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Replaces the timestamp on the stack with a string representation of its instant.
 */
public class ISO8601 extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final DateTimeFormatter dtf = ISODateTimeFormat.dateTime();
  
  public ISO8601(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object obj = stack.peek();
    
    String tz = null;
    
    if (obj instanceof String) {
      tz = (String) obj;
      stack.pop();
    } else if (!(obj instanceof Long)) {
      throw new WarpScriptException(getName() + " operates on a timestamp or a timestamp + timezone.");
    }
    
    obj = stack.pop();
        
    if (!(obj instanceof Long)) {
      throw new WarpScriptException(getName() + " operates on a timestamp or a timestamp + timezone.");
    }
    
    long ts = (long) obj;
    
    
    DateTimeFormatter dtf;
    
    //
    // Set the timezone
    //
    
    if (null == tz) {
      dtf = this.dtf.withZoneUTC();
    } else {
      dtf = this.dtf.withZone(DateTimeZone.forID(tz));
    }
    
    long millis = ts / Constants.TIME_UNITS_PER_MS;
    
    String dt = dtf.print(millis);
    
    if (Constants.TIME_UNITS_PER_MS > 1) {
      //
      // Add sub millisecond string
      //
      
      StringBuilder sb = new StringBuilder();
      
      sb.append(dt, 0, 23);
      
      long subms = Constants.TIME_UNITS_PER_MS;
      
      subms += ts % Constants.TIME_UNITS_PER_MS;
      
      String str = Long.toString(subms);
      
      sb.append(str, 1, str.length());
      
      sb.append(dt, 23, dt.length());
      
      stack.push(sb.toString());
    } else {
      stack.push(dt);      
    }
    
    return stack;
  }
}
