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

package io.warp10.script.ext.r;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.renjin.sexp.DoubleVector;
import org.renjin.sexp.ExternalPtr;
import org.renjin.sexp.IntVector;
import org.renjin.sexp.ListVector;
import org.renjin.sexp.Logical;
import org.renjin.sexp.LogicalVector;
import org.renjin.sexp.SEXP;
import org.renjin.sexp.StringVector;

/**
 * Convert R types back to Java types known to Einstein
 */
public class RTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public RTO(String name) {
    super(name);
  }
   
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    stack.push(fromR(o));
    
    return stack;
  }

  public static final Object fromR(Object o) throws WarpScriptException {
    if (o instanceof ExternalPtr) {
      return ((ExternalPtr) o).getInstance();
    } else if (o instanceof DoubleVector) {
      DoubleVector vector = (DoubleVector) o;
      List<Double> list = new ArrayList<Double>();
      Iterator<Double> iter = vector.iterator();
      while(iter.hasNext()) {
        list.add(iter.next());
      }
      return list;
    } else if (o instanceof IntVector) {
      IntVector vector = (IntVector) o;
      List<Long> list = new ArrayList<Long>();
      Iterator<Integer> iter = vector.iterator();
      while(iter.hasNext()) {
        list.add(iter.next().longValue());
      }
      return list;
    } else if (o instanceof StringVector) {
      StringVector vector = (StringVector) o;
      List<String> list = new ArrayList<String>();
      Iterator<String> iter = vector.iterator();
      while(iter.hasNext()) {
        list.add(iter.next().intern());
      }
      return list;
    } else if (o instanceof LogicalVector) {
      LogicalVector vector = (LogicalVector) o;
      List<Boolean> list = new ArrayList<Boolean>();
      Iterator<Logical> iter = vector.iterator();
      while(iter.hasNext()) {
        Logical logical = iter.next();
        if (Logical.TRUE.equals(logical)) {
          list.add(Boolean.TRUE);
        } else if (Logical.FALSE.equals(logical)) {
          list.add(Boolean.FALSE);
        } else {
          list.add(null);
        }
      }
      return list;
    } else if (o instanceof ListVector) {
      ListVector vector = (ListVector) o;
      Iterator<SEXP> iter = vector.iterator();
      List<Object> list = new ArrayList<Object>();
      while(iter.hasNext()) {
        SEXP sexp = iter.next();
        list.add(fromR(sexp));
      }
      return list;
    } else {
      throw new WarpScriptException("Invalid type " + o.getClass().getCanonicalName());
    }
  }
}
