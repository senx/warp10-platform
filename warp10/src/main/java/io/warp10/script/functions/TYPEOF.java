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

import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import processing.core.PFont;
import processing.core.PGraphics;
import processing.core.PImage;
import processing.core.PShape;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.BitSet;
import java.util.Vector;

/**
 * Push on the stack the type of the object on top of the stack
 */
public class TYPEOF extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public static final String TYPE_NULL = "NULL";
  public static final String TYPE_STRING = "STRING";
  public static final String TYPE_LONG = "LONG";
  public static final String TYPE_DOUBLE = "DOUBLE";
  public static final String TYPE_BOOLEAN = "BOOLEAN";
  public static final String TYPE_LIST = "LIST";
  public static final String TYPE_MAP = "MAP";
  public static final String TYPE_MACRO = "MACRO";
  public static final String TYPE_MAPPER = "MAPPER";
  public static final String TYPE_REDUCER = "REDUCER";
  public static final String TYPE_GTS = "GTS";
  public static final String TYPE_BYTES = "BYTES";
  public static final String TYPE_PGRAPHICSIMAGE = "PGRAPHICS";
  public static final String TYPE_GEOSHAPE = "GEOSHAPE";
  public static final String TYPE_SET = "SET";
  public static final String TYPE_BITSET = "BITSET";
  public static final String TYPE_VECTOR = "VLIST";
  public static final String TYPE_REALVECTOR = "VECTOR";
  public static final String TYPE_REALMATRIX = "MATRIX";
  public static final String TYPE_PIMAGE = "PIMAGE";
  public static final String TYPE_PFONT = "PFONT";
  public static final String TYPE_PSHAPE = "PSHAPE";

  private boolean recursivetypeof;

  public TYPEOF(String name, boolean recursivetypeof) {
    super(name);
    this.recursivetypeof = recursivetypeof;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    if (this.recursivetypeof) {
      stack.push(this.rtypeof(o,0,stack.DEFAULT_MAX_RECURSION_LEVEL));
    } else {
      stack.push(this.typeof(o));
    }
    return stack;
  }

  private String rtypeof(Object o, int recursionlevel, int maxrecursionlevel) {
    if (recursionlevel > maxrecursionlevel){
      return "... (recursion limit reached)";
    }
    else
    {
      if (o instanceof List) {
         if (0 == ((List) o).size()) {
           return "empty LIST ";
         } else {
           //type of the first element in the list
           return  "LIST of " + this.rtypeof(((List) o).get(0), recursionlevel + 1, maxrecursionlevel);
         }
      } else if (o instanceof Map){
        if (0 == ((Map) o).size()) {
          return "empty MAP";
        } else {
          Object onekey = ((Map) o).keySet().toArray()[0]; //typeof the first available key/value
          return  "MAP (key:" + this.rtypeof(onekey, recursionlevel + 1, maxrecursionlevel) +
                  ", value:" + this.rtypeof(((Map) o).get(onekey), recursionlevel + 1, maxrecursionlevel) +
                  ")";
        }
      } else {
        return this.typeof(o);
      }
    }
  }

  private String typeof(Object o) {
    if (null == o) {
      return(TYPE_NULL);
    } else if (o instanceof String) {
      return(TYPE_STRING);
    } else if (o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte || o instanceof BigInteger) {
      return(TYPE_LONG);
    } else if (o instanceof Double || o instanceof Float || o instanceof BigDecimal) {
      return(TYPE_DOUBLE);
    } else if (o instanceof Boolean) {
      return(TYPE_BOOLEAN);
    } else if (o instanceof Vector) {  // place before List. Vector implements List.
      return(TYPE_VECTOR);
    } else if (o instanceof List) {
      return(TYPE_LIST);
    } else if (o instanceof Map) {
      return(TYPE_MAP);
    } else if (o instanceof Macro) {
      return(TYPE_MACRO);
    } else if (o instanceof WarpScriptMapperFunction) {
      return(TYPE_MAPPER);
    } else if (o instanceof WarpScriptReducerFunction) {
      return(TYPE_REDUCER);
    } else if (o instanceof GeoTimeSerie) {
      return(TYPE_GTS);
    } else if (o instanceof byte[] ) {
      return(TYPE_BYTES);
    } else if (o instanceof PGraphics) {
      return(TYPE_PGRAPHICSIMAGE);
    } else if (o instanceof PImage) {
      return(TYPE_PIMAGE);
    } else if (o instanceof PFont) {
      return(TYPE_PFONT);
    } else if (o instanceof PShape) {
      return(TYPE_PSHAPE);
    } else if (o instanceof GeoXPLib.GeoXPShape) {
      return(TYPE_GEOSHAPE);
    } else if (o instanceof Set) {
      return(TYPE_SET);
    } else if (o instanceof BitSet) {
      return(TYPE_BITSET);
    } else if (o instanceof ArrayRealVector) {
      return(TYPE_REALVECTOR);
    } else if (o instanceof RealMatrix) {
      return(TYPE_REALMATRIX);
    } else {
      return(o.getClass().getCanonicalName());
    }
  }

}
