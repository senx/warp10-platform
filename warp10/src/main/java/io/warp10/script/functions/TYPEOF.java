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
import processing.core.PGraphics;
import processing.core.PImage;
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

  public TYPEOF(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (null == o) {
      stack.push(TYPE_NULL);
    } else if (o instanceof String) {
      stack.push(TYPE_STRING);
    } else if (o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte || o instanceof BigInteger) {
      stack.push(TYPE_LONG);
    } else if (o instanceof Double || o instanceof Float || o instanceof BigDecimal) {
      stack.push(TYPE_DOUBLE);
    } else if (o instanceof Boolean) {
      stack.push(TYPE_BOOLEAN);
    } else if (o instanceof Vector) {  // place before List. Vector implements List.
      stack.push(TYPE_VECTOR);
    } else if (o instanceof List) {
      stack.push(TYPE_LIST);
    } else if (o instanceof Map) {
      stack.push(TYPE_MAP);
    } else if (o instanceof Macro) {
      stack.push(TYPE_MACRO);
    } else if (o instanceof WarpScriptMapperFunction) {
      stack.push(TYPE_MAPPER);
    } else if (o instanceof WarpScriptReducerFunction) {
      stack.push(TYPE_REDUCER);
    } else if (o instanceof GeoTimeSerie) {
      stack.push(TYPE_GTS);
    } else if (o instanceof byte[] ) {
      stack.push(TYPE_BYTES);
    } else if (o instanceof PGraphics) {
      stack.push(TYPE_PGRAPHICSIMAGE);
    } else if (o instanceof PImage) {
      stack.push(TYPE_PIMAGE);
    } else if (o instanceof GeoXPLib.GeoXPShape) {
      stack.push(TYPE_GEOSHAPE);
    } else if (o instanceof Set) {
      stack.push(TYPE_SET);
    } else if (o instanceof BitSet) {
      stack.push(TYPE_BITSET);
    } else if (o instanceof ArrayRealVector) {
      stack.push(TYPE_REALVECTOR);
    } else if (o instanceof RealMatrix) {
      stack.push(TYPE_REALMATRIX);
    } else {
      stack.push(o.getClass().getCanonicalName());
    }

    return stack;
  }
}
