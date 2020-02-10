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

import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptNAryFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import processing.core.PFont;
import processing.core.PGraphics;
import processing.core.PImage;
import processing.core.PShape;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

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
  public static final String TYPE_AGGREGATOR = "AGGREGATOR";
  public static final String TYPE_FILLER = "FILLER";
  public static final String TYPE_FILTER = "FILTER";
  public static final String TYPE_OPERATOR = "OPERATOR";
  public static final String TYPE_GTS = "GTS";
  public static final String TYPE_GTSENCODER = "GTSENCODER";
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
  public static final String TYPE_COUNTER = "COUNTER";

  public interface Typeofable {
    String typeof();
  }

  public TYPEOF(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    stack.push(typeof(o));
    return stack;
  }

  public static String typeof(Object o) {
    if (null == o) {
      return TYPE_NULL;
    } else if (o instanceof String) {
      return TYPE_STRING;
    } else if (o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte || o instanceof BigInteger) {
      return TYPE_LONG;
    } else if (o instanceof Double || o instanceof Float || o instanceof BigDecimal) {
      return TYPE_DOUBLE;
    } else if (o instanceof Boolean) {
      return TYPE_BOOLEAN;
    } else if (o instanceof Vector) {  // place before List. Vector implements List.
      return TYPE_VECTOR;
    } else if (o instanceof List) {
      return TYPE_LIST;
    } else if (o instanceof Map) {
      return TYPE_MAP;
    } else if (o instanceof Macro) {
      return TYPE_MACRO;
    } else if (o instanceof WarpScriptAggregatorFunction) {
      return TYPE_AGGREGATOR;
    } else if (o instanceof WarpScriptFillerFunction) {
      return TYPE_FILLER;
    } else if (o instanceof WarpScriptFilterFunction) {
      return TYPE_FILTER;
    } else if (o instanceof WarpScriptNAryFunction) {
      return TYPE_OPERATOR;
    } else if (o instanceof GeoTimeSerie) {
      return TYPE_GTS;
    } else if (o instanceof GTSEncoder) {
      return TYPE_GTSENCODER;
    } else if (o instanceof byte[]) {
      return TYPE_BYTES;
    } else if (o instanceof PGraphics) {
      return TYPE_PGRAPHICSIMAGE;
    } else if (o instanceof PImage) {
      return TYPE_PIMAGE;
    } else if (o instanceof PFont) {
      return TYPE_PFONT;
    } else if (o instanceof PShape) {
      return TYPE_PSHAPE;
    } else if (o instanceof GeoXPLib.GeoXPShape) {
      return TYPE_GEOSHAPE;
    } else if (o instanceof Set) {
      return TYPE_SET;
    } else if (o instanceof BitSet) {
      return TYPE_BITSET;
    } else if (o instanceof ArrayRealVector) {
      return TYPE_REALVECTOR;
    } else if (o instanceof RealMatrix) {
      return TYPE_REALMATRIX;
    } else if (o instanceof AtomicLong) {
      return TYPE_COUNTER;
    } else if (o instanceof Typeofable) {
      try {
        return (((Typeofable) o).typeof());
      } catch (Exception e) {
        return defaultType(o);
      }
    } else {
      return defaultType(o);
    }
  }

  public static String defaultType(Object o) {
    String canonicalName = o.getClass().getCanonicalName();
    if (null == canonicalName) {
      return "X-(Local/Anonymous Class)";
    } else {
      return canonicalName;
    }
  }

}
