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
  public static final String TYPE_COUNTER = "COUNTER";

  public static interface Typeofable {
    public String typeof();
  }

  public TYPEOF(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    stack.push(this.typeof(o));
    return stack;
  }

  public static String typeof(Object o) {
    if (null == o) {
      return (TYPE_NULL);
    } else if (o instanceof Typeofable) {
      return ("X-" + ((Typeofable) o).typeof());
    } else {
      return typeof(o.getClass());
    }
  }

  public static String typeof(Class c) {
    if (String.class.isAssignableFrom(c)) {
      return (TYPE_STRING);
    } else if (Long.class.isAssignableFrom(c) || Integer.class.isAssignableFrom(c) || Short.class.isAssignableFrom(c) || Byte.class.isAssignableFrom(c) || BigInteger.class.isAssignableFrom(c)) {
      return (TYPE_LONG);
    } else if (Double.class.isAssignableFrom(c) || Float.class.isAssignableFrom(c) || BigDecimal.class.isAssignableFrom(c)) {
      return (TYPE_DOUBLE);
    } else if (Boolean.class.isAssignableFrom(c)) {
      return (TYPE_BOOLEAN);
    } else if (Vector.class.isAssignableFrom(c)) {  // place before List. Vector implements List.
      return (TYPE_VECTOR);
    } else if (List.class.isAssignableFrom(c)) {
      return (TYPE_LIST);
    } else if (Map.class.isAssignableFrom(c)) {
      return (TYPE_MAP);
    } else if (Macro.class.isAssignableFrom(c)) {
      return (TYPE_MACRO);
    } else if (WarpScriptMapperFunction.class.isAssignableFrom(c)) {
      return (TYPE_MAPPER);
    } else if (WarpScriptReducerFunction.class.isAssignableFrom(c)) {
      return (TYPE_REDUCER);
    } else if (GeoTimeSerie.class.isAssignableFrom(c)) {
      return (TYPE_GTS);
    } else if (byte[].class.isAssignableFrom(c)) {
      return (TYPE_BYTES);
    } else if (PGraphics.class.isAssignableFrom(c)) {
      return (TYPE_PGRAPHICSIMAGE);
    } else if (PImage.class.isAssignableFrom(c)) {
      return (TYPE_PIMAGE);
    } else if (PFont.class.isAssignableFrom(c)) {
      return (TYPE_PFONT);
    } else if (PShape.class.isAssignableFrom(c)) {
      return (TYPE_PSHAPE);
    } else if (GeoXPLib.GeoXPShape.class.isAssignableFrom(c)) {
      return (TYPE_GEOSHAPE);
    } else if (Set.class.isAssignableFrom(c)) {
      return (TYPE_SET);
    } else if (BitSet.class.isAssignableFrom(c)) {
      return (TYPE_BITSET);
    } else if (ArrayRealVector.class.isAssignableFrom(c)) {
      return (TYPE_REALVECTOR);
    } else if (RealMatrix.class.isAssignableFrom(c)) {
      return (TYPE_REALMATRIX);
    } else if (AtomicLong.class.isAssignableFrom(c)) {
      return (TYPE_COUNTER);
    } else if (Typeofable.class.isAssignableFrom(c)) {
      try {
        return (((Typeofable) c.newInstance()).typeof());
      } catch(Exception e) {
        return ("X-" + c.getSimpleName().toUpperCase());
      }
    } else {
      return (c.getCanonicalName());
    }
  }

}
