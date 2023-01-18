//
//   Copyright 2020-2022  SenX S.A.S.
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

import com.geoxp.GeoXPLib.GeoXPShape;
import io.warp10.WarpURLEncoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.UnsafeString;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Mark;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.processing.Pencode;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;

import processing.core.PGraphics;
import processing.core.PImage;
import processing.core.PShapeSVG;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;

/**
 * Replaces the stack so far with a WarpScript snippet which will regenerate
 * the same stack content.
 *
 * Some elements may fail to be converted correctly (i.e. macros)
 *
 */
public class SNAPSHOT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static ThreadLocal<AtomicInteger> recursionDepth = new ThreadLocal<AtomicInteger>() {
    @Override
    protected AtomicInteger initialValue() {
      return new AtomicInteger();
    }
  };

  public static interface SnapshotEncoder {
    public boolean addElement(SNAPSHOT snapshot, StringBuilder sb, Object o, boolean readable) throws WarpScriptException;
  }

  public static interface Snapshotable {
    public String snapshot();
  }

  private static List<SnapshotEncoder> encoders = null;

  /**
   * Maximum level of data structure nesting
   */
  private static final int MAX_RECURSION_LEVEL = 16;

  private final boolean snapshotSymbols;

  private final boolean toMark;

  private final boolean countbased;

  /**
   * Should we compress wrappers when snapshotting GTS/Encoders?
   */
  private final boolean compresswrappers;

  /**
   * Should we pop the elements out of the stack when building the snapshot
   */
  private final boolean pop;

  /**
   * Should we generate a snapshot easier for humans to read.
   */
  private final boolean readable;

  public SNAPSHOT(String name, boolean snapshotSymbols, boolean toMark, boolean pop, boolean countbased) {
    this(name, snapshotSymbols, toMark, pop, countbased, true);
  }

  public SNAPSHOT(String name, boolean snapshotSymbols, boolean toMark, boolean pop, boolean countbased, boolean compresswrappers) {
    this(name, snapshotSymbols, toMark, pop, countbased, compresswrappers, false);
  }

  public SNAPSHOT(String name, boolean snapshotSymbols, boolean toMark, boolean pop, boolean countbased, boolean compresswrappers, boolean readable) {
    super(name);
    this.snapshotSymbols = snapshotSymbols;
    this.toMark = toMark;
    this.pop = pop;

    this.countbased = countbased;
    this.compresswrappers = compresswrappers;
    this.readable = readable;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    int lastidx = 0;

    StringBuilder sb = new StringBuilder();

    if (this.countbased) {
      Object top = stack.pop();

      if (!(top instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a number of stack levels to snapshot.");
      }

      lastidx = ((Number) top).intValue() - 1;

      if (lastidx > stack.depth() - 1) {
        lastidx = stack.depth() - 1;
      }
    } else if (!this.toMark) {
      lastidx = stack.depth() - 1;
    } else {
      int i = 0;
      while (i < stack.depth() && !(stack.get(i) instanceof Mark)) {
        i++;
      }
      lastidx = i;
      if (lastidx >= stack.depth()) {
        throw new WarpScriptException(getName() + " expects a MARK on the stack.");
      }
    }

    for (int i = lastidx; i >= 0; i--) {

      // Do not include the MARK
      if (this.toMark && lastidx == i) {
        continue;
      }

      Object o = stack.get(i);

      addElement(this, sb, o, readable);
    }

    //
    // Add the defined symbols if requested to do so
    //
    if (this.snapshotSymbols) {
      for (Entry<String, Object> entry : stack.getSymbolTable().entrySet()) {
        addElement(this, sb, entry.getValue(), readable);
        addElement(this, sb, entry.getKey(), readable);
        sb.append(WarpScriptLib.STORE);
        sb.append(" ");
      }

      //
      // Snapshot the registers
      //

      Object[] regs = stack.getRegisters();

      sb.append(WarpScriptLib.CLEARREGS);
      sb.append(" ");
      for (int i = 0; i < regs.length; i++) {
        if (null != regs[i]) {
          addElement(this, sb, regs[i], readable);
          sb.append(WarpScriptLib.POPR);
          sb.append(i);
          sb.append(" ");
        }
      }
    }

    // Clear the stack
    if (pop) {
      if (stack.depth() - 1 == lastidx) {
        stack.clear();
      } else {
        while (lastidx >= 0) {
          stack.pop();
          lastidx--;
        }
      }
    }

    // Push the snapshot onto the stack
    stack.push(sb.toString());

    return stack;
  }

  public static void addElement(StringBuilder sb, Object o) throws WarpScriptException {
    addElement(null, sb, o, false);
  }

  public static void addElement(StringBuilder sb, Object o, boolean readable) throws WarpScriptException {
    addElement(null, sb, o, readable);
  }

  public static void addElement(SNAPSHOT snapshot, StringBuilder sb, Object o) throws WarpScriptException {
    addElement(snapshot, sb, o, false);
  }

  public static void addElement(SNAPSHOT snapshot, StringBuilder sb, Object o, boolean readable) throws WarpScriptException {

    AtomicInteger depth = null;

    try {
      depth = recursionDepth.get();

      if (depth.addAndGet(1) > MAX_RECURSION_LEVEL) {
        throw new WarpScriptException("Recursive data structures exceeded " + MAX_RECURSION_LEVEL + " levels.");
      }

      if (null == o) {
        sb.append(WarpScriptLib.NULL);
        sb.append(" ");
      } else if (o instanceof AtomicLong) {
        sb.append(WarpScriptLib.COUNTER);
        sb.append(" ");
        if (0 != ((AtomicLong) o).get()) {
          sb.append(((AtomicLong) o).get());
          sb.append(" ");
          sb.append(WarpScriptLib.COUNTERSET);
          sb.append(" ");
        }
      } else if (o instanceof Number) {
        sb.append(o);
        sb.append(" ");
      } else if (o instanceof String) {
        sb.append("'");
        if (readable) {
          appendProcessedString(sb, o.toString());
        } else {
          try {
            sb.append(WarpURLEncoder.encode(o.toString(), StandardCharsets.UTF_8));
          } catch (UnsupportedEncodingException uee) {
            throw new WarpScriptException(uee);
          }
        }
        sb.append("' ");
      } else if (o instanceof Boolean) {
        if (Boolean.TRUE.equals(o)) {
          sb.append("true ");
        } else {
          sb.append("false ");
        }
      } else if (o instanceof GeoTimeSerie || o instanceof GTSEncoder) {
        sb.append("'");
        //
        // Create a stack so we can call WRAP
        //

        MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());
        stack.maxLimits();

        stack.push(o);
        WRAP w = new WRAP("", false, null == snapshot ? true : snapshot.compresswrappers);
        w.apply(stack);

        sb.append(stack.pop());

        sb.append("' ");
        if (o instanceof GeoTimeSerie) {
          sb.append(WarpScriptLib.UNWRAP);
        } else {
          sb.append(WarpScriptLib.UNWRAPENCODER);
        }
        sb.append(" ");
      } else if (o instanceof Vector) {
        if (readable) {
          sb.append(WarpScriptLib.VECTOR_START);
          sb.append(" ");
          for (Object oo : (Vector) o) {
            addElement(snapshot, sb, oo, true);
          }
          sb.append(WarpScriptLib.VECTOR_END);
          sb.append(" ");
        } else {
          sb.append(WarpScriptLib.EMPTY_VECTOR);
          sb.append(" ");
          for (Object oo : (Vector) o) {
            addElement(snapshot, sb, oo, false);
            sb.append(WarpScriptLib.INPLACEADD);
            sb.append(" ");
          }
        }
      } else if (o instanceof List) {
        if (readable) {
          sb.append(WarpScriptLib.LIST_START);
          sb.append(" ");
          for (Object oo : (List) o) {
            addElement(snapshot, sb, oo, true);
          }
          sb.append(WarpScriptLib.LIST_END);
          sb.append(" ");
        } else {
          sb.append(WarpScriptLib.EMPTY_LIST);
          sb.append(" ");
          for (Object oo : (List) o) {
            addElement(snapshot, sb, oo, false);
            sb.append(WarpScriptLib.INPLACEADD);
            sb.append(" ");
          }
        }
      } else if (o instanceof Set) {
        if (readable) {
          sb.append(WarpScriptLib.SET_START);
          sb.append(" ");
          for (Object oo : (Set) o) {
            addElement(snapshot, sb, oo, true);
          }
          sb.append(WarpScriptLib.SET_END);
          sb.append(" ");
        } else {
          sb.append(WarpScriptLib.EMPTY_SET);
          sb.append(" ");
          for (Object oo : (Set) o) {
            addElement(snapshot, sb, oo, false);
            sb.append(WarpScriptLib.INPLACEADD);
            sb.append(" ");
          }
        }
      } else if (o instanceof Map) {
        if (readable) {
          sb.append(WarpScriptLib.MAP_START);
          sb.append(System.lineSeparator());
          for (Entry<Object, Object> entry: ((Map<Object, Object>) o).entrySet()) {
            addElement(snapshot, sb, entry.getKey(), true);
            addElement(snapshot, sb, entry.getValue(), true);
            sb.append(System.lineSeparator());
          }
          sb.append(WarpScriptLib.MAP_END);
          sb.append(" ");
        } else {
          sb.append(WarpScriptLib.EMPTY_MAP);
          sb.append(" ");
          for (Entry<Object, Object> entry: ((Map<Object, Object>) o).entrySet()) {
            addElement(snapshot, sb, entry.getValue(), false);
            addElement(snapshot, sb, entry.getKey(), false);
            sb.append(WarpScriptLib.PUT);
            sb.append(" ");
          }
        }
      } else if (o instanceof BitSet) {
        sb.append("'");
        sb.append(new String(OrderPreservingBase64.encode(((BitSet) o).toByteArray()), StandardCharsets.UTF_8));
        sb.append("' ");
        sb.append(WarpScriptLib.OPB64TO);
        sb.append(" ");
        sb.append(WarpScriptLib.BYTESTOBITS);
        sb.append(" ");
      } else if (o instanceof byte[]) {
        sb.append("'");
        sb.append(new String(OrderPreservingBase64.encode((byte[]) o), StandardCharsets.UTF_8));
        sb.append("' ");
        sb.append(WarpScriptLib.OPB64TO);
        sb.append(" ");
      } else if (o instanceof GeoXPShape) {
        sb.append("'");
        sb.append(GEOPACK.pack((GeoXPShape) o));
        sb.append("' ");
        sb.append(WarpScriptLib.GEOUNPACK);
        sb.append(" ");
      } else if (o instanceof Mark) {
        sb.append(WarpScriptLib.MARK);
        sb.append(" ");
      } else if (o instanceof Snapshotable) { // Also includes Macro
        sb.append(((Snapshotable) o).snapshot());
        sb.append(" ");
      } else if (o instanceof RSAPublicKey) {
        sb.append("{ 'algorithm' 'RSA' 'exponent' '");
        sb.append(((RSAPublicKey) o).getPublicExponent());
        sb.append("' 'modulus' '");
        sb.append(((RSAPublicKey) o).getModulus());
        sb.append("' } ");
        sb.append(WarpScriptLib.RSAPUBLIC);
        sb.append(" ");
      } else if (o instanceof RSAPrivateKey) {
        sb.append("{ 'algorithm' 'RSA' 'exponent' '");
        sb.append(((RSAPrivateKey) o).getPrivateExponent());
        sb.append("' 'modulus' '");
        sb.append(((RSAPrivateKey) o).getModulus());
        sb.append("' } ");
        sb.append(WarpScriptLib.RSAPRIVATE);
        sb.append(" ");
      } else if (o instanceof PGPPublicKey) {
        try {
          addElement(sb, ((PGPPublicKey) o).getEncoded(false));
        } catch (IOException ioe) {
          throw new WarpScriptException("Error while serializing PGP public key.", ioe);
        }
        sb.append(WarpScriptLib.PGPPUBLIC);
        sb.append(" ");
        String keyid = "000000000000000" + Long.toHexString(((PGPPublicKey) o).getKeyID());
        keyid = keyid.substring(keyid.length() - 16, keyid.length()).toUpperCase();
        addElement(sb, keyid);
        sb.append(WarpScriptLib.GET);
        sb.append(" ");
        addElement(sb, PGPPUBLIC.KEY_KEY);
        sb.append(WarpScriptLib.GET);
        sb.append(" ");
      } else if (o instanceof PGPSecretKeyRing) {
        try {
          addElement(sb, ((PGPSecretKeyRing) o).getEncoded());
        } catch (IOException ioe) {
          throw new WarpScriptException("Error while serializing PGP secret key ring.", ioe);
        }
        sb.append(WarpScriptLib.PGPRING);
        sb.append(" 0 ");
        sb.append(WarpScriptLib.GET);
        sb.append(" ");
      } else if (o instanceof PGPPublicKeyRing) {
        try {
          addElement(sb, ((PGPPublicKeyRing) o).getEncoded());
        } catch (IOException ioe) {
          throw new WarpScriptException("Error while serializing PGP public key ring.", ioe);
        }
        sb.append(WarpScriptLib.PGPRING);
        sb.append(" 0 ");
        sb.append(WarpScriptLib.GET);
        sb.append(" ");
      } else if (o instanceof NamedWarpScriptFunction) {
        sb.append(o.toString());
        sb.append(" ");
      } else if (o instanceof PImage && !(o instanceof PGraphics)) {
        // PGraphics cannot be snapshot properly because it would require PFont to be snapshotable, which is not.
        sb.append("'");
        sb.append(Pencode.PImageToString((PImage) o, null));
        sb.append("' ");
        sb.append(WarpScriptLib.PDECODE);
        sb.append(" ");
      } else if (o instanceof PShapeSVG) {
        PShapeSVG pshape = (PShapeSVG) o;
        // The source SVG is package-protected, so use reflection on the PShapeSVG to get it.
        try {
          Field elementField = PShapeSVG.class.getDeclaredField("element");
          elementField.setAccessible(true);
          Object element = elementField.get(pshape);
          sb.append("'");
          sb.append(element.toString());
          sb.append("' ");
          sb.append(WarpScriptLib.PLOADSHAPE);
          sb.append(" ");
        } catch (NoSuchFieldException | IllegalAccessException e) {
          sb.append("'UNSUPPORTED:" + WarpURLEncoder.encode(o.getClass().toString(), StandardCharsets.UTF_8) + "' ");
        }
      } else if (o instanceof RealVector) {
        RealVector vector = (RealVector) o;
        if (readable) {
          sb.append(WarpScriptLib.LIST_START);
          sb.append(" ");
          for (int i = 0; i < vector.getDimension(); i++) {
            sb.append(vector.getEntry(i));
            sb.append(" ");
          }
          sb.append(WarpScriptLib.LIST_END);
          sb.append(" ");
        } else {
          sb.append(WarpScriptLib.EMPTY_LIST);
          sb.append(" ");
          for (int i = 0; i < vector.getDimension(); i++) {
            sb.append(vector.getEntry(i));
            sb.append(" ");
            sb.append(WarpScriptLib.INPLACEADD);
            sb.append(" ");
          }
        }
        sb.append(WarpScriptLib.TOVEC);
        sb.append(" ");
      } else if (o instanceof RealMatrix) {
        RealMatrix matrix = (RealMatrix) o;
        if (readable) {
          sb.append(WarpScriptLib.LIST_START);
          sb.append(System.lineSeparator());
          for (int i = 0; i < matrix.getColumnDimension(); i++) {
            sb.append(WarpScriptLib.LIST_START);
            sb.append(" ");
            for (int j = 0; j < matrix.getRowDimension(); j++) {
              sb.append(matrix.getEntry(i, j));
              sb.append(" ");
            }
            sb.append(WarpScriptLib.LIST_END);
            sb.append(System.lineSeparator());
          }
          sb.append(WarpScriptLib.LIST_END);
          sb.append(" ");
        } else {
          sb.append(WarpScriptLib.EMPTY_LIST);
          sb.append(" ");
          for (int i = 0; i < matrix.getColumnDimension(); i++) {
            sb.append(WarpScriptLib.EMPTY_LIST);
            sb.append(" ");
            for (int j = 0; j < matrix.getRowDimension(); j++) {
              sb.append(matrix.getEntry(i, j));
              sb.append(" ");
              sb.append(WarpScriptLib.INPLACEADD);
              sb.append(" ");
            }
            sb.append(WarpScriptLib.INPLACEADD);
            sb.append(" ");
          }
        }
        sb.append(WarpScriptLib.TOMAT);
        sb.append(" ");
      } else if (o instanceof Matcher) {
        sb.append("'");
        sb.append(((Matcher) o).pattern());
        sb.append("' ");
        sb.append(WarpScriptLib.MATCHER);
        sb.append(" ");
      } else {
        // Check if any of the defined encoders can encode the current element
        // loops will be caught by the recursion level check
        boolean encoded = false;
        if (null != encoders) {
          for (SnapshotEncoder encoder: encoders) {
            encoded = encoder.addElement(snapshot, sb, o, readable);
            if (encoded) {
              break;
            }
          }
        }

        // Some types are not supported
        // Nevertheless we need to have the correct levels of the stack preserved, so
        // we push an informative string onto the stack there
        if (!encoded) {
          sb.append("'UNSUPPORTED:" + WarpURLEncoder.encode(o.getClass().toString(), StandardCharsets.UTF_8) + "' ");
        }
      }
    } catch (UnsupportedEncodingException uee) {
      throw new WarpScriptException(uee);
    } finally {
      if (null != depth && 0 == depth.addAndGet(-1)) {
        recursionDepth.remove();
      }
    }
  }

  /**
   * Process a string to make it readable and compatible in WarpScript code.
   * @param sb A StringBuilder whose given String will appended to.
   * @param s A String which may contain invalid WarpScript characters. It will not be encapsulated with quote
   *          characters so it should be appended to the StringBuilder before and after. Double quotes will not
   *          be escaped, so only single quotes are acceptable.
   */

  public static void appendProcessedString(StringBuilder sb, String s) {
    int lastIdx = 0;
    int idx = 0;

    //
    // Replace anything below 32 and ' by %## (invalid character)
    //

    while(idx < s.length()) {
      if ('%' == s.charAt(idx) || '\'' == s.charAt(idx) || s.charAt(idx) < ' ') {

        for (int i = 0; i < idx - lastIdx; i++) {
          sb.append(s.charAt(lastIdx + i));
        }
        sb.append("%" + (s.charAt(idx) >>> 4) + Integer.toHexString(s.charAt(idx) & 0xF));
        lastIdx = ++idx;
      } else {
        idx++;
      }
    }

    if (idx > lastIdx) {
      for (int i = 0; i < idx - lastIdx; i++) {
        sb.append(s.charAt(lastIdx + i));        
      }
    }
  }

  public synchronized static void addEncoder(SnapshotEncoder encoder) {
    if (null == encoders) {
      encoders = new ArrayList<SnapshotEncoder>();
    }
    encoders.add(encoder);
  }
}
