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

import java.io.UnsupportedEncodingException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.geoxp.GeoXPLib.GeoXPShape;
import com.google.common.base.Charsets;

import io.warp10.WarpURLEncoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStack.Mark;
import io.warp10.script.WarpScriptStackFunction;

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
  
  public static interface Snapshotable {
    public String snapshot();
  }
  
  /**
   * Maximum level of data structure nesting
   */
  private static final int MAX_RECURSION_LEVEL = 16;
  
  private static final String uuid = UUID.randomUUID().toString();

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
  
  public SNAPSHOT(String name, boolean snapshotSymbols, boolean toMark, boolean pop, boolean countbased) {
    this(name, snapshotSymbols, toMark, pop, countbased, true);
  }
  
  public SNAPSHOT(String name, boolean snapshotSymbols, boolean toMark, boolean pop, boolean countbased, boolean compresswrappers) {
    super(name);
    this.snapshotSymbols = snapshotSymbols;
    this.toMark = toMark;
    this.pop = pop;

    this.countbased = countbased;
    this.compresswrappers = compresswrappers;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    int lastidx = 0;
    
    StringBuilder sb = new StringBuilder();

    if (this.countbased) {
      Object top = stack.pop();
      
      lastidx = ((Number) top).intValue() - 1;

      if (lastidx > stack.depth() - 1) {
        lastidx = stack.depth() - 1;
      }      
    } else if (!this.toMark) {
      lastidx = stack.depth() - 1;
    } else {      
      int i = 0;
      while(i < stack.depth() && !(stack.get(i) instanceof Mark)) {
        i++;
      }
      lastidx = i;
      if (lastidx >= stack.depth()) {
        lastidx = stack.depth() - 1;
      }
    }
    
    for (int i = lastidx; i >= 0; i--) {
      Object o = stack.get(i);
      
      addElement(this, sb, o);
    }      

    //
    // Add the defined symbols if requested to do so
    //
    
    if (this.snapshotSymbols) {
      for (Entry<String,Object> entry: stack.getSymbolTable().entrySet()) {
        addElement(this, sb, entry.getValue());
        addElement(this, sb, entry.getKey());
        sb.append(WarpScriptLib.STORE);
        sb.append(" ");
      }      
    }
    
    // Clear the stack
    if (pop) {
      if (stack.depth() - 1 == lastidx) {
        stack.clear();
      } else {
        while(lastidx >= 0) {
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
    addElement(null, sb, o);
  }
  
  public static void addElement(SNAPSHOT snapshot, StringBuilder sb, Object o) throws WarpScriptException {
    
    AtomicInteger depth = null;
        
    try {
      depth = recursionDepth.get();
      
      if (depth.addAndGet(1) > MAX_RECURSION_LEVEL) {
        throw new WarpScriptException("Recursive data structures exceeded " + MAX_RECURSION_LEVEL + " levels.");
      }      

      if (null == o) {
        sb.append(WarpScriptLib.NULL);
        sb.append(" ");
      } else if (o instanceof Number) {
        sb.append(o);
        sb.append(" ");
      } else if (o instanceof String) {
        sb.append("'");
        try {
          sb.append(WarpURLEncoder.encode(o.toString(), "UTF-8"));
        } catch (UnsupportedEncodingException uee) {
          throw new WarpScriptException(uee);
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
        WRAP w = new WRAP("", false, snapshot.compresswrappers);        
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
        sb.append(WarpScriptLib.LIST_START);
        sb.append(WarpScriptLib.LIST_END);
        sb.append(" ");
        for (Object oo: (List) o) {
          addElement(snapshot, sb, oo);
          sb.append(WarpScriptLib.INPLACEADD);
          sb.append(" ");          
        }
        sb.append(WarpScriptLib.TO_VECTOR);
        sb.append(" ");
      } else if (o instanceof List) {
        sb.append(WarpScriptLib.LIST_START);
        sb.append(WarpScriptLib.LIST_END);
        sb.append(" ");
        for (Object oo: (List) o) {
          addElement(snapshot, sb, oo);
          sb.append(WarpScriptLib.INPLACEADD);
          sb.append(" ");
        }
      } else if (o instanceof Set) {
        sb.append(WarpScriptLib.LIST_START);
        sb.append(WarpScriptLib.LIST_END);
        sb.append(" ");
        for (Object oo: (List) o) {
          addElement(snapshot, sb, oo);
          sb.append(WarpScriptLib.INPLACEADD);
          sb.append(" ");          
        }
        sb.append(" ");
        sb.append(WarpScriptLib.TO_SET);
        sb.append(" ");
      } else if (o instanceof Map) {
        sb.append(WarpScriptLib.MAP_START);
        sb.append(WarpScriptLib.MAP_END);
        sb.append(" ");
        for (Entry<Object, Object> entry: ((Map<Object,Object>) o).entrySet()) {
          addElement(snapshot, sb, entry.getValue());
          addElement(snapshot, sb, entry.getKey());
          sb.append(WarpScriptLib.PUT);
          sb.append(" ");
        }
      } else if (o instanceof BitSet) {
        sb.append("'");
        sb.append(new String(OrderPreservingBase64.encode(((BitSet) o).toByteArray()), Charsets.UTF_8));
        sb.append("' ");
        sb.append(WarpScriptLib.OPB64TO);
        sb.append(" ");
        sb.append(WarpScriptLib.BYTESTOBITS);
        sb.append(" ");
      } else if (o instanceof byte[]) {
        sb.append("'");
        sb.append(new String(OrderPreservingBase64.encode((byte[]) o), Charsets.UTF_8));
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
      } else if (o instanceof Snapshotable) {
        sb.append(((Snapshotable) o).snapshot());
        sb.append(" ");
      } else if (o instanceof Macro) {
        sb.append(o.toString());
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
      } else if (o instanceof NamedWarpScriptFunction) {
        sb.append(o.toString());
        sb.append(" ");
      } else {
        // Some types are not supported
        // functions, PImage...
        // Nevertheless we need to have the correct levels of the stack preserved, so
        // we push an informative string onto the stack there
        try {
          sb.append("'UNSUPPORTED:" + WarpURLEncoder.encode(o.getClass().toString(), "UTF-8") + "' ");
        } catch (UnsupportedEncodingException uee) {
          throw new WarpScriptException(uee);
        }        
      }          
    } finally {
      if (null != depth && 0 == depth.addAndGet(-1)) {
        recursionDepth.remove();
      }
    }    
  }
}
