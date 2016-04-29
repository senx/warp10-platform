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

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;

/**
 * Replaces the stack so far with a WarpScript snippet which will regenerate
 * the same stack content.
 * 
 * Some elements may fail to be converted correctly (i.e. macros)
 *
 */
public class SNAPSHOT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  /**
   * Maximum level of data structure nesting
   */
  private static final int MAX_RECURSION_LEVEL = 16;
  
  private static final String uuid = UUID.randomUUID().toString();

  public SNAPSHOT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    StringBuilder sb = new StringBuilder();

    //
    // Add a stack attribute with a counter to avoid endless recursion
    // in case we have self-referencing data structures
    //
        
    stack.setAttribute(uuid, new AtomicInteger());
    
    for (int i = stack.depth() - 1; i >= 0; i--) {
      Object o = stack.get(i);
      
      addElement(stack, sb, o);
    }
    
    // Clear the stack
    stack.clear();
    
    // Push the snapshot onto the stack
    stack.push(sb.toString());
    
    stack.setAttribute(uuid, null);
    
    return stack;
  }
  
  private static void addElement(WarpScriptStack stack, StringBuilder sb, Object o) throws WarpScriptException {
    AtomicInteger depth = (AtomicInteger) stack.getAttribute(uuid);
    
    if (depth.addAndGet(1) > MAX_RECURSION_LEVEL) {
      throw new WarpScriptException("Self referencing data structures exceeded " + MAX_RECURSION_LEVEL + " levels.");
    }
    
    try {
      if (o instanceof Number) {
        sb.append(o);
        sb.append(" ");
      } else if (o instanceof String) {
        sb.append("'");
        try {
          sb.append(URLEncoder.encode(o.toString(), "UTF-8"));
        } catch (UnsupportedEncodingException uee) {
          throw new WarpScriptException(uee);
        }
        sb.append("' ");
      } else if (o instanceof Boolean) {
        if (Boolean.TRUE.equals(o)) {
          sb.append("true ");
        } else {
          sb.append(" false");
        }
      } else if (o instanceof GeoTimeSerie) {
        sb.append("'");
        stack.push(o);
        WRAP w = new WRAP("");
        w.apply(stack);
        sb.append(stack.pop());
        sb.append("' UNWRAP ");
      } else if (o instanceof List) {
        sb.append("[ ");
        for (Object oo: (List) o) {
          addElement(stack, sb, oo);
        }
        sb.append("] ");
      } else if (o instanceof Map) {
        sb.append("{ ");
        for (Entry<Object, Object> entry: ((Map<Object,Object>) o).entrySet()) {
          addElement(stack, sb, entry.getKey());
          addElement(stack, sb, entry.getValue());
        }
        sb.append("} ");
      } else if (o instanceof BitSet) {
        sb.append("'");
        sb.append(new String(OrderPreservingBase64.encode(((BitSet) o).toByteArray()), Charsets.UTF_8));
        sb.append("' OPB64-> BYTESTOBITS ");
      } else if (o instanceof byte[]) {
        sb.append("'");
        sb.append(new String(OrderPreservingBase64.encode((byte[]) o), Charsets.UTF_8));
        sb.append("' OPB64-> ");
      } else {
        // Some types are not supported
        // functions, macros, PImage...
        // Nevertheless we need to have the correct levels of the stack preserved, so
        // we push an informative string onto the stack there
        try {
          sb.append("'UNSUPPORTED:" + URLEncoder.encode(o.getClass().toString(), "UTF-8") + "' ");
        } catch (UnsupportedEncodingException uee) {
          throw new WarpScriptException(uee);
        }        
      }          
    } finally {
      depth.addAndGet(-1);
    }    
  }
}
