//
//   Copyright 2020  SenX S.A.S.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import io.warp10.continuum.gts.Varint;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Converts a list of LONGs to a byte array containing varint encoding of the numbers
 */
public class TOVARINT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOVARINT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof List) && !(top instanceof Long)) {
      throw new WarpScriptException(getName() + " operates on a LONG or LIST of LONGs.");
    }
    
    if (top instanceof Long) {
      stack.push(Varint.encodeUnsignedLong(((Long) top).longValue()));
    } else {
      List<Object> longs = (List<Object>) top;
      
      ByteArrayOutputStream baos = new ByteArrayOutputStream(longs.size());
      
      try {
        for (int i = 0; i < longs.size(); i++) {
          if (!(longs.get(i) instanceof Long)) {
            throw new WarpScriptException(getName() + " operates on a LIST of LONGs.");
          }
          baos.write(Varint.encodeUnsignedLong(((Long) longs.get(i)).longValue()));
        }
        stack.push(baos.toByteArray());
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " error while encoding values.", ioe);
      }      
    }
    
    return stack;
  }
}
