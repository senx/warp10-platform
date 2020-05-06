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

package io.warp10.script.processing.image;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import processing.core.PGraphics;
import processing.core.PImage;

public class Psize extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Psize(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (top instanceof PGraphics) {
      PGraphics pg = (PGraphics) top;
      stack.push((long) pg.pixelWidth);
      stack.push((long) pg.pixelHeight);
    } else if (top instanceof PImage) {
      PImage pi = (PImage) top;      
      stack.push((long) pi.pixelWidth);
      stack.push((long) pi.pixelHeight);
    } else {
      throw new WarpScriptException(getName() + " expects an image or a PGraphics instance.");
    }
        
    return stack;
  }
}
