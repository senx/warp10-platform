//
//   Copyright 2021  SenX S.A.S.
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
import io.warp10.script.functions.TYPEOF;
import processing.core.PImage;

/**
 * Resize a PImage using bilinear interpolation.
 */
public class Presize extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public Presize(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a height in pixel.");
    }

    int height = -1;
    try {
      height = Math.toIntExact((Long) top);
      if (height < 0) {
        throw new IllegalArgumentException();
      }
    } catch (ArithmeticException | IllegalArgumentException e) {
      throw new WarpScriptException(getName() + " expects the height to be positive and less than " + Integer.MAX_VALUE + ".");
    }

    top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a width in pixel.");
    }

    int width = -1;
    try {
      width = Math.toIntExact((Long) top);
      if (width < 0) {
        throw new IllegalArgumentException();
      }
    } catch (ArithmeticException | IllegalArgumentException e) {
      throw new WarpScriptException(getName() + " expects the height to be positive and less than " + Integer.MAX_VALUE + ".");
    }

    long PIXEL_LIMIT = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MAX_PIXELS);

    if ((long) width * height > PIXEL_LIMIT) {
      throw new WarpScriptException(getName() + " only allows " + TYPEOF.TYPE_PGRAPHICSIMAGE + " or " + TYPEOF.TYPE_PIMAGE + " with a total number of pixels less than " + PIXEL_LIMIT + " requested size was " + width + "x" + height + " (" + ((long) width * height) + ").");
    }

    top = stack.pop();

    if (top instanceof PImage) { // PGraphics extends PImage
      PImage img = (PImage) top;
      img.resize(width, height);
      stack.push(img);
    } else {
      throw new WarpScriptException(getName() + " expects a " + TYPEOF.TYPE_PIMAGE + " or a " + TYPEOF.TYPE_PGRAPHICSIMAGE + " instance.");
    }

    return stack;
  }
}
