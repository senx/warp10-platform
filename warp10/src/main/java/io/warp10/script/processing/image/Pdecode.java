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

package io.warp10.script.processing.image;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.awt.Image;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;
import javax.swing.ImageIcon;

import org.apache.commons.codec.binary.Base64;

import processing.awt.PGraphicsJava2D;
import processing.core.PApplet;
import processing.core.PImage;
import processing.opengl.PGraphics3D;

/**
 * Decode a base64 encoded image content into a PImage instance
 */
public class Pdecode extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Pdecode(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    String data = top.toString();

    if (!(top instanceof String) || !data.startsWith("data:image/")) {
      throw new WarpScriptException(getName() + " expects a base64 data URI on top of the stack.");
    }
        
    data = data.substring(data.indexOf(",") + 1);
    
    byte[] bytes = Base64.decodeBase64(data);
    
    Image awtImage = new ImageIcon(bytes).getImage();
    
    if (awtImage instanceof BufferedImage) {
      BufferedImage buffImage = (BufferedImage) awtImage;
      int space = buffImage.getColorModel().getColorSpace().getType();
      if (space == ColorSpace.TYPE_CMYK) {
        throw new WarpScriptException(getName() + " only supports RGB images.");
      }
    }
    
    PImage image = new PImage(awtImage);
    
    //
    // Check transparency
    //
    
    if (null != image.pixels) {
      for (int i = 0; i < image.pixels.length; i++) {
        // since transparency is often at corners, hopefully this
        // will find a non-transparent pixel quickly and exit
        if ((image.pixels[i] & 0xff000000) != 0xff000000) {
          image.format = PImage.ARGB;
          break;
        }
      }      
    }

    stack.push(image);
    
    return stack;
  }
}
