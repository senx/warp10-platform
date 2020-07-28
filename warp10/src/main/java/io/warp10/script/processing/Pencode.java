//
//   Copyright 2018-2020  SenX S.A.S.
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

package io.warp10.script.processing;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;

import org.apache.commons.codec.binary.Base64;

import com.sun.imageio.plugins.png.PNGMetadata;

/**
 * Encode the PGraphics on the stack into a base64 string suitable for a data URL.
 */
public class Pencode extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Pencode(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    Map<Object,Object> chunks = null;
    
    if (top instanceof Map) {
      chunks = (Map<Object,Object>) top;
      top = stack.pop();
    }
    
    if (!(top instanceof processing.core.PGraphics)) {
      throw new WarpScriptException(getName() + " operates on a PGraphics instance.");
    }

    processing.core.PGraphics pg = (processing.core.PGraphics) top;
    
    pg.endDraw();
    
    BufferedImage bimage = new BufferedImage(pg.pixelWidth, pg.pixelHeight, BufferedImage.TYPE_INT_ARGB);
    bimage.setRGB(0, 0, pg.pixelWidth, pg.pixelHeight, pg.pixels, 0, pg.pixelWidth);
    Iterator<ImageWriter> iter = ImageIO.getImageWritersByFormatName("png");
    ImageWriter writer = null;
    if (iter.hasNext()) {
      writer = iter.next();
    }
    ImageWriteParam param = writer.getDefaultWriteParam();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BufferedOutputStream output = new BufferedOutputStream(baos);
    
    try {
      writer.setOutput(ImageIO.createImageOutputStream(output));
      
      IIOImage iioimage = new IIOImage(bimage, null, null);
      
      if (null != chunks) {
        PNGMetadata metadata = new PNGMetadata();
        
        for (Entry<Object,Object> entry: chunks.entrySet()) {
          if ("tEXt".equals(entry.getKey()) || "zTXt".equals(entry.getKey())) {
            boolean zTXt = "zTXt".equals(entry.getKey());
            Object chunklist = entry.getValue();
            
            if (!(chunklist instanceof List)) {
              throw new WarpScriptException(getName() + " expects chunk type to be associated with a list of chunks.");
            }
            
            for (Object chunkelt: (List<Object>) chunklist) {
              if (!(chunkelt instanceof Map)) {
                throw new WarpScriptException(getName() + " expects tEXt and zTXt chunks to be MAP instances.");
              }
              Map<Object,Object> chunkmap = (Map<Object,Object>) chunkelt;
              
              if (chunkmap.get("keyword") instanceof String && chunkmap.get("text") instanceof String) {
                if (zTXt) {
                  metadata.zTXt_keyword.add((String) chunkmap.get("keyword"));
                  metadata.zTXt_text.add((String) chunkmap.get("text"));
                  metadata.zTXt_compressionMethod.add(0);
                } else {
                  metadata.tEXt_keyword.add((String) chunkmap.get("keyword"));
                  metadata.tEXt_text.add((String) chunkmap.get("text"));                  
                }
              } else {
                throw new WarpScriptException(getName() + " tEXt and zTXt chunks MUST contains 'keyword' and 'text' entries of type STRING.");
              }
            }
          } else if ("iTXt".equals(entry.getKey())) {
            Object chunklist = entry.getValue();
            
            if (!(chunklist instanceof List)) {
              throw new WarpScriptException(getName() + " expects chunk type to be associated with a list of chunks.");
            }
            
            for (Object chunkelt: (List<Object>) chunklist) {
              if (!(chunkelt instanceof Map)) {
                throw new WarpScriptException(getName() + " expects iTXt chunks to be MAP instances.");
              }
              Map<Object,Object> chunkmap = (Map<Object,Object>) chunkelt;
              
              if (chunkmap.get("keyword") instanceof String && chunkmap.get("text") instanceof String) {
                metadata.iTXt_keyword.add((String) chunkmap.get("keyword"));
                metadata.iTXt_text.add((String) chunkmap.get("text"));
                metadata.iTXt_compressionFlag.add(Boolean.TRUE.equals(chunkmap.get("compressionFlag")));
                // 0 is the only supported compression method (http://www.libpng.org/pub/png/spec/1.2/PNG-Chunks.html)
                metadata.iTXt_compressionMethod.add(0);
                metadata.iTXt_languageTag.add(chunkmap.getOrDefault("languageTag", "").toString());
                metadata.iTXt_translatedKeyword.add(chunkmap.getOrDefault("translatedKeyword", "").toString());
              } else {
                throw new WarpScriptException(getName() + " iTXt chunks MUST contains 'keyword' and 'text' entries of type STRING.");
              }
            }
          } else {
            throw new WarpScriptException(getName() + " only 'tEXt', 'zTXt' and 'iTXt' chunks can be specified.");
          }
        }
        iioimage.setMetadata(metadata);
      }

      writer.write(null, iioimage, param);
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " error while encoding PGraphics.", ioe);
    }
    
    writer.dispose();

    StringBuilder sb = new StringBuilder("data:image/png;base64,");
    sb.append(Base64.encodeBase64String(baos.toByteArray()));
    
    stack.push(sb.toString());

    //
    // Re-issue a 'beginDraw' so we can continue using the PGraphics instance
    //
    
    pg.beginDraw();

    return stack;
  }
}
