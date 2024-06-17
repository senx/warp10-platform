//
//   Copyright 2018-2024  SenX S.A.S.
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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.TYPEOF;
import processing.core.PGraphics;
import processing.core.PImage;

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

    if (!(top instanceof PImage)) {
      throw new WarpScriptException(getName() + " operates on a " + TYPEOF.TYPE_PGRAPHICSIMAGE + " or " + TYPEOF.TYPE_PIMAGE + " instance.");
    }

    PImage image = (PImage) top;

    String imageStr;
    try {
      imageStr = PImageToString(image, chunks);
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " failed.", wse);
    }

    stack.push(imageStr);

    return stack;
  }

  public static String PImageToString(PImage image, Map<Object,Object> chunks) throws WarpScriptException {
    PGraphics  pg = null;

    if (image instanceof PGraphics) {
      pg = (PGraphics) image;
      pg.endDraw();
    }

    String format = "png";

    if (null != chunks && chunks.containsKey("format")) {
      format = String.valueOf(chunks.get("format"));

      if (!"png".equals(format) && !"jpeg".equals(format)) {
        throw new WarpScriptException("Only formats 'png' and 'jpeg' are supported.");
      }
    }

    //
    // For JPEG output we need to remove the alpha channel
    //

    BufferedImage bimage = new BufferedImage(image.pixelWidth, image.pixelHeight, "jpeg".equals(format) ? BufferedImage.TYPE_INT_RGB : BufferedImage.TYPE_INT_ARGB);
    bimage.setRGB(0, 0, image.pixelWidth, image.pixelHeight, image.pixels, 0, image.pixelWidth);
    Iterator<ImageWriter> iter = ImageIO.getImageWritersByFormatName(format);
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

      if (null != chunks && "png".equals(format)) {
        PNGMetadata metadata = new PNGMetadata();

        for (Entry<Object,Object> entry: chunks.entrySet()) {
          if ("tEXt".equals(entry.getKey()) || "zTXt".equals(entry.getKey())) {
            boolean zTXt = "zTXt".equals(entry.getKey());
            Object chunklist = entry.getValue();

            if (!(chunklist instanceof List)) {
              throw new WarpScriptException("Chunk type must be associated with a list of chunks.");
            }

            for (Object chunkelt: (List<Object>) chunklist) {
              if (!(chunkelt instanceof Map)) {
                throw new WarpScriptException("tEXt and zTXt chunks must be MAP instances.");
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
                throw new WarpScriptException("tEXt and zTXt chunks MUST contains 'keyword' and 'text' entries of type STRING.");
              }
            }
          } else if ("iTXt".equals(entry.getKey())) {
            Object chunklist = entry.getValue();

            if (!(chunklist instanceof List)) {
              throw new WarpScriptException("Chunk type must be associated with a list of chunks.");
            }

            for (Object chunkelt: (List<Object>) chunklist) {
              if (!(chunkelt instanceof Map)) {
                throw new WarpScriptException("iTXt chunks must be MAP instances.");
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
                throw new WarpScriptException("iTXt chunks MUST contains 'keyword' and 'text' entries of type STRING.");
              }
            }
          } else {
            throw new WarpScriptException("Only 'tEXt', 'zTXt' and 'iTXt' chunks can be specified.");
          }
        }
        iioimage.setMetadata(metadata);
      } else if (null != chunks && "jpeg".equals(format)) {
        if (chunks.get("quality") instanceof Double) {
          ImageWriteParam jpgWriteParam = writer.getDefaultWriteParam();
          jpgWriteParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
          jpgWriteParam.setCompressionQuality(((Double) chunks.get("quality")).floatValue());
        }
      }

      writer.write(null, iioimage, param);
    } catch (IOException ioe) {
      throw new WarpScriptException("Error while encoding PGraphics or PImage.", ioe);
    }

    writer.dispose();

    StringBuilder sb = new StringBuilder("data:image/" + format + ";base64,");
    sb.append(Base64.encodeBase64String(baos.toByteArray()));

    //
    // Re-issue a 'beginDraw' so we can continue using the PGraphics instance
    //

    if (null != pg) {
      pg.beginDraw();
    }

    return sb.toString();
  }
}
